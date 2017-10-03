import traceback
from datetime import datetime

from django.db import transaction

import util
from django_q.tasks import async
from util import After, InOrder

from .models import Barrier, Pipeline, Slot, Status


class Future(object):

    def __init__(self, slot):
        self._after_all_pipelines = []
        self.output = slot

    @property
    def is_done(self):
        return self.output.status == Slot.STATUS.FILLED

    @property
    def value(self):
        if self.is_done:
            return self.output.value

        raise Exception()


class Pipe(object):
    def __init__(self, *args, **kwargs):
        self.pk = None
        self.parent = None

        self._args = [Future(k) if isinstance(k, Slot) else k for k in args]
        self._kwargs = {k: Future(v) if isinstance(v, Slot) else v for k, v in kwargs.items()}

        self.class_path = "%s.%s" % (self.__module__, self.__class__.__name__)
        self.output = None

    @property
    def pipeline(self):
        if self.pk:
            return Pipeline.objects.get(pk=self.pk)

    @classmethod
    def from_id(cls, id):
        pipeline = Pipeline.objects.get(id=id)
        klass = util.import_class(pipeline.class_path)

        obj = klass(*pipeline.args, **pipeline.kwargs)
        obj.pk = id
        obj.parent = pipeline.parent_pipeline
        obj.output = pipeline.output

        return obj

    def dependent_slots(self):
        slots = set()
        for arg in self._args:
            if isinstance(arg, Future):
                slots.add(arg.output)

        for key, arg in self._kwargs.items():
            if isinstance(arg, Future):
                slots.add(arg.output)

        return slots

    @transaction.atomic
    def save(self):
        if self.pk:
            p = Pipeline.objects.get(pk=self.pk)
        else:
            # create new Pipeline
            p = Pipeline.objects.create(
                class_path=self.class_path,
                parent_pipeline=self.parent
            )

            if p.parent_pipeline:
                p.root_pipeline = p.parent_pipeline.root_pipeline

            p.output = Slot.objects.create()

            self.pk = p.pk
            self.output = p.output

        p.args = [k.output if isinstance(k, Future) else k for k in self._args]
        p.kwargs = {k: v.output if isinstance(v, Future) else v for k, v in self._kwargs.items()}
        p.output = self.output

        p.save()

    def run(self, *args, **kwargs):
        raise NotImplementedError()

    def start(self):
        self.save()
        async(evaluate, self.pk)

    @property
    def args(self):
        return [k.value if isinstance(k, Future) else k for k in self._args]

    @property
    def kwargs(self):
        return {k: v.value if isinstance(v, Future) else v for k, v in self._kwargs.items()}

    def is_ready(self):
        for arg in self.args:
            if isinstance(arg, Future) and not arg.is_done:
                return False

        for arg in self.kwargs.values():
            if isinstance(arg, Future) and not arg.is_done:
                return False

        return True


def notify_barrier(barrier):
    if all(slot.status == Slot.STATUS.FILLED for slot in barrier.blocking_slots.all()):
        with transaction.atomic():
            barrier.status = Barrier.STATUS.FIRED
            barrier.triggered = datetime.utcnow()
            barrier.save()

        async(evaluate, barrier.target.pk)


def notify_barriers(slot):
    for barrier in slot.barrier_set.all():
        notify_barrier(barrier)


def update_pipeline_status(pipeline):
    with transaction.atomic():
        if not pipeline.output.status == Slot.STATUS.FILLED:
            return

        if not all(child_pipeline.status == Pipeline.STATUS.DONE for child_pipeline in pipeline.children.all()):
            return

        pipeline.status = Pipeline.STATUS.DONE
        pipeline.save()

    if not pipeline.is_root_pipeline:
        update_pipeline_status(pipeline.parent_pipeline)


def fill_slot(pipeline, slot, value):
    if isinstance(value, Slot):
        with transaction.atomic():
            slot.filler = pipeline
            slot.reference_slot = value
            slot.save()
    else:
        with transaction.atomic():
            slot.filler = pipeline
            slot.value = value
            slot.status = Slot.STATUS.FILLED
            slot.save()

        notify_barriers(slot)
        update_pipeline_status(pipeline)

        for _slot in slot.slot_set.all():
            fill_slot(pipeline, _slot, value)


def exception(pipeline, e):
    Status.objects.create(
        pipeline=pipeline,
        error=unicode(e),
        message=traceback.format_exc()
    )


def evaluate(pipeline_pk):
    After._thread_init()
    InOrder._thread_init()
    InOrder._local._activated = False

    pipe = Pipe.from_id(pipeline_pk)
    args = pipe.args
    kwargs = pipe.kwargs

    assert pipe.is_ready()

    pipeline = pipe.pipeline
    pipeline.status = pipeline.STATUS.RUN
    pipeline.save()

    if not util.is_generator_function(pipe.run):
        # HINT: if pipe is not a generator, then process directly
        try:
            result = pipe.run(*args, **kwargs)
        except Exception as e:
            exception(pipeline, e)
            raise

        fill_slot(pipeline, pipeline.output, result)
        return

    sub_stage_dict = {}
    sub_stage_ordering = []
    next_value = None

    try:
        pipeline_iter = pipe.run(*args, **kwargs)
    except Exception, e:
        exception(pipeline, e)
        raise

    while True:
        try:
            yielded = pipeline_iter.send(next_value)
        except StopIteration:
            break
        except Exception, e:
            exception(pipeline, e)
            raise

        assert isinstance(yielded, Pipe)
        assert yielded not in sub_stage_dict

        yielded.parent = pipeline
        yielded.save()

        next_value = Future(yielded.output)
        next_value._after_all_pipelines.extend(
            After._local._after_all_futures
        )
        next_value._after_all_pipelines.extend(
            InOrder._local._in_order_futures
        )
        sub_stage_dict[yielded] = next_value
        sub_stage_ordering.append(yielded)
        InOrder._add_future(next_value)

    if not sub_stage_ordering:
        # HINT: the pipeline iter didn't return anything
        return fill_slot(pipeline, pipeline.output, None)

    fill_slot(sub_stage_ordering[-1].pipeline, pipeline.output, sub_stage_ordering[-1].output)

    for sub_stage in sub_stage_ordering:
        future = sub_stage_dict[sub_stage]
        child_pipe = sub_stage

        # HINT, the child pipe's dependent include it's args, kwargs
        # and all pipeline record in _after_all_pipelines
        dependent_slots = child_pipe.dependent_slots()
        dependent_slots.update(_future.output for _future in future._after_all_pipelines)

        barrier = Barrier.objects.create(
            target_id=child_pipe.pk,
        )
        barrier.blocking_slots.add(*dependent_slots)
        barrier.save()

        notify_barrier(barrier)
