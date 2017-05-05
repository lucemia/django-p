from .models import Pipeline, Slot, Barrier, Status
from django_q.tasks import async
from django.db import transaction
from datetime import datetime
import util
from util import After, InOrder
import traceback


class Future(object):

    def __init__(self, pipe):
        self._after_all_pipelines = {}
        self.output = pipe.output

    @property
    def is_done(self):
        return self._value.status == Slot.STATUS.FILLED

    @property
    def value(self):
        if self.is_done:
            return self._value


class Pipe(object):
    def __init__(self, *args, **kwargs):
        self.pk = None
        self.parent_pk = None
        self.args = args
        self.kwargs = kwargs

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

        obj = klass(pipeline.parent_pipeline_id, *pipeline.args, **pipeline.kwargs)
        obj.pk = id
        obj.output = pipeline.output

        return obj

    @transaction.atomic
    def save(self, **kwargs):
        if self.pk:
            p = Pipeline.objects.get(pk=self.pk)
        else:
            # create new Pipeline
            p = Pipeline.objects.create(
                class_path=self.class_path,
                parent_pipeline_id=self.parent_pk
            )

            if self.parent_pk:
                root = p
                while not root.is_root_pipeline:
                    root = root.parent_pipeline

                p.root_pipeline = root

            # CHECK: p.output may be share with the same slot with other pipeline
            # need to check it is right design
            if not p.output:
                p.output = Slot.objects.create()

            self.pk = p.pk
            self.output = p.output

        p.args = self.args
        p.kwargs = self.kwargs
        p.output = self.output

        for k in kwargs:
            setattr(p, k, kwargs[k])

        p.save()

    def run(self, *args, **kwargs):
        raise NotImplementedError()

    def start(self):
        self.save()
        async(evaluate, self.pk)


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
    with transaction.atomic():
        slot.filler = pipeline
        slot.value = value
        slot.status = Slot.STATUS.FILLED
        slot.save()

    notify_barriers(slot)
    update_pipeline_status(pipeline)


@transaction.atomic
def switch_slot(org_slot, new_slot):
    for barrier in org_slot.barrier_set.all():
        barrier.blocking_slots.remove(org_slot)
        barrier.blocking_slots.add(new_slot)

    for pipeline in org_slot.pipeline_set.all():
        pipeline.output = new_slot
        pipeline.save()

    org_slot.delete()


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
    pipeline = pipe.pipeline

    args = pipeline.args
    kwargs = pipeline.kwargs

    # Pipe should not start evaluate while input is not ready
    assert not any(isinstance(k, Slot) for k in args)
    assert not any(isinstance(kwargs[k], Slot) for k in kwargs)

    # if any(isinstance(k, Slot) for k in args) or any(isinstance(kwargs[k], Slot) for k in kwargs):
    #     async(evaluate, pipeline_pk)
    #     return

    # if pipe is not generator, then process directly
    if not util.is_generator_function(pipe.run):
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
        print pipe.run, args, kwargs
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

        yielded.parent_pk = pipeline.pk
        yielded.save()

        next_value = Future(yielded)
        next_value._after_all_pipelines.update(
            After._local._after_all_futures
        )
        next_value._after_all_pipelines.update(
            InOrder._local._in_order_futures
        )
        sub_stage_dict[yielded] = next_value
        sub_stage_ordering.append(yielded)
        InOrder._add_future(next_value)

    if not sub_stage_ordering:
        return fill_slot(pipeline, pipeline.output, None)

    # CHECK: should share the output?
    switch_slot(pipeline.output, sub_stage_ordering[-1].output)

    for sub_stage in sub_stage_ordering:
        future = sub_stage_dict[sub_stage]
        child_pipeline = sub_stage

        dependent_slots = set()
        for arg in child_pipeline.args:
            if isinstance(arg, Slot):
                dependent_slots.add(arg)
        for key, arg in child_pipeline.kwargs.iteritems():
            if isinstance(arg, Slot):
                dependent_slots.add(arg)

        for other_future in future._after_all_pipelines:
            slot = other_future.output
            dependent_slots.add(slot)

        # dependent_slots.add(future.output)

        barrier = Barrier.objects.create(
            target_id=child_pipeline.pk,
        )
        barrier.blocking_slots.add(*dependent_slots)
        barrier.save()

        notify_barrier(barrier)
