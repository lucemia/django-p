from .models import Pipeline, Slot, Barrier
from django_q.tasks import async
from datetime import datetime
import util
from util import After, InOrder


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
        self.args = args
        self.kwargs = kwargs
        self.class_path = "%s.%s" % (self.__module__, self.__class__.__name__)
        self.output = None

    @classmethod
    def from_id(cls, id):
        pipeline = Pipeline.objects.get(id=id)
        klass = util.import_class(pipeline.class_path)
        obj = klass(*pipeline.args, **pipeline.kwargs)
        obj.pk = id
        obj.output = pipeline.output
        return obj

    def save(self, **kwargs):
        if self.pk:
            p, _ = Pipeline.objects.update_or_create(id=self.pk)
        else:
            p = Pipeline()

        p.class_path = self.class_path
        p.args = self.args
        p.kwargs = self.kwargs
        p.output = self.output

        for k in kwargs:
            setattr(p, k, kwargs[k])

        p.save()

        if not p.output:
            p.output = Slot.objects.create(filler=p)
            p.save()

        self.pk = p.id
        self.output = p.output

    def run(self):
        raise NotImplementedError()

    def start(self):
        self.save()
        async(evaluate, self.pk)


def notify_barrier(barrier):
    if all(slot.status == Slot.STATUS.FILLED for slot in barrier.blocking_slots.all()):
        barrier.status = Barrier.STATUS.FIRED
        barrier.triggered = datetime.utcnow()
        barrier.save()

        async(evaluate, barrier.target.pk)


def notify_barriers(slot):
    for barrier in slot.barrier_set.all():
        notify_barrier(barrier)


def fill_slot(filler, slot, value):
    slot.filler_id = filler.pk
    slot.value = value
    slot.status = Slot.STATUS.FILLED
    slot.save()
    notify_barriers(slot)
    filler.save(status=Pipeline.STATUS.DONE)

def evaluate(pipeline_pk):
    After._thread_init()
    InOrder._thread_init()
    InOrder._local._activated = False

    pipeline = Pipe.from_id(pipeline_pk)

    # FIXME: handle not generator case
    pipeline_is_generator = util.is_generator_function(pipeline.run)

    args = pipeline.args
    kwargs = pipeline.kwargs
    assert not any(isinstance(k, Slot) for k in args)
    assert not any(isinstance(kwargs[k], Slot) for k in kwargs)

    if not pipeline_is_generator:
        result = pipeline.run(*args, **kwargs)
        fill_slot(pipeline, pipeline.output, result)
        return

    last_sub_stage = None
    sub_stage_dict = {}
    sub_stage_ordering = []
    next_value = None

    pipeline_iter = pipeline.run(*args, **kwargs)

    while True:
        try:
            yielded = pipeline_iter.send(next_value)
        except StopIteration:
            break
        except Exception, e:
            raise

        assert isinstance(yielded, Pipe)
        assert yielded not in sub_stage_dict

        yielded.save()
        last_sub_stage = yielded
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

    if last_sub_stage:
        pipeline.output = last_sub_stage.output
        pipeline.save()
    else:
        fill_slot(pipeline, pipeline.output, None)
        return

    for sub_stage in sub_stage_ordering:
        future = sub_stage_dict[sub_stage]
        sub_stage.save()
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
        notify_barrier(barrier)
