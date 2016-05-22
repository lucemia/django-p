from django.db import models
from jsonfield import JSONField
from aenum import Enum
from . import util

# Create your models here.

def _choices(enum):
    choices = []
    for x in dir(enum):
        if x.startswith('_'):
            continue
        else:
            choices.append((getattr(enum, x), x))

    return choices


class Pipeline(models.Model):
    STATUS = Enum("STATUS", "ABORTED DONE RUN WAITING")

    class_path = models.CharField(max_length=255)
    root_pipeline = models.ForeignKey("Pipeline")

    fanned_out = models.ManyToManyField("Pipeline")
    started = models.DateTimeField(auto_now_add=True)
    finalized = models.DateTimeField(auto_now=True)

    params = JSONField(defaults={})
    status = models.IntegerField(choices=_choices(STATUS), default=STATUS.WAITING)

    current_attempt = models.IntegerField(default=0)
    max_attempts = models.IntegerField(default=1)
    next_retry_time = models.DateTimeField()
    retry_message = models.TextField()

    is_root_pipeline = models.BooleanField(default=False)
    abort_message = models.TextField()
    abort_requested = models.BooleanField(default=False)

    def get_pipe():
        pass


class Slot(models.Model):
    STATUS = Enum("STATUS", "WAITING FILLED")

    root_pipeline = models.ForeignKey(Pipeline)
    filler = models.ForeignKey(Pipeline)

    value = JSONField(default={})
    status = models.IntegerField(choices=_choices(STATUS), default=STATUS.WAITING)

    filled = models.DateTimeField(auto_now=True)


class Barrier(models.Model):
    STATUS = Enum("STATUS", "START FINALIZE ABORT")

    root_pipeline = models.ForeignKey(Pipeline)
    target = models.ForeignKey(Pipeline)

    blocking_slots = models.ManyToManyField(Slot)
    triggered = models.DateTimeField(auto_now_add=True)
    status = models.IntegerField(choices=_choices(STATUS), default=STATUS.WAITING)


class Status(models.model):
    root_pipeline = models.ForeignKey(Pipeline)
    message = models.TextField()

    updated = models.DateTimeField(auto_now=True)


class Pipe(object):
    # may use proxy model of Pipeline
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.outputs = None
        self._context = None

    @classmethod
    def from_id(cls, pipeline_id):
        # TODO:
        pass

    def start(self):
        self._context = PipeContext()
        self.outputs = PipeFuture(self.outputs)

        return self._context.start(self)

    def run(self, *args, **kwargs):
        raise NotImplementedError()


class PipeFuture(object):
    def __init__(self, output_names):
        self._after_all_pipelines = set()
        self._output_dict = {
            'default': Slot(name='default')
        }

        for name in output_names:
            if name in self._output_dict:
                raise
            self._output_dict[name] = Slot(name=name)

    def _inherit_outputs(self, pipeline_name):
        pass

    def __getattr__(self, name):
        if name not in self._output_dict:
            raise
        return self._output_dict[name]


class PipeContext(object):

    def fill_slot(self, filler, slot, value):
        # do in transaction

        slot.filler = filler
        self.value = value
        self.status = Slot.STATUS.FILLED
        self.save()

        # call notify barrier async
        # barrier handler

        self.notify_barriers(slot.pk)

    def notify_barriers(self, slot_key):
        slot = Slot.objects.get(id=slot_key)

        results = slot.barrier_set.all()

        for barrier in results:
            ready_slots = []
            for block_slot in barrier.blocking_slots:
                if block_slot.status == Slot.STATUS.FILLED:
                    ready_slots.append(block_slot)

            pending_slots = set(barrier.blocking_slots) - set(ready_slots)

            if not pending_slots:
                if barrier.status != Barrier.STATUS.FIRED:
                    barrier.status = Barrier.STATUS.FIRED
                    barrier.triggered = _gettime()
                    barrier.save()

                purpose = barrier.purpose
                run_pipeline(barrier.target.pk)

    # abort

    def start(self, pipe):
        for name, slot in pipe.outputs._output_dict.iteritems():
            pass

        # TODO: create pipeline object from pipe
        pipeline = None

        bulk_create = []
        for name, slot in pipe.outputs._output_dict.iteritems():
            bulk_create.append(Slot(
                root_pipeline=pipeline
            ))

        bulk_create.extend(self._create_barrier_entities(
            output_slots
        ))

        models.Models.bulk_create(bulk_create)

        tasks.run_pipeline()


    def evaluate(self, pipeline_pk):
        pipeline_record = Pipeline.objects.get(pk=pipeline_pk)

        # convert to a python pipe instance
        pipe = Pipe.from_id(pipeline_pk)

        caller_output = pipe.outputs
        root_pipeline_pk = pipeline_record.root_pipeline_id

        pipe._context = self
        pipeline_iter = pipe.run()

        next_value = None
        while True:
            try:
                yielded = pipeline_iter.send(next_value)
            except StopIteration:
                break
            except Exception, e:
                raise

            if isinstance(yielded, Pipe):
                next_value = PipeFuture(yielded.output_names)
            else:
                raise
