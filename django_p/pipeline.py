from .models import Pipeline, Slot, Barrier
from django_q.tasks import async, result
from datetime import datetime
import threading
import inspect


class Future(object):

    def __init__(self, pipe):
        self._after_all_pipelines = {}
        self._output_dict = {
            'default': Slot.objects.update_or_create(filler_id=pipe.pk, name='default')[0]
        }

        for name in pipe.output_names:
            assert name not in self._output_dict

            self._output_dict[name] = Slot.objects.update_or_create(
                filler_id=pipe.pk, name=name)[0]

    def __getattr__(self, name):
        if name not in self._output_dict:
            raise
        return self._output_dict[name]


class Pipe(object):
    output_names = []

    def __init__(self, *args, **kwargs):
        self._class_path = "%s.%s"%(self.__module__, self.__class__.__name__)
        self.args = args
        self.kwargs = kwargs

    @property
    def pk(self):
        return self.pipeline_pk


    def run(self):
        raise NotImplementedError()

    def start(self):
        pipeline = Pipeline.objects.create(
            class_path=self._class_path,
        )
        self.pipeline_pk = pipeline.pk
        start(self)

    @classmethod
    def from_id(cls, id):
        raise NotImplementedError()

class After(object):
    """Causes all contained Pipelines to run after the given ones complete.

    Must be used in a 'with' block.
    """

    _local = threading.local()

    def __init__(self, *futures):
        """Initializer.

        Args:
          *futures: PipelineFutures that all subsequent pipelines should follow.
            May be empty, in which case this statement does nothing.
        """
        for f in futures:
            if not isinstance(f, Future):
                raise TypeError('May only pass PipelineFuture instances to After(). %r',
                                type(f))
        self._futures = set(futures)

    def __enter__(self):
        """When entering a 'with' block."""
        After._thread_init()
        After._local._after_all_futures.extend(self._futures)

    def __exit__(self, type, value, trace):
        """When exiting a 'with' block."""
        for future in self._futures:
            After._local._after_all_futures.remove(future)
        return False

    @classmethod
    def _thread_init(cls):
        """Ensure thread local is initialized."""
        if not hasattr(cls._local, '_after_all_futures'):
            cls._local._after_all_futures = []


class InOrder(object):
    """Causes all contained Pipelines to run in order.

    Must be used in a 'with' block.
    """

    _local = threading.local()

    @classmethod
    def _add_future(cls, future):
        """Adds a future to the list of in-order futures thus far.

        Args:
          future: The future to add to the list.
        """
        if cls._local._activated:
            cls._local._in_order_futures.add(future)

    def __init__(self):
        """Initializer."""

    def __enter__(self):
        """When entering a 'with' block."""
        # Reentrancy checking gives false errors in test mode since everything is
        # on the same thread, and all pipelines are executed in order in test mode
        # anyway, so disable InOrder for tests.
        InOrder._thread_init()
        assert not InOrder._local._activated, 'Already in an InOrder "with" block.'
        InOrder._local._activated = True
        InOrder._local._in_order_futures.clear()

    def __exit__(self, type, value, trace):
        """When exiting a 'with' block."""
        InOrder._local._activated = False
        InOrder._local._in_order_futures.clear()
        return False

    @classmethod
    def _thread_init(cls):
        """Ensure thread local is initialized."""
        if not hasattr(cls._local, '_in_order_futures'):
            cls._local._in_order_futures = set()
            cls._local._activated = False





def _generate_args(pipeline, future):
    params = {
        'args': [],
        'kwargs': {},
        'after_all': [],
        'output_slots': {},
        'class_path': pipeline._class_path,
    }
    dependent_slots = set()

    arg_list = params['args']
    for current_arg in pipeline.args:
        if isinstance(current_arg, Future):
            current_arg = current_arg.default
        if isinstance(current_arg, Slot):
            arg_list.append({'type': 'slot', 'slot_key': str(current_arg.pk)})
            dependent_slots.add(current_arg.pk)
        else:
            arg_list.append({'type': 'value', 'value': current_arg})

    kwarg_dict = params['kwargs']
    for name, current_arg in pipeline.kwargs.iteritems():
        if isinstance(current_arg, Future):
            current_arg = current_arg.default
        if isinstance(current_arg, Slot):
            kwarg_dict[name] = {'type': 'slot',
                                'slot_key': str(current_arg.pk)}
            dependent_slots.add(current_arg.pk)
        else:
            kwarg_dict[name] = {'type': 'value', 'value': current_arg}

    after_all = params['after_all']
    for other_future in future._after_all_pipelines:
        slot_key = other_future._output_dict['default'].pk
        after_all.append(slot_key)
        dependent_slots.add(slot_key)

    output_slots = params['output_slots']
    output_slot_keys = set()
    for name, slot in future._output_dict.iteritems():
        output_slot_keys.add(slot.pk)
        output_slots[name] = slot.pk

    return dependent_slots, output_slot_keys, params


def notify_barriers(slot):
    for barrier in slot.barrier_set.all():
        if all(slot.status == Slot.STATUS.FILLED for slot in barrier.blocking_slots.all()):
            barrier.status = Barrier.STATUS.FIRED
            barrier.triggered = datetime.utcnow()
            barrier.save()

            async('evaluate', barrier.target.pk)


def fill_slot(filler, slot, value):
    slot.filler = filler
    slot.value = value
    slot.status = Slot.STATUS.FILLED
    slot.filled = datetime.utcnow()
    slot.save()

    notify_barriers(slot)


def start(pipe):

    pipe.outputs = Future(pipe)
    _, output_slots, params = _generate_args(pipe, pipe.outputs)

    pipeline = Pipeline.objects.get(pk=pipe.pk)
    pipeline.params = params
    pipeline.save()

    barrier = Barrier.objects.create(
        target_id=pipe.pk,
        status=Barrier.PURPOSE.FINALIZE
    )
    barrier.blocking_slots.add(*output_slots)


    async(evaluate, pipe.pk, "start")



def evaluate(pipeline_pk, purpose, attempt=0):
    # FIXME: After, InOrder

    pipeline = Pipeline.get(pk=pipeline_pk)

    # FIXME: handle not generator case
    pipeline_iter = pipeline.run()

    last_sub_stage = None
    sub_stage_dict = {}
    sub_stage_ordering = []
    next_value = None

    while True:
        try:
            yielded = pipeline_iter.send(next_value)
        except StopIteration:
            break
        except Exception, e:
            raise

        assert isinstance(yielded, Pipe)
        assert yielded not in sub_stage_dict

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
        # FIXME:
        pass

    for sub_stage in sub_stage_ordering:
        future = sub_stage_dict[sub_stage]

        dependent_slots, output_slots, params = _generate_args(
            sub_stage, future)

        child_pipeline = Pipeline.objects.create(
            root_pipeline=pipeline,
            params=params,
            class_path=sub_stage._class_path
        ).save()

        barrier = Barrier.objects.create(
            root_pipeline=pipeline,
            target=child_pipeline,
            purpose=Barrier.PURPOSE.START,
            blocking_slots=dependent_slots
        )
