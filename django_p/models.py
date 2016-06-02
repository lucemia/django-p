from django.db import models
from jsonfield import JSONField
from model_utils import Choices
import util
# Create your models here.


class Pipeline(models.Model):
    STATUS = Choices(
        (3, 'WAITING', "WAITING"),
        (2, 'RUN', "RUN"),
        (1, 'DONE', "DONE"),
        (0, 'ABORTED', "ABORTED")
    )

    class_path = models.CharField(max_length=255)
    root_pipeline = models.ForeignKey(
        "Pipeline", null=True, blank=True, related_name="descendants")
    parent_pipeline = models.ForeignKey(
        "Pipeline", null=True, blank=True, related_name="children")

    started = models.DateTimeField(auto_now_add=True)
    finalized = models.DateTimeField(auto_now=True)
    output = models.ForeignKey("Slot", null=True)

    params = JSONField(default={})

    status = models.IntegerField(choices=STATUS, default=STATUS.WAITING)

    # current_attempt = models.IntegerField(default=0)
    # max_attempts = models.IntegerField(default=1)
    # next_retry_time = models.DateTimeField()
    # retry_message = models.TextField()

    abort_message = models.TextField()
    abort_requested = models.BooleanField(default=False)

    def _to_value(self, v):
        if v['type'] == "value":
            return v['value']
        elif v['type'] == "Slot":
            slot = Slot.objects.get(pk=v['slot_key'])
            if slot.status == Slot.STATUS.FILLED:
                return slot.value
            else:
                return slot

    def _from_value(self, v):
        from .pipeline import Future

        if isinstance(v, Future):
            v = v.output
        if isinstance(v, Slot):
            return {"type": "Slot", "slot_key": v.pk}
        else:
            return {"type": "value", "value": v}

    @property
    def args(self):
        return [self._to_value(k) for k in self.params['args']]

    @args.setter
    def args(self, value):
        self.params['args'] = [self._from_value(k) for k in value]

    @property
    def kwargs(self):
        return {k: self._to_value(self._kwargs[k]) for k in self.params['kwargs']}

    @kwargs.setter
    def kwargs(self, value):
        self.params['kwargs'] = {k: self._from_value(k) for k in value}

    @property
    def is_root_pipeline(self):
        return self.root_pipeline is None

    def __unicode__(self):
        return "%s:%s" % (self.class_path, self.pk)


class Slot(models.Model):
    STATUS = Choices(
        (1, 'FILLED', "FILLED"),
        (0, 'WAITING', "WAITING")
    )

    filler = models.ForeignKey(Pipeline, null=True)

    value = JSONField(default=None)
    status = models.IntegerField(choices=STATUS, default=STATUS.WAITING)

    filled = models.DateTimeField(auto_now=True)

    def __unicode__(self):
        return "%s:%s" % (self.filler, self.value if self.status == Slot.STATUS.FILLED else "waiting")


class Barrier(models.Model):
    PURPOSE = Choices(
        (2, "ABORT", "ABORT"),
        (1, "FINALIZE", "FINALIZE"),
        (0, "START", "START")
    )

    STATUS = Choices(
        (1, "FIRED", "FIRED"),
        (0, "WAITING", "WAITING")
    )

    target = models.OneToOneField(Pipeline)

    blocking_slots = models.ManyToManyField(Slot)
    triggered = models.DateTimeField(null=True, auto_now=True)
    status = models.IntegerField(choices=STATUS, default=STATUS.WAITING)

    def __unicode__(self):
        return unicode(self.target)

# class Status(models.Model):
#     root_pipeline = models.ForeignKey(Pipeline)
#     message = models.TextField()

#     updated = models.DateTimeField(auto_now=True)
