from django.db import models
from jsonfield import JSONField
from model_utils import Choices
from . import util

# Create your models here.

class Pipeline(models.Model):
    STATUS = Choices(
        (3, 'WAITING'),
        (2, 'RUN'),
        (1, 'DONE'),
        (0, 'ABORTED')
    )

    class_path = models.CharField(max_length=255)
    root_pipeline = models.ForeignKey("Pipeline", null=True, blank=True)
    parent_pipeline = models.ForeignKey("Pipeline", null=True, blank=True)

    started = models.DateTimeField(auto_now_add=True)
    finalized = models.DateTimeField(auto_now=True)

    params = JSONField(defaults={})
    status = models.IntegerField(choices=STATUS, default=STATUS.WAITING)

    current_attempt = models.IntegerField(default=0)
    max_attempts = models.IntegerField(default=1)
    next_retry_time = models.DateTimeField()
    retry_message = models.TextField()

    abort_message = models.TextField()
    abort_requested = models.BooleanField(default=False)

    @property
    def is_root_pipeline(self):
        return self.root_pipeline == None

    @classmethod
    def get(cls, pk=None):
        # TODO:
        raise NotImplementedError()


class Slot(models.Model):
    STATUS = Choices(
        (1, 'FILLED'),
        (0, 'WAITING')
    )

    filler = models.ForeignKey(Pipeline)
    name = models.CharField(max_length=255)

    value = JSONField(default={})
    status = models.IntegerField(choices=STATUS, default=STATUS.WAITING)

    filled = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = (('filler', 'name'), )


class Barrier(models.Model):
    PURPOSE = Choices(
        (2, "ABORT"),
        (1, "FINALIZE"),
        (0, "START")
    )

    STATUS = Choices(
        (1, "FIRED"),
        (0, "WAITING")
    )

    target = models.ForeignKey(Pipeline)

    blocking_slots = models.ManyToManyField(Slot)
    triggered = models.DateTimeField()
    status = models.IntegerField(choices=STATUS, default=STATUS.WAITING)


class Status(models.model):
    root_pipeline = models.ForeignKey(Pipeline)
    message = models.TextField()

    updated = models.DateTimeField(auto_now=True)
