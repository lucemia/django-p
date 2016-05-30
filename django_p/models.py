from django.db import models
from jsonfield import JSONField
from model_utils import Choices
from . import util
import inspect

# Create your models here.



class Pipeline(models.Model):
    STATUS = Choices(
        (3, 'WAITING', "WAITING"),
        (2, 'RUN', "RUN"),
        (1, 'DONE', "DONE"),
        (0, 'ABORTED', "ABORTED")
    )

    class_path = models.CharField(max_length=255)
    root_pipeline = models.ForeignKey("Pipeline", null=True, blank=True, related_name="descendants")
    parent_pipeline = models.ForeignKey("Pipeline", null=True, blank=True, related_name="children")

    started = models.DateTimeField(auto_now_add=True)
    finalized = models.DateTimeField(auto_now=True)

    params = JSONField(default={})
    status = models.IntegerField(choices=STATUS, default=STATUS.WAITING)

    # current_attempt = models.IntegerField(default=0)
    # max_attempts = models.IntegerField(default=1)
    # next_retry_time = models.DateTimeField()
    # retry_message = models.TextField()


    abort_message = models.TextField()
    abort_requested = models.BooleanField(default=False)


    @property
    def is_root_pipeline(self):
        return self.root_pipeline is None

    @classmethod
    def get(cls, pk=None):
        # TODO:
        raise NotImplementedError()

    def __unicode__(self):
        return "%s:%s" % (self.class_path, self.pk)


class Slot(models.Model):
    STATUS = Choices(
        (1, 'FILLED', "FILLED"),
        (0, 'WAITING', "WAITING")
    )

    filler = models.ForeignKey(Pipeline)
    name = models.CharField(max_length=255)

    value = JSONField(default={})
    status = models.IntegerField(choices=STATUS, default=STATUS.WAITING)

    filled = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = (('filler', 'name'), )

    def __unicode__(self):
        return "%s:%s" % (self.filler, self.name)


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
    triggered = models.DateTimeField(null=True)
    status = models.IntegerField(choices=STATUS, default=STATUS.WAITING)

    def __unicode__(self):
        return unicode(self.target)

# class Status(models.Model):
#     root_pipeline = models.ForeignKey(Pipeline)
#     message = models.TextField()

#     updated = models.DateTimeField(auto_now=True)
