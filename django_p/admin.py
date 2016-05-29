from django.contrib import admin
from .models import Pipeline, Barrier, Slot

# Register your models here.

admin.site.register(Pipeline)
admin.site.register(Barrier)
admin.site.register(Slot)
