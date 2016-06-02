from django.contrib import admin
from .models import Pipeline, Barrier, Slot

# Register your models here.


class PipelineAdmin(admin.ModelAdmin):
    list_display = ('id', 'class_path', 'root_pipeline',
                    'started', 'output', 'status')


class BarrierAdmin(admin.ModelAdmin):
    list_display = ('id', 'target', 'triggered', 'status')


class SlotAdmin(admin.ModelAdmin):
    list_display = ('id', 'filler', 'value', 'status', 'filled')

admin.site.register(Pipeline, PipelineAdmin)
admin.site.register(Barrier, BarrierAdmin)
admin.site.register(Slot, SlotAdmin)
