# -*- coding: utf-8 -*-
# Generated by Django 1.9.4 on 2016-05-29 10:27
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('django_p', '0002_auto_20160529_1026'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='pipeline',
            name='current_attempt',
        ),
        migrations.RemoveField(
            model_name='pipeline',
            name='max_attempts',
        ),
        migrations.RemoveField(
            model_name='pipeline',
            name='next_retry_time',
        ),
        migrations.RemoveField(
            model_name='pipeline',
            name='retry_message',
        ),
    ]
