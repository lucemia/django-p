# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2017-10-03 07:05
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('django_p', '0008_auto_20170505_0624'),
    ]

    operations = [
        migrations.AddField(
            model_name='slot',
            name='reference_slot',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, to='django_p.Slot'),
        ),
    ]
