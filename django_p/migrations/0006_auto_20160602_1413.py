# -*- coding: utf-8 -*-
# Generated by Django 1.9.4 on 2016-06-02 14:13
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion
import jsonfield.fields


class Migration(migrations.Migration):

    dependencies = [
        ('django_p', '0005_auto_20160602_0908'),
    ]

    operations = [
        migrations.AlterField(
            model_name='slot',
            name='filler',
            field=models.ForeignKey(
                null=True, on_delete=django.db.models.deletion.CASCADE, to='django_p.Pipeline'),
        ),
        migrations.AlterField(
            model_name='slot',
            name='value',
            field=jsonfield.fields.JSONField(default=None),
        ),
    ]
