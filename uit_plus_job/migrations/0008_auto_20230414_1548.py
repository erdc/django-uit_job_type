# Generated by Django 3.2.16 on 2023-04-14 20:48

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('uit_plus_job', '0007_auto_20220214_1923'),
    ]

    operations = [
        migrations.AlterField(
            model_name='environmentprofile',
            name='id',
            field=models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID'),
        ),
        migrations.AlterField(
            model_name='environmentprofile',
            name='modules',
            field=models.JSONField(default=dict, null=True),
        ),
        migrations.AlterField(
            model_name='uitplusjob',
            name='_environment_variables',
            field=models.JSONField(default=dict, null=True),
        ),
        migrations.AlterField(
            model_name='uitplusjob',
            name='_module_use',
            field=models.JSONField(default=dict, null=True),
        ),
        migrations.AlterField(
            model_name='uitplusjob',
            name='_modules',
            field=models.JSONField(default=dict, null=True),
        ),
        migrations.AlterField(
            model_name='uitplusjob',
            name='custom_logs',
            field=models.JSONField(default=dict),
        ),
        migrations.AlterField(
            model_name='uitplusjob',
            name='qstat',
            field=models.JSONField(default=dict, null=True),
        ),
        migrations.AlterField(
            model_name='uitplusjob',
            name='system',
            field=models.CharField(choices=[('jim', 'jim'), ('mustang', 'mustang'), ('narwhal', 'narwhal'), ('onyx', 'onyx')], default='onyx', max_length=10),
        ),
    ]
