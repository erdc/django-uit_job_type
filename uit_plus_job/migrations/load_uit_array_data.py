"""Script to manually migrate data from ArrayFields that were converted to JSONFields by this migration.
Due to limitations with PostgreSQL the data cannot be migrated in the same step as the database schema is modified.

Instructions:
    1. Before updating to this version of django-uit_job_type, export the DB data:

        tethys manage dumpdata uit_plus_job | tail -n +5 > uit_db.json

    2. Update django-uit_job_type and run migrations

        tethys db migrate

    3. Import the ArrayField data from the exported data file in step 1:

        tethys manage shell < $(python -c "import uit_plus_job; print(uit_plus_job.__path__[0])")/migrations/load_uit_array_data.py

"""

import json

from uit_plus_job.models import EnvironmentProfile, UitPlusJob

json_data = json.load(open('uit_db.json'))
model_data = {}

for row in json_data:
    value = model_data.setdefault(row['model'], {})
    value[row['pk']] = row['fields']

key = f'{EnvironmentProfile._meta.app_label}.{EnvironmentProfile._meta.model_name}'
data = model_data[key]

for ep in EnvironmentProfile.objects.all():
    ep.default_for_versions = data[ep.pk]['default_for_versions']
    ep.save()

key = f'{UitPlusJob._meta.app_label}.{UitPlusJob._meta.model_name}'
data = model_data[key]

for job in UitPlusJob.objects.all():
    row_data = data[job.pk]
    job._optional_directives = json.loads(row_data['_optional_directives'])
    if row_data['_array_indices']:
        job._array_indices = [int(i) for i in json.loads(row_data['_array_indices'])]
    job.save()
