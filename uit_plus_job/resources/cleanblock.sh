#!/bin/bash
#PBS -l walltime={{ cleanup_walltime }}
#PBS -q transfer
#PBS -A {{ project_id }}
#PBS -l select=1:ncpus=1
#PBS -j oe
#PBS -S /bin/bash

# Remove job_work_dir
rm -rf {{ job_work_dir }} || true

# Remove job_home_dir
rm -rf {{ job_home_dir }} || true

{% if archive %}
# Remove job_archive_dir
archive rm -rf {{ job_archive_dir }} || true
{% endif %}
