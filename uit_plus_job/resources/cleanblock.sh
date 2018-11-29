## Cleanup ------------------------------------------------
# Using the "here document" syntax, create a job script
# for cleaning up your data.
cd {{ job_work_dir }}
rm -f clean_job
cat >clean_job <<END
#!/bin/bash
#PBS -l walltime={{ cleanup_walltime }}
#PBS -q transfer
#PBS -A {{ project_id }}
#PBS -l select=1:ncpus=1
#PBS -j oe
#PBS -S /bin/bash

# Remove job_work_dir
archive rm -rf {{ job_work_dir }}

# Remove job_home_dir
rm -rf {{ job_home_dir }}

{% if archive %}
# Remove job_archive_dir
rm -rf {{ job_archive_dir }}
{% endif %}

# Submit the archive job script.
qsub clean_job
