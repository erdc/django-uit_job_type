## Cleanup ------------------------------------------------
# Using the "here document" syntax, create a job script
# for cleaning up your data.
cd {{WORKDIR}}
rm -f clean_job
cat >clean_job <<END
#!/bin/bash
#PBS -l walltime={{walltime}}
#PBS -q transfer
#PBS -A {{Project_ID}}
#PBS -l select=1:ncpus=1
#PBS -j oe
#PBS -S /bin/bash

# Remove job_work_dir
archive rm {{WORKDIR}}/{{JOBID}}

# Remove job_home_dir
archive rm {{HOME}}/{{JOBID}}

{% if archive %}
# Remove job_archive_dir
archive rm {{ARCHIVE_HOME}}/{{JOBID}}
{% endif %}

# Submit the archive job script.
qsub clean_job

