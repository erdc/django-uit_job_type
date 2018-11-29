## Execution Block ----------------------------------------
# Environment Setup

# make and cd to your job working directory
if [ ! -d {{ job_work_dir }} ]; then
  mkdir -p {{ job_work_dir }}
fi
cd {{ job_work_dir }}

# stage input data from archive
{% for archive_input_file in archive_input_files %}
archive get -C ${ARCHIVE_HOME}/{{ archive_input_file }}
{% endfor %}

# stage input data from home
{% for home_input_file in home_input_files %}
cp ${HOME}/{{ home_input_file }} .
{% endfor %}

## Launching ----------------------------------------------
chmod +x {{ executable }}
./{{ executable }}

## Cleanup ------------------------------------------------
# archive your results
# Using the "here document" syntax, create a job script
# for archiving your data.
cd {{ job_work_dir }}
rm -f cleanup_job || true
cat >cleanup_job <<END
#!/bin/bash
#PBS -l walltime={{ cleanup_walltime }}
#PBS -q transfer
#PBS -A {{ project_id }}
#PBS -l select=1:ncpus=1
#PBS -j oe
#PBS -S /bin/bash
cd {{ job_work_dir }}

{# ARCHIVE OUTPUT FILES #}
{% if archive_output_files %}
# make dir
archive mkdir -p {{ job_archive_dir }}

# transfer the archive_output_files to archive home
{% for archive_output_file in archive_output_files %}
archive put -C {{ job_archive_dir }} {{ archive_output_file }}
{% endfor %}

# list all the archive_output_files
archive ls {{ job_archive_dir }}
{% endif %}

{# HOME OUTPUT FILES #}
{% if home_output_files%}
# make dir
mkdir -p {{ job_home_dir }}

# transfer the home_output_files to home
{% for home_output_file in home_output_files %}
cp {{ home_output_file }} {{ job_home_dir }}
{% endfor %}
{% endif %}

{# TRANSFER OUTPUT FILES #}
{% if transfer_output_files%}
# make transfer directory in job home
mkdir -p {{ job_home_dir }}/transfer

# transfer the transfer_output_files to home
{% for transfer_output_file in transfer_output_files %}
cp {{ transfer_output_file }} {{ job_home_dir }}/transfer
{% endfor %}
{% endif %}

# Remove scratch directory from the file system.
cd ${WORKDIR}
rm -rf {{ job_work_dir }}
END

# Submit the cleanup job script.
qsub cleanup_job
