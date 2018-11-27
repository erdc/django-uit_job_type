## Execution Block ----------------------------------------
# Environment Setup

# cd to your scratch directory in /work
if [ ! -d ${{WORKDIR}} ]; then
  mkdir -p ${{WORKDIR}}
fi
cd ${{WORKDIR}}

# create a job-specific subdirectory based on JOBID and cd to it
if [ ! -d {{JOBID}} ]; then
  mkdir -p {{JOBID}}
fi
cd {{JOBID}}

{% if archive_input_files %}
# stage input data from archive
{% for archive_input_file in archive_input_files %}
archive get -C {{ARCHIVE_HOME}}/{{archive_input_file}} "*.*"
{% endfor %}
{% endif %}

{% if home_input_files %}
# stage input data from home
{% for home_input_file in home_input_files %}
archive get -C {{HOME}}/{{home_input_file}} "*.*"
{% endfor %}
{% endif %}

# copy the executable from $HOME
cp {{HOME}}/{{executable}} .

## Launching ----------------------------------------------
{{executable}}


## Cleanup ------------------------------------------------
# archive your results
# Using the "here document" syntax, create a job script
# for archiving your data.
cd {{WORKDIR}}
rm -f archive_job
cat >archive_job <<END
#!/bin/bash
#PBS -l walltime={{walltime}}
#PBS -q transfer
#PBS -A {{Project_ID}}
#PBS -l select=1:ncpus=1
#PBS -j oe
#PBS -S /bin/bash
cd {{WORKDIR}}/{{JOBID}}

{% if archive_output_files %}
# make dir
archive mkdir -C {{ARCHIVE_HOME}} {{JOBID}}
# transfer the archive_output_files to archive home
{% for archive_output_file in archive_output_files %}
archive mv {{archive_output_file}} {{ARCHIVE_HOME}}/{{JOBID}}
{% endfor %}
# list all the archive_output_files
archive ls {{ARCHIVE_HOME}}/{{JOBID}}
{% endif %}

{% if home_output_files or transfer_output_files %}
# make dir
archive mkdir -C {{HOME}} {{JOBID}}
{% if home_output_files%}
# transfer the home_output_files to home
{% for home_output_file in home_output_files %}
archive mv {{home_output_file}} {{HOME}}/{{JOBID}}
{% endfor %}
{% endif %}
{% if transfer_output_files%}
# transfer the transfer_output_files to home
{% for transfer_output_file in transfer_output_files %}
archive mv {{transfer_output_file}} {{HOME}}/{{JOBID}}
{% endfor %}
{% endif %}
{% endif %}

# Remove scratch directory from the file system.
cd {{WORKDIR}}
#rm -rf {{JOBID}}
END

# Submit the archive job script.
qsub archive_job

