## Execution Block ----------------------------------------
echo -e "-------------------------\n"
echo -e "Starting Execute Block...\n"
echo -e "-------------------------\n"

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
#
cp "{{ executable }}" "{{ executable }}.run"
chmod +x "{{ executable }}.run"
./"{{ executable }}.run"

## Cleanup ------------------------------------------------
# Cleanup is handled by a co-submitted script that has this script as its dependency.

echo -e "-------------------------\n"
echo -e "Finished Execute Block...\n"
echo -e "-------------------------\n"
