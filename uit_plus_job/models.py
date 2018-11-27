# Put your persistent store models in this file
from tethys_compute.models.tethys_job import TethysJob
from uit.pbs_script import PbsScript
from uit.uit import Client
from django.db import models
import os
import uuid
import collections
from jinja2 import Template

PbsDirective = collections.namedtuple('directive', ['directive', 'options'])

UIT_to_TETHYS_STATUSES = (
    ('PEN', 'PEN'),
    ('SUB', 'SUB'),
    ('RUN', 'RUN'),
    ('COM', 'COM'),
    ('ERR', 'ERR'),
    ('ABT', 'ABT'),
)


class UitPlusJob(TethysJob):
    """
    UIT+ Job type.
    """
    SYSTEM_CHOICES = (
        ('topaz', 'topaz'),
        ('onyx', 'onyx'),
    )

    NODE_TYPE_CHOICES = (
        ('compute', 'compute'),
        ('gpu', 'gpu'),
        ('bigmem', 'bigmem'),
    )

    job_name = models.CharField(max_length=1024, unique=True)
    project_id = models.CharField(max_length=1024)
    num_nodes = models.IntegerField()
    processes_per_node = models.IntegerField()
    max_time = models.DurationField()
    queue = models.CharField(max_length=100, default='debug')
    node_type = models.CharField(max_length=10, choices=NODE_TYPE_CHOICES, default='compute')
    system = models.CharField(max_length=10, choices=SYSTEM_CHOICES, default='topaz')
    _optional_directives = models.CharField(max_length=2048, null=True)
    _modules = models.CharField(max_length=1024, null=True)
    job_script = models.CharField(max_length=2048)
    transfer_job_script = models.BooleanField(default=True)
    transfer_input_files = models.CharField(max_length=1024, null=True)
    archive_input_files = models.CharField(max_length=1024, null=True)
    home_input_files = models.CharField(max_length=1024, null=True)
    transfer_output_files = models.CharField(max_length=1024, null=True)
    archive_output_files = models.CharField(max_length=1024, null=True)
    home_output_files = models.CharField(max_length=1024, null=True)
    job_id = models.CharField(max_length=1024, null=True)
    _remote_workspace = models.CharField(max_length=1024, blank=True)

    _directives = []

    @property
    def remote_workspace(self):
        if not self._remote_workspace:
            workspace_path = os.path.join(self.label, self.job_name, str(uuid.uuid4()))
            self._remote_workspace = workspace_path
        return self._remote_workspace

    # job work directory
    @property
    def job_work_dir(self):
        return os.path.join("${WORKDIR}", self.remote_workspace)

    # job archive directory
    @property
    def job_archive_dir(self):
        return os.path.join("${ARCHIVE_HOME}", self.remote_workspace)

    # job home directory
    @property
    def job_home_dir(self):
        return os.path.join("${HOME}", self.remote_workspace)

    # TODO: Move Meta definition into class defined by Tethys
    class Meta:
        app_label = 'tethys_apps'

    def get_client(self, token):
        # Create a client with token
        client = Client(token=token)

        # Connect the client
        client.connect()

        # return the client
        return client

    def set_directive(self, directive, value):
        # get the updated directives
        self._directives.append(PbsDirective("-" + directive, value))

        # Save the result
        self._optional_directives = self._directives
        self.save()

    def get_directive(self, directive):
        directives = self._directives
        for i in range(len(directives)):
            if directives[i].directive == directive:
                return directives[i].options

    def get_directives(self):
        return self._directives

    def load_module(self, module):
        self._modules.update({module: "load"})

    def unload_module(self, module):
        self._modules.update({module: "unload"})

    def swap_module(self, module1, module2):
        self._modules.update({module1: module2})

    def get_modules(self):
        return self._modules

    def render_execution_block(self):
        max_job_duration = str(self.max_time)

        context = {
            'WORKDIR': self.job_work_dir,
            'JOBID': self.job_id,
            'ARCHIVE_HOME': self.job_archive_dir,
            'HOME': self.job_home_dir,
            'executable': self.job_script,
            'Project_ID': self.project_id,
            'walltime': max_job_duration,
            'archive_input_files': self.archive_input_files,
            'home_input_files': self.home_input_files,
            'archive_output_files': self.archive_output_files,
            'home_output_files': self.home_output_files,
            'transfer_output_files': self.transfer_output_files,
        }
        bash_file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'uit_plus_job', 'resources')
        execution_block_path = os.path.join(bash_file_path, 'executionblock.sh')

        with open(execution_block_path, 'r') as execution_block_file:
            text = execution_block_file.read()
            template = Template(text)
            execution_block = template.render(context)

        return execution_block
        # TODO: Should we need to include -np arguments when we launch

    def generate_pbs_script(self):
        # Create a new PbsScript
        pbs_script = PbsScript(job_name=self.job_name, project_id=self.project_id, num_nodes=self.num_nodes,
                               processes_per_node=self.processes_per_node, max_time=self.max_time)

        # Iterate through UitPlusJob.get_directives()
        # add to PbsScript instance one at a time using PbsScript.set_directive().
        directives = self.get_directives()
        for i in range(len(directives)):
            pbs_script.set_directive(directives[i].directive, directives[i].options)

        # Iterate through UitPlusJob.get_modules()
        # and add to PbsScript instance one at at time using appropriate module method.
        modules = self.get_modules()
        pbs_script._modules = modules

        # Assign PbsScript.execution_block from UitPlusJob.render_execution_block()
        pbs_script.execution_block = self.render_execution_block()

        return pbs_script.render()

    def _execute(self, token):
        # Get client using get_client() method
        client = self.get_client(token=token)

        # Setup working directory on supercomputer
        # using client.call() method
        remote_workspace = self.remote_workspace
        command = 'mkdir -p ' + remote_workspace
        client.call(command=command, work_dir='${WORKDIR}')

        # Transfer any files listed in transfer_input_files to job_work_dir on supercomputer
        # using client.put_file().
        transfer_files = self.transfer_input_files
        for transfer_file in transfer_files:
            client.put_file(transfer_file, self.job_work_dir)

        # Generate PbsScript object using generate_pbs_script().
        pbs_script = self.generate_pbs_script()

        # Submit job using client.submit() with PbsScript object and remote workspace
        job_id = client.submit(pbs_script, self.job_work_dir)

        # Save job id to job_id
        self.job_id = job_id
        self.save()

    def _update_status(self, token):
        # Get client using get_client() method
        client = self.get_client(token=token)

        pbs_command = 'qstat ' + self.job_id
        # Get status using client.call() either qstat/qview (which ever is easier to parse).
        qstat_ret = client.call(command=pbs_command, work_dir=self.job_work_dir)

        # TODO: Need to see the return string from qstat to get out the status.
        # Parse out value
        status = UIT_to_TETHYS_STATUSES[qstat_ret]

        self._update_status = status
        self.save()

    def _process_results(self, token):
        # Get client using get_client() method
        client = self.get_client(token=token)
        # path to store transfer output files
        transfer_output_files_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'uit_plus_job',
                                                  'transfer_output_files')
        # TODO: Where we store the job output files,
        # TODO: we are using /uit_plus_job/transfer_output_files/ + job_name for now
        job_transfer_output_files = os.path.join(transfer_output_files_path, self.job_name)
        if not os.path.exists(transfer_output_files_path):
            os.makedirs(transfer_output_files_path)
            os.makedirs(job_transfer_output_files)
        else:
            if not os.path.exists(job_transfer_output_files):
                os.makedirs(job_transfer_output_files)

        # Get transfer_output_files from job_work_dir
        work_directory_json_response = client.list_dir(self.job_work_dir)
        if work_directory_json_response:
            self.get_remote_file(client=client,
                                 remote_files_path=self.transfer_output_files,
                                 local_path=job_transfer_output_files)
        else:
            # Get transfer_output_files from job_home_dir when job_work_dir doesn't exist
            home_directory_json_response = client.list_dir(self.job_home_dir)
            if home_directory_json_response:
                self.get_remote_file(client=client,
                                     remote_files_path=self.transfer_output_files,
                                     local_path=job_transfer_output_files)

    def get_remote_file(self, client, remote_files_path, local_path):
        for remote_file_path in remote_files_path:
            client.get_file(remote_file_path, local_path)

    def stop(self, token):
        # Get client using get_client() method
        client = self.get_client(token=token)
        pbs_command = 'qdel ' + self.job_id
        # delete the job
        client.call(command=pbs_command, work_dir=self.job_work_dir)

    def pause(self, token):
        # Get client using get_client() method
        client = self.get_client(token=token)
        pbs_command = 'qhold ' + self.job_id
        # delete the job
        client.call(command=pbs_command, work_dir=self.job_work_dir)

    def resume(self, token):
        # Get client using get_client() method
        client = self.get_client(token=token)
        pbs_command = 'qrls ' + self.job_id
        # delete the job
        client.call(command=pbs_command, work_dir=self.job_work_dir)

    def clean(self, token, archive):
        # Get client using get_client() method
        client = self.get_client(token=token)

        pbs_clean_script = self.render_clean_block(archive)

        client.submit(pbs_clean_script, self.job_work_dir)

    def render_clean_block(self, archive=False):
        max_job_duration = str(self.max_time)
        context = {
            'WORKDIR': self.job_work_dir,
            'JOBID': self.job_id,
            'Project_ID': self.project_id,
            'ARCHIVE_HOME': self.job_archive_dir,
            'HOME': self.job_home_dir,
            'walltime': max_job_duration,
            'archive': archive,
        }

        bash_file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'uit_plus_job', 'resources')

        clean_block_path = os.path.join(bash_file_path, 'cleanblock.sh')

        with open(clean_block_path, 'r') as clean_block_file:
            text = clean_block_file.read()
            template = Template(text)
            clean_block = template.render(context)

        return clean_block
