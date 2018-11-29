# Put your persistent store models in this file
import os
import uuid
from django.db import models
from picklefield import PickledObjectField
from jinja2 import Template
from uit.uit import Client
from uit.pbs_script import PbsScript, PbsDirective
from tethys_compute.models.tethys_job import TethysJob


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

    job_id = models.CharField(max_length=1024, null=True)
    project_id = models.CharField(max_length=1024, null=False)
    system = models.CharField(max_length=10, choices=SYSTEM_CHOICES, default='topaz', null=False)
    node_type = models.CharField(max_length=10, choices=NODE_TYPE_CHOICES, default='compute', null=False)
    num_nodes = models.IntegerField(default=1, null=False)
    processes_per_node = models.IntegerField(default=1, null=False)
    max_time = models.DurationField(null=False)
    queue = models.CharField(max_length=100, default='debug', null=False)
    job_script = models.TextField(null=False)
    transfer_job_script = models.BooleanField(default=True)
    transfer_input_files = PickledObjectField(default=list)
    archive_input_files = PickledObjectField(default=list)
    home_input_files = PickledObjectField(default=list)
    transfer_output_files = PickledObjectField(default=list)
    archive_output_files = PickledObjectField(default=list)
    home_output_files = PickledObjectField(default=list)
    _modules = PickledObjectField(default=dict)
    _optional_directives = PickledObjectField(default=list)
    _remote_workspace_id = models.CharField(max_length=64, default=str(uuid.uuid4()))
    _remote_workspace = models.TextField(blank=True)


    @property
    def job_script_name(self):
        try:
            return os.path.split(self.job_script)[-1]
        except (AttributeError, IndexError):
            return ''

    @property
    def remote_workspace(self):
        if not self._remote_workspace:
            workspace_path = os.path.join(self.label, self.name, str(self._remote_workspace_id))
            self._remote_workspace = workspace_path
        return self._remote_workspace

    # job work directory
    @property
    def work_dir(self):
        return os.path.join("${WORKDIR}", self.remote_workspace)

    # job archive directory
    @property
    def archive_dir(self):
        return os.path.join("${ARCHIVE_HOME}", self.remote_workspace)

    # job home directory
    @property
    def home_dir(self):
        return os.path.join("${HOME}", self.remote_workspace)

    def get_client(self, token):
        # Create a client with token
        client = Client(token=token)

        # Connect the client
        client.connect(system=self.system)

        # return the client
        return client

    def set_directive(self, directive, value):
        # Save the result
        self._optional_directives.append(PbsDirective(directive, value))

    def get_directive(self, directive):
        for d in self._optional_directives:
            if d.directive == directive:
                return d.options

    def get_directives(self):
        return self._optional_directives

    def load_module(self, module):
        self._modules.update({module: "load"})

    def unload_module(self, module):
        self._modules.update({module: "unload"})

    def swap_module(self, module1, module2):
        self._modules.update({module1: module2})

    def get_modules(self):
        return self._modules

    def render_execution_block(self):
        context = {
            'job_work_dir': self.work_dir,
            'job_archive_dir': self.archive_dir,
            'job_home_dir': self.home_dir,
            'executable': self.job_script_name,
            'project_id': self.project_id,
            'archive_input_files': self.archive_input_files,
            'home_input_files': self.home_input_files,
            'archive_output_files': self.archive_output_files,
            'home_output_files': self.home_output_files,
            'transfer_output_files': self.transfer_output_files,
        }

        resources_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'uit_plus_job', 'resources')
        execution_block_path = os.path.join(resources_dir, 'executionblock.sh')

        with open(execution_block_path, 'r') as execution_block_file:
            text = execution_block_file.read()
            template = Template(text)
            execution_block = template.render(context)

        return execution_block

    def generate_pbs_script(self):
        # Create a new PbsScript
        pbs_script = PbsScript(job_name=self.name, project_id=self.project_id, num_nodes=self.num_nodes,
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
        import pdb; pdb.set_trace()
        command = 'mkdir -p ' + self.work_dir
        ret = client.call(command=command, work_dir='${WORKDIR}')

        # TODO: Check to make sure the directory was created before moving on... Raise exception if error occurs? Unless the dir already exists.

        # Transfer any files listed in transfer_input_files to work_dir on supercomputer
        # using client.put_file().
        for transfer_file in self.transfer_input_files:
            client.put_file(transfer_file, self.work_dir)

        # Transfer the job_script to the work_dir on supercomputer
        # using client.put_file().
        if self.transfer_job_script:
            client.put_file(self.job_script, self.work_dir)

        # Generate PbsScript object using generate_pbs_script().
        pbs_script = self.generate_pbs_script()

        # Submit job using client.submit() with PbsScript object and remote workspace
        job_id = client.submit(pbs_script, self.work_dir)

        # Save job id to job_id
        self.job_id = job_id
        self.save()

    def _update_status(self, token):
        # Get client using get_client() method
        client = self.get_client(token=token)

        pbs_command = 'qstat ' + self.job_id
        # Get status using client.call() either qstat/qview (which ever is easier to parse).
        qstat_ret = client.call(command=pbs_command, work_dir=self.work_dir)

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
        job_transfer_output_files = os.path.join(transfer_output_files_path, self.name)
        if not os.path.exists(transfer_output_files_path):
            os.makedirs(transfer_output_files_path)
            os.makedirs(job_transfer_output_files)
        else:
            if not os.path.exists(job_transfer_output_files):
                os.makedirs(job_transfer_output_files)

        # Get transfer_output_files from work_dir
        work_directory_json_response = client.list_dir(self.work_dir)
        if work_directory_json_response:
            self.get_remote_file(client=client,
                                 remote_files_path=self.transfer_output_files,
                                 local_path=job_transfer_output_files)
        else:
            # Get transfer_output_files from home_dir when work_dir doesn't exist
            home_directory_json_response = client.list_dir(self.home_dir)
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
        client.call(command=pbs_command, work_dir=self.work_dir)

    def pause(self, token):
        # Get client using get_client() method
        client = self.get_client(token=token)
        pbs_command = 'qhold ' + self.job_id
        # delete the job
        client.call(command=pbs_command, work_dir=self.work_dir)

    def resume(self, token):
        # Get client using get_client() method
        client = self.get_client(token=token)
        pbs_command = 'qrls ' + self.job_id
        # delete the job
        client.call(command=pbs_command, work_dir=self.work_dir)

    def clean(self, token, archive):
        # Get client using get_client() method
        client = self.get_client(token=token)

        pbs_clean_script = self.render_clean_block(archive)

        client.submit(pbs_clean_script, self.work_dir)

    def render_clean_block(self, archive=False):
        max_job_duration = str(self.max_time)
        context = {
            'WORKDIR': self.work_dir,
            'JOBID': self.job_id,
            'Project_ID': self.project_id,
            'ARCHIVE_HOME': self.archive_dir,
            'HOME': self.home_dir,
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
