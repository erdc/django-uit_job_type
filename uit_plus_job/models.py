# Put your persistent store models in this file
import os
import uuid
import types
import inspect
import datetime as dt
from django.db import models
from picklefield import PickledObjectField
from jinja2 import Template
from uit.uit import Client
from uit.pbs_script import PbsScript, PbsDirective
from tethys_compute.models.tethys_job import TethysJob
from uit_plus_job.util import strfdelta


class UitPlusJob(PbsScript, TethysJob):
    """
    UIT+ Job type.
    """
    UIT_TO_TETHYS_STATUSES = {
        'B': 'RUN',  # Array job: at least one subjob has started
        'E': 'COM',  # Job is exiting after having run.
        'F': 'COM',  # Job is finished.
        'H': 'PEN',  # Job is held.
        'M': 'PEN',  # Job was moved to another server.
        'Q': 'PEN',  # Job is queued.
        'R': 'RUN',  # Job is running.
        'S': 'ABT',  # Job is suspended.
        'T': 'PEN',  # Job is being moved to a new location.
        'U': 'ABT',  # Cycle-harvesting job is suspended due to keyboard activity.
        'W': 'PEN',  # Job is waiting for its submitter-assigned start time to be reached.
        'X': 'PEN',  # Subjob has completed execution or has been deleted.
    }

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
    max_cleanup_time = models.DurationField(null=False, default=dt.timedelta(hours=1))
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

    def __init__(self, *args, **kwargs):
        """
        constructor
        """
        # Build kwargs for PbsScript constructor
        pbs_kwargs = {}

        # Get arguments of PbsScript constructor dynamically
        pbs_signature = inspect.signature(PbsScript.__init__)

        # Get list of fields
        upj_fields = UitPlusJob._meta.get_fields()

        # Handle case when Django models are instantiated manually with kwargs
        if kwargs:
            for param in pbs_signature.parameters.keys():
                if param != 'self':
                    pbs_kwargs[param] = kwargs.get(param, None)

        # When a Django model loads objects from the database, it passes in args, not kwargs
        if len(args) == len(upj_fields):
            # Get list of field names in the order Django passes them in
            all_field_names = []
            for field in upj_fields:
                all_field_names.append(field.name)

            # Match up given arg values with field names
            for field_name, value in zip(all_field_names, args):
                if field_name in pbs_signature.parameters:
                    pbs_kwargs[field_name] = value

        PbsScript.__init__(self, **pbs_kwargs)

        TethysJob.__init__(self, *args, **kwargs)


    @property
    def job_script_name(self):
        try:
            return os.path.split(self.job_script)[-1]
        except (AttributeError, IndexError):
            return ''

    @property
    def token(self):
        if not getattr(self, '_token', None) or self._token is None:
            try:
                social = self.user.social_auth.get(provider='UITPlus')
                self._token = social.extra_data['access_token']
            except (KeyError, AttributeError):
                self._token = None
        return self._token

    @property
    def remote_workspace_suffix(self):
        if not self._remote_workspace:
            workspace_path = os.path.join(self.label, self.name, str(self._remote_workspace_id))
            self._remote_workspace = workspace_path
        return self._remote_workspace

    # job work directory
    @property
    def work_dir(self):
        if not getattr(self, '_work_dir', None):
            WORKDIR = self.get_environment_variable('WORKDIR')
            self._work_dir = os.path.join(WORKDIR, self.remote_workspace_suffix)
        return self._work_dir

    # job archive directory
    @property
    def archive_dir(self):
        if not getattr(self, '_archive_dir', None):
            ARCHIVE_HOME = self.get_environment_variable('ARCHIVE_HOME')
            self._archive_dir = os.path.join(ARCHIVE_HOME, self.remote_workspace_suffix)
        return self._archive_dir

    # job home directory
    @property
    def home_dir(self):
        if not getattr(self, '_home_dir', None):
            HOME = self.get_environment_variable('HOME')
            self._home_dir = os.path.join(HOME, self.remote_workspace_suffix)
        return self._home_dir

    @property
    def client(self):
        if not getattr(self, '_client', None) or self._client is None:
            # Create a client with token
            self._client = Client(token=self.token)

            # Connect the client
            self._client.connect(system=self.system)

        # return the client
        return self._client

    def invoke_on_client(self, method, retries=3, **kwargs):
        """
        Robust wrapper around client methods. Will retry, reties times if failed due to DP routing error.
        """
        # Validate method is method on client
        attempts = 1
        client_method = getattr(self.client, method, None)

        if not client_method or not isinstance(client_method, types.MethodType):
            raise ValueError('{} is not a valid method of UIT Client.')

        last_exception = None

        while attempts <= retries:
            try:
                ret = client_method(**kwargs)
                return ret
            except RuntimeError as e:
                # "DP Route error" indicates failure of SSH Tunnel client on UIT Plus server.
                # Successive calls should work.
                if 'DP Route error' in str(e):
                    attempts += 1
                    last_exception = e
                    continue
                else:
                    # Raise other Runtime Errors
                    raise

        kwarg_str = ', '.join(['{}="{}"'.format(k, v) for k, v in kwargs.items()])
        raise RuntimeError('Max number of retries reached without success for '
                           'method: {}({}). Last exception encountered: {}'.format(method, kwarg_str, last_exception))

    def get_environment_variable(self, variable):
        """
        Get the value of an environment variable.
        :Args:
            variable(str): name of environment variable (e.g.: "WORKDIR").

        Returns:
            str: value of environment variable.
        """
        command = 'echo ${}'.format(variable)
        ret = self.invoke_on_client('call', command=command, work_dir='/tmp')
        return ret.strip()

    def render_execution_block(self):
        cleanup_walltime = strfdelta(self.max_cleanup_time, '%H:%M:%S')

        context = {
            'job_work_dir': self.work_dir,
            'job_archive_dir': self.archive_dir,
            'job_home_dir': self.home_dir,
            'executable': self.job_script_name,
            'project_id': self.project_id,
            'cleanup_walltime': cleanup_walltime,
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

    def _execute(self):
        # Get client
        client = self.client

        # Setup working directory on supercomputer
        command = 'mkdir -p ' + self.work_dir
        ret = self.invoke_on_client(method='call', command=command, work_dir='/tmp')

        # if not ret:
        #     raise RuntimeError('An error occurred while setting up job directory on {}'.format(self.system))

        # Transfer any files listed in transfer_input_files to work_dir on supercomputer
        for transfer_file in self.transfer_input_files:
            transfer_file_name = os.path.split(transfer_file)[-1]
            remote_path = os.path.join(self.work_dir, transfer_file_name)
            ret = self.invoke_on_client('put_file', local_path=transfer_file, remote_path=remote_path)

            if 'success' in ret and ret['success'] == 'false':
                self._status = 'ERR'
                self.save()
                raise RuntimeError('An exception occurred while transferring input files: {}'.format(ret['error']))

        # Transfer the job_script to the work_dir on supercomputer
        if self.transfer_job_script:
            remote_path = os.path.join(self.work_dir, self.job_script_name)
            ret = self.invoke_on_client('put_file', local_path=self.job_script, remote_path=remote_path)

            if 'success' in ret and ret['success'] == 'false':
                self._status = 'ERR'
                self.save()
                raise RuntimeError('An exception occurred while transferring the job script: {}'.format(ret['error']))

        # Set the execution block
        self.execution_block = self.render_execution_block()

        # Submit job with PbsScript object and remote workspace
        job_id = client.submit(self, self.work_dir)

        # Save job id to job_id
        self.job_id = job_id
        self.save()

    def _parse_status(self, status_string):
        """
        Parse status string returned from qstat command.

        Args:
            status_string(str): stdout from qstat command.

        Returns:
            str: TethysJob status string.
        """
        # EXAMPLE:
        # Job id    Name    User    Time    Use S   Queue
        # --------  -----   ------- ------  --- -   ------
        # 2924080.topaz10   rdp nswain  00:11:59    R   debug
        try:
            lines = status_string.split('\n')
            status_line = lines[2]
            cols = status_line.split()
            status = cols[4].strip()
            return self.UIT_TO_TETHYS_STATUSES[status]

        except (IndexError,):
            return 'ERR'

    def _update_status(self):
        # Get status using qstat.
        pbs_command = 'qstat ' + self.job_id
        ret = self.invoke_on_client(method='call', command=pbs_command, work_dir=self.work_dir)

        if ret['success'] == 'true':
            status = self._parse_status(ret['stdout'])
        else:
            status = 'ERR'

        self._status = status
        self.save()

    def _process_results(self, token):
        # Get client using get_client() method
        client = self.client
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
        work_directory_json_response = self.invoke_on_client('list_dir', path=self.work_dir)
        if work_directory_json_response:
            self.get_remote_file(remote_files_path=self.transfer_output_files,
                                 local_path=job_transfer_output_files)
        else:
            # Get transfer_output_files from home_dir when work_dir doesn't exist
            home_directory_json_response = self.invoke_on_client('list_dir', path=self.home_dir)
            if home_directory_json_response:
                self.get_remote_file(remote_files_path=self.transfer_output_files,
                                     local_path=job_transfer_output_files)

    def get_remote_file(self, remote_files_path, local_path):
        for remote_file_path in remote_files_path:
            ret = self.invoke_on_client('get_file', remote_path=remote_file_path, local_path=local_path)

        return ret['success'] == 'true'

    def stop(self):
        # delete the job
        pbs_command = 'qdel ' + self.job_id
        ret = self.invoke_on_client(method='call', command=pbs_command, work_dir=self.work_dir)
        # TODO: CHECK SUCCESS

    def pause(self):
        # hold the job
        pbs_command = 'qhold ' + self.job_id
        ret = self.invoke_on_client(method='call', command=pbs_command, work_dir=self.work_dir)
        # TODO: CHECK SUCCESS

    def resume(self):
        # resume the job
        pbs_command = 'qrls ' + self.job_id
        ret = self.invoke_on_client(method='call', command=pbs_command, work_dir=self.work_dir)
        # TODO: CHECK SUCCESS

    def clean(self, archive=False):
        # Get client
        client = self.client

        # clean the job directories
        pbs_clean_script = self.render_clean_block(archive)
        client.submit(pbs_clean_script, self.work_dir)

    def render_clean_block(self, archive=False):
        cleanup_walltime = strfdelta(self.max_cleanup_time, '%H:%M:%S')

        context = {
            'job_work_dir': self.work_dir,
            'project_id': self.project_id,
            'job_archive_dir': self.archive_dir,
            'job_home_dir': self.home_dir,
            'archive': archive,
            'cleanup_walltime': cleanup_walltime,
        }

        bash_file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'uit_plus_job', 'resources')

        clean_block_path = os.path.join(bash_file_path, 'cleanblock.sh')

        with open(clean_block_path, 'r') as clean_block_file:
            text = clean_block_file.read()
            template = Template(text)
            clean_block = template.render(context)

        return clean_block
