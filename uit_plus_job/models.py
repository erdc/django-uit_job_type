# Put your persistent store models in this file
import os
import uuid
import types
import inspect
import logging
import datetime as dt
from pathlib import Path
from django.db import models
from picklefield import PickledObjectField
from jinja2 import Template
from uit.exceptions import DpRouteError
from uit.uit import Client
from uit.pbs_script import PbsScript, PbsDirective
from tethys_compute.models.tethys_job import TethysJob
from uit_plus_job.util import strfdelta

log = logging.getLogger('tethys.' + __name__)

class UitPlusJob(PbsScript, TethysJob):
    """
    UIT+ Job type.
    """
    UIT_TO_TETHYS_STATUSES = {
        'B': 'RUN',  # Array job: at least one subjob has started
        'E': 'COM',  # Job is exiting after having run.
        'F': 'COM',  # Job is finished.
        'H': 'SUB',  # Job is held.
        'M': 'SUB',  # Job was moved to another server.
        'Q': 'SUB',  # Job is queued.
        'R': 'RUN',  # Job is running.
        'S': 'ABT',  # Job is suspended.
        'T': 'SUB',  # Job is being moved to a new location.
        'U': 'ABT',  # Cycle-harvesting job is suspended due to keyboard activity.
        'W': 'SUB',  # Job is waiting for its submitter-assigned start time to be reached.
        'X': 'RUN',  # Subjob has completed execution or has been deleted.
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
    _remote_workspace_id = models.CharField(max_length=100)
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

        self.save()

        # TODO: Decide where to store the job output files, for now we are using `../../transfer/ + job_name`
        self._local_transfer_dir = os.path.join(str(Path(__file__).parent.parent), 'transfer',
                                                self.name, self.remote_workspace_id)


    @property
    def job_script_name(self):
        """
        returns the job_script name.
        """
        try:
            return os.path.split(self.job_script)[-1]
        except (AttributeError, IndexError):
            return ''

    @property
    def token(self):
        """
        returns the user access token.
        """

        if not getattr(self, '_token', None) or self._token is None:
            try:
                social = self.user.social_auth.get(provider='UITPlus')
                self._token = social.extra_data['access_token']
            except (KeyError, AttributeError):
                self._token = None
        return self._token

    @property
    def remote_workspace_id(self):
        """
        Returns the UUID associated with this job to be used as a workspace io
        """

        if not self._remote_workspace_id:
            self._remote_workspace_id = str(uuid.uuid4())
        return self._remote_workspace_id

    @property
    def remote_workspace_suffix(self):
        """
        returns the job specific suffix
        """
        if not self._remote_workspace:
            workspace_path = os.path.join(self.label, self.name, str(self.remote_workspace_id))
            self._remote_workspace = workspace_path
        return self._remote_workspace

    @property
    def work_dir(self):
        """
        returns the job work directory from super computer
        """
        if not getattr(self, '_work_dir', None):
            WORKDIR = self.get_environment_variable('WORKDIR')
            self._work_dir = os.path.join(WORKDIR, self.remote_workspace_suffix)
        return self._work_dir

    @property
    def archive_dir(self):
        """
        return the job archive directory from super computer
        """
        if not getattr(self, '_archive_dir', None):
            ARCHIVE_HOME = self.get_environment_variable('ARCHIVE_HOME')
            self._archive_dir = os.path.join(ARCHIVE_HOME, self.remote_workspace_suffix)
        return self._archive_dir

    @property
    def home_dir(self):
        """
        returns the job home directory from super computer
        """
        if not getattr(self, '_home_dir', None):
            HOME = self.get_environment_variable('HOME')
            self._home_dir = os.path.join(HOME, self.remote_workspace_suffix)
        return self._home_dir

    @property
    def client(self):
        """
        returns the uit client based on a valid token
        """
        if not getattr(self, '_client', None) or self._client is None:
            # Create a client with token
            self._client = Client(token=self.token)

            # Connect the client
            self._client.connect(system=self.system)

        # return the client
        return self._client

    def get_environment_variable(self, variable):
        """
        Get the value of an environment variable.
        :Args:
            variable(str): name of environment variable (e.g.: "WORKDIR").

        Returns:
            str: value of environment variable.
        """
        command = 'echo ${}'.format(variable)
        ret = self.client.call(command=command, work_dir='/tmp')
        return ret.strip()

    def _execute(self):
        """
        Executes the job using the UIT Plus Python client
        """
        # Get client
        client = self.client

        # Setup working directory on supercomputer
        command = 'mkdir -p ' + self.work_dir
        try:
            self.client.call(command=command, work_dir='/tmp')
        except RuntimeError as e:
            self._status = 'ERR'
            self.save()
            raise RuntimeError('Error setting up job directory on "{}": {}'.format(self.system, str(e))

        # Transfer any files listed in transfer_input_files to work_dir on supercomputer
        for transfer_file in self.transfer_input_files:
            transfer_file_name = os.path.split(transfer_file)[-1]
            remote_path = os.path.join(self.work_dir, transfer_file_name)
            ret = self.client.put_file(local_path=transfer_file, remote_path=remote_path)

            if 'success' in ret and ret['success'] == 'false':
                self._status = 'ERR'
                self.save()
                raise RuntimeError('Failed to transfer input files: {}'.format(ret['error']))

        # Transfer the job_script to the work_dir on supercomputer
        if self.transfer_job_script:
            remote_path = os.path.join(self.work_dir, self.job_script_name)
            ret = self.client.put_file(local_path=self.job_script, remote_path=remote_path)

            if 'success' in ret and ret['success'] == 'false':
                self._status = 'ERR'
                self.save()
                raise RuntimeError('Failed to transfer the job script: {}'.format(ret['error']))

        # Render the execution block
        context = {
            'job_work_dir': self.work_dir,
            'job_archive_dir': self.archive_dir,
            'job_home_dir': self.home_dir,
            'home_input_files': self.home_input_files,
            'archive_input_files': self.archive_input_files,
            'executable': self.job_script_name,
            'project_id': self.project_id,
        }

        resources_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'uit_plus_job', 'resources')
        execution_block_path = os.path.join(resources_dir, 'executionblock.sh')

        with open(execution_block_path, 'r') as f:
            text = f.read()
            template = Template(text)
            self.execution_block = template.render(context)

        # Submit job with PbsScript object and remote workspace
        execute_job_id = client.submit(self, self.work_dir)


        # Render cleanup script
        cleanup_walltime = strfdelta(self.max_cleanup_time, '%H:%M:%S')
        context = {
            'execute_job_id': execute_job_id,
            'job_work_dir': self.work_dir,
            'job_archive_dir': self.archive_dir,
            'job_home_dir': self.home_dir,
            'project_id': self.project_id,
            'cleanup_walltime': cleanup_walltime,
            'archive_output_files': self.archive_output_files,
            'home_output_files': self.home_output_files,
            'transfer_output_files': self.transfer_output_files,
        }

        cleanup_template = os.path.join(resources_dir, 'clean_after_exec.sh')
        with open(cleanup_template, 'r') as f:
            text = f.read()
            template = Template(text)
            cleanup_script = template.render(context)
        self.extended_properties['cleanup_job_id'] = client.submit(cleanup_script, self.work_dir, 'cleanup.pbs')

        self.job_id = execute_job_id
        self._status = 'SUB'
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
            status_line = lines[5]
            cols = status_line.split()
            status = cols[9].strip()
            print("{}->".format(self.UIT_TO_TETHYS_STATUSES[status]), end="")
            return self.UIT_TO_TETHYS_STATUSES[status]

        except (IndexError, AttributeError):
            return 'ERR'

    def _update_status(self):
        """
         Retrieve a job’s status using the UIT Plus Python client.
         Translate UitJob status to TethysJob status and save in database
        """
        # Get status using qstat with -H option to get historical data when job finishes.
        try:
            pbs_command = 'qstat -H ' + self.job_id
            status_string = self.client.call(command=pbs_command, work_dir='/tmp')
        except DpRouteError as e:
            log.info('Ignoring DP_Route error: {}'.format(e))
            return
        except RuntimeError as e:
            log.error('Attempt to get status for job %s failed: %s', self.job_id, str(e))
            self._status = 'ERR'
            return

        new_status = self._parse_status(status_string)

        if new_status == "COM":
            if 'cleanup_job_id' in self.extended_properties:
                if self.job_id != self.extended_properties['cleanup_job_id']:
                    new_status = "SUB"
                    self.job_id = self.extended_properties['cleanup_job_id']
            else:
                raise RuntimeError("Could not find cleanup script ID.")

        print(new_status)
        self._status = new_status
        self.save()

    def _process_results(self):
        """
         Processes the results using the UIT Plus Python client
        """
        # Ensure the local transfer directory exists
        Path(self._local_transfer_dir).mkdir(parents=True, exist_ok=True)
        remote_dir = os.path.join(self.home_dir, 'transfer')
        self.get_remote_files(remote_dir, self.transfer_output_files)
        self.get_remote_files(remote_dir, ["log.stdout", "log.stderr"])



    def get_remote_files(self, remote_dir, remote_filenames):
        """
        Transfers files from job_home_dir using the client.get_file method

        Parameters
        ----------
        remote_dir: str
            the remote directory from which to pull files
        remote_filenames: str
            list of file names to retrieve from remote_dir

        Return
        -------
        Returns True if all file transfers succeed.
        """

        success = True
        for remote_file in remote_filenames:
            local_path = os.path.join(self._local_transfer_dir, remote_file)
            remote_path = os.path.join(remote_dir, remote_file)
            try:
                self.client.get_file(remote_path=remote_path, local_path=local_path)
                if not os.path.exists(local_path):
                    success = False
            except IOError as e:
                success = False
                logging.ERROR("Failed to get remote file: {}".format(str(e)))
        return success

    def stop(self):
        """
        Stops/cancels a job using the UIT Plus Python client
        """
        # delete the job
        pbs_command = 'qdel ' + self.job_id
        try:
            ret = self.client.call(command=pbs_command, work_dir=self.work_dir)
            return True
        except RuntimeError:
            return False

    def pause(self):
        """
        Pauses a job using the UIT Plus Python client
        """
        # hold the job
        pbs_command = 'qhold ' + self.job_id
        try:
            ret = self.client.call(command=pbs_command, work_dir=self.work_dir)
            return True
        except RuntimeError:
            return False

    def resume(self):
        """
        Resumes a paused job using the UIT Plus Python client
        """
        # resume the job
        pbs_command = 'qrls ' + self.job_id
        try:
            ret = self.client.call(command=pbs_command, work_dir=self.work_dir)
            return True
        except RuntimeError:
            return False

    def clean(self, archive=False):
        """
        Remove all files and directories associated with the job
        """
        # Get client
        client = self.client

        # clean the job directories
        pbs_clean_script = self.render_clean_script(archive)
        try:
            # TODO: Refactor to remove home and work dir using client.call() Change cleanblock.sh to only remove the archive directory. Only submit if archive is True
            ret = client.submit(pbs_clean_script, self.work_dir, remote_name='clean.pbs')
            return True
        except RuntimeError:
            return False

    def render_clean_script(self, archive=False):
        """
        Render the execution block from a template using django templating
        Parameters
        ----------
        archive: bool
            Remove files from archive if True. Defaults to False
        """
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
