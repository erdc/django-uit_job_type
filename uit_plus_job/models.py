# Put your persistent store models in this file
import os
import shutil
import threading
import uuid
import inspect
import logging
import datetime as dt
from pathlib import Path
from django.db import models
from django.db.models.signals import pre_delete
from django.dispatch import receiver
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

    archive_input_files = PickledObjectField(default=list)
    archive_output_files = PickledObjectField(default=list)
    home_input_files = PickledObjectField(default=list)
    home_output_files = PickledObjectField(default=list)
    job_id = models.CharField(max_length=1024, null=True)
    job_script = models.TextField(null=False)
    max_cleanup_time = models.DurationField(null=False, default=dt.timedelta(hours=1))
    max_time = models.DurationField(null=False)
    node_type = models.CharField(max_length=10, choices=NODE_TYPE_CHOICES, default='compute', null=False)
    num_nodes = models.IntegerField(default=1, null=False)
    processes_per_node = models.IntegerField(default=1, null=False)
    project_id = models.CharField(max_length=1024, null=False)
    queue = models.CharField(max_length=100, default='debug', null=False)
    system = models.CharField(max_length=10, choices=SYSTEM_CHOICES, default='topaz', null=False)
    transfer_input_files = PickledObjectField(default=list)
    transfer_job_script = models.BooleanField(default=True)
    transfer_output_files = PickledObjectField(default=list)
    _modules = PickledObjectField(default=dict)
    _optional_directives = PickledObjectField(default=list)
    _remote_workspace = models.TextField(blank=True)
    _remote_workspace_id = models.CharField(max_length=100)

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

    @property
    def archive_dir(self):
        """
        return the job archive directory from super computer
        """
        if not getattr(self, '_archive_dir', None):
            archive_home = self.get_environment_variable('ARCHIVE_HOME')
            self._archive_dir = os.path.join(archive_home, self.remote_workspace_suffix)
        return self._archive_dir

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

    @property
    def home_dir(self):
        """
        returns the job home directory from super computer
        """
        if not getattr(self, '_home_dir', None):
            home = self.get_environment_variable('HOME')
            self._home_dir = os.path.join(home, self.remote_workspace_suffix)
        return self._home_dir

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
    def work_dir(self):
        """
        returns the job work directory from super computer
        """
        if not getattr(self, '_work_dir', None):
            workdir = self.get_environment_variable('WORKDIR')
            self._work_dir = os.path.join(workdir, self.remote_workspace_suffix)
        return self._work_dir

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
            raise RuntimeError('Error setting up job directory on "{}": {}'.format(self.system, str(e)))

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
            'execute_job_num': execute_job_id.split('.', 1)[0],
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
        # topaz10:
        #                                                             Req'd  Req'd   Elap
        # Job ID          Username Queue    Jobname    SessID NDS TSK Memory Time  S Time
        # --------------- -------- -------- ---------- ------ --- --- ------ ----- - -----
        # 3101546.topaz10 user     transfer cleanup.pb --     1   1   --     00:05 Q --
        try:
            lines = status_string.split('\n')
            status_line = lines[5]
            cols = status_line.split()
            status = cols[9].strip()
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

        self._status = new_status
        self.save()

    def _process_results(self):
        """
         Processes the results using the UIT Plus Python client
        """
        # Ensure the local transfer directory exists
        Path(self.workspace).mkdir(parents=True, exist_ok=True)
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
        remote_filenames: List[str]
            list of file names to retrieve from remote_dir

        Return
        -------
        Returns True if all file transfers succeed.
        """

        success = True
        for remote_file in remote_filenames:
            local_path = os.path.join(self.workspace, remote_file)
            remote_path = os.path.join(remote_dir, remote_file)
            try:
                self.client.get_file(remote_path=remote_path, local_path=local_path)
                if not os.path.exists(local_path):
                    success = False
            except RuntimeError as e:
                success = False
                log.error("Failed to get remote file: {}".format(str(e)))
                with open(local_path, 'w+') as f:
                    print("Could not transfer file: {}".format(str(e)), file=f)

        return success

    def stop(self):
        """
        Stops/cancels a job using the UIT Plus Python client
        """
        # delete the job
        pbs_command = 'qdel ' + self.job_id
        try:
            self.client.call(command=pbs_command, work_dir=self.work_dir)
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
            self.client.call(command=pbs_command, work_dir=self.work_dir)
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
            self.client.call(command=pbs_command, work_dir=self.work_dir)
            return True
        except RuntimeError:
            return False

    def clean(self, archive=False):
        """
        Remove all files and directories associated with the job
        """

        # Remove local workspace
        thread = threading.Thread(target=shutil.rmtree, args=(self.workspace, True))
        thread.daemon = True
        thread.start()

        # Remove remote locations
        rm_cmd = "rm -rf {} || true"
        commands = []
        for path in (self.work_dir, self.home_dir):
            # TODO: We should probably change this to figure out the actual remote workspace path, instead of
            #  assuming it is one above our work/home path.
            commands.append(rm_cmd.format(os.path.abspath(os.path.join(path, '..'))))
        if archive:
            commands.append("archive rm -rf {} || true".format(self.archive_dir))

        for cmd in commands:
            thread = threading.Thread(target=self.client.call, kwargs={'command': cmd, 'work_dir': '/'})
            thread.daemon = True
            thread.start()
            log.info("Executing command '{}' on topaz".format(cmd))
        return True


@receiver(pre_delete, sender=UitPlusJob)
def uit_job_pre_delete(sender, instance, using, **kwargs):
    """
    Pre-delete hook to make sure we clean up our workspace

    Args:
        sender: The model's class
        instance: The instance being deleted
        using: The DB alias in use
        **kwargs:

    Returns:
        Nothing

    """
    try:
        instance.clean()
    except Exception as e:
        log.exception(str(e))