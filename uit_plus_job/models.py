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
from django.utils import timezone
from picklefield import PickledObjectField
from jinja2 import Template
from tethys_apps.base.function_extractor import TethysFunctionExtractor
from uit.exceptions import DpRouteError
from uit.uit import Client
from uit.pbs_script import PbsScript
from tethys_compute.models.tethys_job import TethysJob
from uit_plus_job.util import strfdelta


log = logging.getLogger('tethys.' + __name__)


class UitPlusJob(PbsScript, TethysJob):
    """UIT+ Job type for use in Tethys Apps.

    Attributes:
        archive_input_files (list): files to transfer from the archive filesystem to the working directory prior to running the job.
        archive_output_files (list): files to transfer from the working directory to the archive filesystem after the job has finished running.
        home_input_files (list): files to transfer from the user's home directory to the working directory prior to running the job.
        home_output_files (list): files to transfer from the working directory to the user's home directory after the job has finished running.
        intermediate_transfer_interval (int): frequency in minutes to transfer intermediate results.
        job_id (str): id of the job assigned by PBS.
        job_script (str): path to PBS script for the job.
        last_intermediate_transfer (datetime): the last date and time an intermediate data transfer occurred.
        max_cleanup_time (duration): maximum amount of time in minutes the cleanup job should be allowed to run.
        max_time (duration): maximum amount of time in minutes the job should be allowed to run.
        node_type (str): type of node on which the job should run.
        num_nodes (int): number of nodes to request.
        processes_per_node (int): number of processors per node to request.
        project_id (str): project ID to be passed in the PBS Header.
        queue (str): name of the queue into which to submit the job.
        system (str): name of the system to run on.
        transfer_input_files (list): files to transfer from the job workspace in the app to the working directory prior to running the job.
        transfer_intermediate_files (list): files to transfer to the job workspace in the app each intermediate_transfer_interval
        transfer_job_script (bool): transfer the job_script from the app to the working directory when True. Defaults to True.
        transfer_output_files (list): files to transfer from the working directory to the job workspace in the app after the job has finished running
    """  # noqa: E501
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

    # TODO derive these choices from pyuit
    SYSTEM_CHOICES = (
        ('jim', 'jim'),
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
    intermediate_transfer_interval = models.IntegerField(default=0, null=False)
    job_id = models.CharField(max_length=1024, null=True)
    job_script = models.TextField(null=False)
    last_intermediate_transfer = models.DateTimeField(null=False, default=timezone.now)
    max_cleanup_time = models.DurationField(null=False, default=dt.timedelta(hours=1))
    max_time = models.DurationField(null=False)
    node_type = models.CharField(max_length=10, choices=NODE_TYPE_CHOICES, default='compute', null=False)
    num_nodes = models.IntegerField(default=1, null=False)
    processes_per_node = models.IntegerField(default=1, null=False)
    project_id = models.CharField(max_length=1024, null=False)
    queue = models.CharField(max_length=100, default='debug', null=False)
    system = models.CharField(max_length=10, choices=SYSTEM_CHOICES, default='topaz', null=False)
    transfer_input_files = PickledObjectField(default=list)
    transfer_intermediate_files = PickledObjectField(default=list)
    transfer_job_script = models.BooleanField(default=True)
    transfer_output_files = PickledObjectField(default=list)
    _modules = PickledObjectField(default=dict)
    _optional_directives = PickledObjectField(default=list)
    _process_intermediate_results_function = models.CharField(max_length=1024, null=True)
    _remote_workspace = models.TextField(blank=True)
    _remote_workspace_id = models.CharField(max_length=100)
    # TODO integrate remote_workpsace/remote_workspace_id with pyuit?
    # TODO add _environment_variables to DB

    def __init__(self, *args, **kwargs):
        """Constructor."""
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

        self._client = None
        self._home_dir = None
        self._token = None
        self._archive_dir = None
        self._work_dir = None

        self.save()

    @property
    def archive_dir(self):
        """Get the job archive directory from the HPC.

        Returns:
            str: Archive Directory
        """
        if self._archive_dir is None:
            archive_home = self.get_environment_variable('ARCHIVE_HOME')
            self._archive_dir = os.path.join(archive_home, self.remote_workspace_suffix)
        return self._archive_dir

    @property
    def client(self):
        """Get the UIT client based on a valid token.

        Returns:
            Client: UIT Client object
        """
        if self._client is None:
            # Create a client with token
            self._client = Client(token=self.token)

            # Connect the client
            self._client.connect(system=self.system)

        # return the client
        return self._client

    @property
    def home_dir(self):
        """Get the job home directory from the HPC.

        Returns:
            str: The job home directory
        """
        if self._home_dir is None:
            home = self.get_environment_variable('HOME')
            self._home_dir = os.path.join(home, self.remote_workspace_suffix)
        return self._home_dir

    @property
    def job_script_name(self):
        """Get the job_script name.

        Returns:
            str: The job script name
        """
        try:
            return os.path.split(self.job_script)[-1]
        except (AttributeError, IndexError):
            return ''

    @property
    def process_intermediate_results_function(self):
        """Get the function used to process intermediate results.

        Returns:
            Function: The process function, or None if the function cannot be resolved.
        """
        if self._process_intermediate_results_function:
            function_extractor = TethysFunctionExtractor(
                self._process_intermediate_results_function, None)
            if function_extractor.valid:
                return function_extractor.function

    @process_intermediate_results_function.setter
    def process_intermediate_results_function(self, function):
        if isinstance(function, str):
            self._process_results_function = function
            return
        module_path = inspect.getmodule(function).__name__.split('.')
        module_path.append(function.__name__)
        self._process_results_function = '.'.join(module_path)

    @property
    def remote_workspace_id(self):
        """Get the UUID associated with this job to be used as a workspace id.

        Returns:
            str: Remote workspace ID
        """

        if not self._remote_workspace_id:
            self._remote_workspace_id = str(uuid.uuid4())
        return self._remote_workspace_id

    @property
    def remote_workspace_suffix(self):
        """Get the job specific suffix.

        Made up of a combination of label, name, and remote workspace ID.

        Returns:
            str: Suffix
        """
        if not self._remote_workspace:
            self._remote_workspace = os.path.join(self.label, self.name, str(self.remote_workspace_id))
        return self._remote_workspace

    @property
    def token(self):
        """Get the user access token.

        Returns:
            str: Access Token
        """
        if self._token is None:
            try:
                social = self.user.social_auth.get(provider='UITPlus')
                self._token = social.extra_data['access_token']
            except (KeyError, AttributeError):
                self._token = None
        return self._token

    @property
    def work_dir(self):
        """Get the job work directory from the HPC.

        Returns:
            str: Work Directory
        """
        if self._work_dir is None:
            workdir = self.get_environment_variable('WORKDIR')
            self._work_dir = os.path.join(workdir, self.remote_workspace_suffix)
        return self._work_dir

    def get_environment_variable(self, variable):
        """Get the value of an environment variable from the HPC.

        Args:
            variable (str): Name of environment variable (e.g.: "WORKDIR").

        Returns:
            str: value of environment variable.
        """
        return self.client.env.get(variable)

    def _execute(self, remote_name=None):
        """Execute the job using the UIT Plus Python client."""
        # Get client
        client = self.client

        self._work_dir = self.work_dir
        remote_name = remote_name or f'{self.remote_workspace_id}.pbs'

        # Setup working directory on supercomputer
        command = 'mkdir -p ' + self.work_dir
        try:
            self.client.call(command=command)
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
        execute_job_id = client.submit(self, working_dir=self.work_dir, remote_name=remote_name)
        self.cleanup = False  # TODO rethink how cleanup should work
        if self.cleanup:
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
            self.extended_properties['cleanup_job_id'] = client.submit(cleanup_script, self.work_dir, f'cleanup.{execute_job_id}.pbs')

        self.job_id = execute_job_id
        self._status = 'SUB'
        self.save()

    def _parse_status(self, status_string):
        """Parse status string returned from qstat command.

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
        """Retrieve a job’s status using the UIT Plus Python client.

        Translates UitJob status to TethysJob status and saves to the database
        """
        # TODO can we leverage the code from pyuit.Job here?
        # Get status using qstat with -H option to get historical data when job finishes.
        try:
            pbs_command = 'qstat -H ' + self.job_id
            status_string = self.client.call(command=pbs_command, working_dir='/tmp')
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
            # else:
            #     raise RuntimeError("Could not find cleanup script ID.")

        self._status = new_status
        self.save()

        # Get intermediate results, if applicable
        if self.transfer_intermediate_files:
            if self.intermediate_transfer_interval == 0 \
                    or (timezone.now() - self.last_intermediate_transfer).minute > \
                    self.intermediate_transfer_interval:
                self.last_intermediate_transfer = timezone.now()
                thread = threading.Thread(target=self.get_intermediate_results)
                thread.daemon = True
                thread.start()
        self.save()

    def _process_results(self):
        """Process the results using the UIT Plus Python client."""
        remote_dir = os.path.join(self.home_dir, 'transfer')
        remote_dir = self.work_dir
        self.get_remote_files(remote_dir, self.transfer_output_files)
        # self.get_remote_files(remote_dir, ["log.stdout", "log.stderr"])
        # TODO get log files

    def get_intermediate_results(self):
        """Retrieve intermediate result files from the supercomputer."""
        if self.get_remote_files(self.work_dir, self.transfer_intermediate_files):
            if self.process_intermediate_results_function:
                self.process_intermediate_results_function()

    def get_remote_files(self, remote_dir, remote_filenames):
        """Transfer files from a directory on the super computer.

        Args:
            remote_dir (str): Remote directory from which to pull files
            remote_filenames (List[str]): Files to retrieve from remote_dir

        Returns:
            bool: True if all file transfers succeed.
        """

        # Ensure the local transfer directory exists
        Path(self.workspace).mkdir(parents=True, exist_ok=True)

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
        """Stops/cancels this job.

        Returns:
            bool: True if job was deleted.
        """
        # delete the job
        pbs_command = 'qdel ' + self.job_id
        try:
            self.client.call(command=pbs_command, working_dir=self.work_dir)
            return True
        except RuntimeError:
            return False

    def pause(self):
        """Pauses this job.

        Returns:
            bool: True if job was paused.
        """
        # hold the job
        pbs_command = 'qhold ' + self.job_id
        try:
            self.client.call(command=pbs_command, working_dir=self.work_dir)
            return True
        except RuntimeError:
            return False

    def resume(self):
        """Resumes this job if paused.

        Returns:
            bool: True if job was resumed.
        """
        # resume the job
        pbs_command = 'qrls ' + self.job_id
        try:
            self.client.call(command=pbs_command, working_dir=self.work_dir)
            return True
        except RuntimeError:
            return False

    def clean(self, archive=False):
        """Remove all files and directories associated with the job.

        Removal takes place on unmonitored background thread so as not to disturb the user (as deletes on the HPC can take a long time). This means that we will always return True even if the files were not deleted.

        Args:
            archive (bool): Flag to indicate whether files should be removed from the archive as well.

        Returns:
            bool: True. Always.
        """  # noqa: E501

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
            thread = threading.Thread(target=self.client.call, kwargs={'command': cmd, 'working_dir': '/'})
            thread.daemon = True
            thread.start()
            log.info(f"Executing command '{cmd}' on {self.system}")
        return True


@receiver(pre_delete, sender=UitPlusJob)
def uit_job_pre_delete(sender, instance, using, **kwargs):
    """Pre-delete hook to make sure we clean up our workspace.

    Args:
        sender: The model's class
        instance: The instance being deleted
        using: The DB alias in use
        **kwargs:
    """
    try:
        instance.stop()
        instance.clean()
    except Exception as e:
        log.exception(str(e))
