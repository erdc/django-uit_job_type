# Put your persistent store models in this file
import asyncio
import os
import posixpath
import re
import shutil
import inspect
import logging
import datetime as dt
from collections import OrderedDict
from pathlib import Path, PurePosixPath
from functools import partial, wraps

from channels.db import database_sync_to_async
from django.db import models
from django.utils import timezone
from django.db.models import JSONField
from django.contrib.auth.models import User
from social_django.utils import load_strategy
from tethys_apps.base.function_extractor import TethysFunctionExtractor
from uit.exceptions import UITError
from uit import AsyncClient, PbsScript, PbsJob, PbsArrayJob
from uit.pbs_script import PbsDirective
from tethys_compute.models.tethys_job import TethysJob


log = logging.getLogger("tethys." + __name__)


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
        None: "PEN",  # No status set so job was just created
        "B": "RUN",  # Array job: at least one subjob has started
        "E": "COM",  # Job is exiting after having run.
        "F": "COM",  # Job is finished.
        "H": "PAS",  # Job is held.
        "M": "SUB",  # Job was moved to another server.
        "Q": "SUB",  # Job is queued.
        "R": "RUN",  # Job is running.
        "S": "ABT",  # Job is suspended.
        "T": "SUB",  # Job is being moved to a new location.
        "U": "ABT",  # Cycle-harvesting job is suspended due to keyboard activity.
        "W": "SUB",  # Job is waiting for its submitter-assigned start time to be reached.
        "X": "RUN",  # Subjob has completed execution or has been deleted.
    }

    TETHYS_STATUSES_TO_UIT = {
        "PEN": None,
        "COM": "F",
        "PAS": "H",
        "SUB": "Q",
        "RUN": "R",
        "ABT": "S",
        "ERR": "F",
    }

    # job vars
    job_id = models.CharField(max_length=1024, null=True)
    archive_input_files = JSONField(blank=True, default=list, null=True)
    home_input_files = JSONField(blank=True, default=list, null=True)
    transfer_input_files = JSONField(blank=True, default=list, null=True)
    _remote_workspace = models.TextField(blank=True)
    _remote_workspace_id = models.CharField(max_length=100)
    _base_dir = models.TextField(blank=True)
    qstat = JSONField(default=dict, null=True)
    archived = models.BooleanField(default=False)

    # pbs_script vars
    project_id = models.CharField(max_length=1024, null=False)
    num_nodes = models.IntegerField(default=1, null=False)
    processes_per_node = models.IntegerField(default=1, null=False)
    _max_time = models.DurationField(null=False)
    queue = models.CharField(max_length=100, default="debug", null=False)
    node_type = models.CharField(max_length=10, default="compute", null=False)
    system = models.CharField(max_length=10, null=False)
    execution_block = models.TextField(null=False)
    _modules = JSONField(default=dict, null=True)
    _module_use = JSONField(default=dict, null=True)
    _optional_directives = JSONField(blank=True, default=list, null=True)
    _environment_variables = JSONField(default=dict, null=True)
    _array_indices = JSONField(blank=True, default=list, null=True)

    # other
    archive_output_files = JSONField(blank=True, default=list, null=True)
    home_output_files = JSONField(blank=True, default=list, null=True)
    intermediate_transfer_interval = models.IntegerField(default=0, null=False)
    last_intermediate_transfer = models.DateTimeField(null=False, default=timezone.now)
    max_cleanup_time = models.DurationField(null=False, default=dt.timedelta(hours=1))
    transfer_intermediate_files = JSONField(blank=True, default=list, null=True)
    transfer_job_script = models.BooleanField(default=True)
    transfer_output_files = JSONField(blank=True, default=list, null=True)
    custom_logs = JSONField(default=dict, null=False)
    _process_intermediate_results_function = models.CharField(max_length=1024, null=True)
    _update_status_interval = dt.timedelta(seconds=30)  # This is not effective until jobs table uses WS

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
                if param != "self":
                    pbs_kwargs[param] = kwargs.get(param, None)

        # When a Django model loads objects from the database, it passes in args, not kwargs
        if len(args) + 1 == len(upj_fields):
            # Get list of field names in the order Django passes them in
            all_field_names = [field.name for field in upj_fields if field.name != "tethysjob_ptr"]
            all_field_names[all_field_names.index("_max_time")] = "max_time"
            # Match up given arg values with field names
            for field_name, value in zip(all_field_names, args):
                if field_name in pbs_signature.parameters:
                    pbs_kwargs[field_name] = value

        try:
            PbsScript.__init__(self, **pbs_kwargs)
            self._system_decommissioned = False
        except ValueError as e:
            if e.args[0].startswith(f'"{self.system}"'):
                # system is no longer supported
                self._system_decommissioned = True
            else:
                raise e

        # Some database fields get dropped if TethysJob is initialized before PbsScript
        TethysJob.__init__(self, *args, **kwargs)

        if self._system_decommissioned:
            self.status = "Purged"  # Must be set after TethysJob.__init__

        self._client = None
        self._token = None
        self._home_dir = None
        self._archive_dir = None
        self._working_dir = None
        self._pbs_job = None

        self.remote_workspace_suffix  # initialize "remote" variables

        self.save()

    def __str__(self):
        return TethysJob.__str__(self)

    @staticmethod
    def _ensure_connected(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            if not self.client.connected:
                await self.get_token()
                self.client.token = self.token
                await self.client.get_userinfo()
                await self.client.connect(self.system, retry_on_failure=True)

            return await func(self, *args, **kwargs)

        return wrapper

    @_ensure_connected
    async def connect(self):
        """
        A no-op method to allow the _ensure_connected decorator to be triggered manually.
        """
        pass

    @database_sync_to_async
    def _safe_save(self):
        self.save()

    async def safe_close(self):
        if self._client is not None:
            await self.client.safe_close()

    @database_sync_to_async
    def _safe_delete(self, using, keep_parents):
        super().delete(using, keep_parents)

    @classmethod
    def instance_from_pbs_job(cls, job, user):
        script = job.script
        instance = cls(
            name=job.name,
            user=user,
            label=job.label,
            workspace=job.workspace.as_posix(),
            description=job.description,
            extended_properties=job.metadata,
            job_id=job.job_id,
            _status=cls.UIT_TO_TETHYS_STATUSES.get(job.status),
            qstat=job.qstat,
            project_id=script.project_id,
            system=script.system,
            node_type=script.node_type,
            num_nodes=script.num_nodes,
            processes_per_node=script.processes_per_node,
            queue=script.queue,
            max_time=script.max_time,
            execution_block=script.execution_block,
            _optional_directives=script._optional_directives,
            _modules=script._modules,
            _module_use=script._module_use,
            _array_indices=script._array_indices,
            # max_cleanup_time=None,
            home_input_files=job.home_input_files,
            home_output_files=[],
            archive_input_files=job.archive_input_files,
            archive_output_files=[],
            transfer_input_files=job.transfer_input_files,
            transfer_intermediate_files=[],
            transfer_output_files=[],
            _remote_workspace_id=job._remote_workspace_id,
            _remote_workspace=job._remote_workspace,
            _base_dir=job._base_dir,
        )
        instance.environment_variables = script._environment_variables
        instance._pbs_job = job
        # Note that this preserves information that is not serialized in the database (like post_processing_script)
        # Non-serialized attributes will be accessible while the object is in memory, but not from an object that is
        # reconstructed from the database.

        return instance

    @property
    def pbs_job(self):
        if self._system_decommissioned:
            raise RuntimeError("The PBS Job is not available on jobs from systems that have been decommissioned.")
        if self._pbs_job is None:
            Job = PbsJob if not self._array_indices else PbsArrayJob
            j = Job(
                script=self,
                client=self.client,
                label=self.label,
                workspace=Path(self.workspace),
                transfer_input_files=self.transfer_input_files,
                home_input_files=self.home_input_files,
                archive_input_files=self.archive_input_files,
                description=self.description,
                metadata=self.extended_properties,
                base_dir=self._base_dir if self._base_dir else None,
            )
            j._remote_workspace_id = self._remote_workspace_id
            j._remote_workspace = PurePosixPath(self._remote_workspace)
            j._job_id = self.job_id
            j._status = self.TETHYS_STATUSES_TO_UIT.get(self._status)
            j._qstat = self.qstat
            j._post_processing_job_id = self.extended_properties.get("post_processing_job_id")
            if self._array_indices and self.qstat is not None:
                for sub_job in j.sub_jobs:
                    sub_job._qstat = self.qstat.get(sub_job.job_id)
                    sub_job._status = sub_job.qstat.get("status")
            self._pbs_job = j
        return self._pbs_job

    @property
    def environment_variables(self):
        return OrderedDict(self._environment_variables)

    @environment_variables.setter
    def environment_variables(self, ordered_dict):
        self._environment_variables = [[k, v] for k, v in ordered_dict.items()]

    @property
    def optional_directives(self):
        """Get a list of all defined directives.

        Returns:
             list: All defined directives.
        """
        return [self.parse_pbs_directive(d) for d in self._optional_directives]

    @staticmethod
    def parse_pbs_directive(directive_str):
        if isinstance(directive_str, (tuple, list)):
            return PbsDirective(*directive_str)
        if isinstance(directive_str, PbsDirective):
            return directive_str
        m = re.match(r"PbsDirective\(directive='(.*?)', options='(.*?)'\)", directive_str)
        return PbsDirective(*m.groups())

    async def get_archive_dir(self):
        """Get the job archive directory from the HPC.

        Returns:
            str: Archive Directory
        """
        if self._archive_dir is None:
            archive_home = await self.get_environment_variable("ARCHIVE_HOME")
            self._archive_dir = posixpath.join(archive_home, self.remote_workspace_suffix)
        return self._archive_dir

    @property
    def workflow_type(self):
        return self.label.split("/")[-1]

    @property
    def client(self):
        """Get the UIT client based on a valid token.

        Returns:
            Client: UIT Client object
        """
        if self._client is None:
            # Create a client with token
            self._client = AsyncClient()

            # Connect the client
            # self._client.connect(system=self.system, retry_on_failure=True)

        # return the client
        return self._client

    @property
    def home_dir(self):
        """Get the job home directory from the HPC.

        Returns:
            str: The job home directory
        """
        if self._home_dir is None:
            self._home_dir = self.client.HOME / self.remote_workspace_suffix
        return self._home_dir

    @property
    def process_intermediate_results_function(self):
        """Get the function used to process intermediate results.

        Returns:
            Function: The process function, or None if the function cannot be resolved.
        """
        if self._process_intermediate_results_function:
            function_extractor = TethysFunctionExtractor(self._process_intermediate_results_function, None)
            if function_extractor.valid:
                return function_extractor.function

    @process_intermediate_results_function.setter
    def process_intermediate_results_function(self, function):
        if isinstance(function, str):
            self._process_results_function = function
            return
        module_path = inspect.getmodule(function).__name__.split(".")
        module_path.append(function.__name__)
        self._process_results_function = ".".join(module_path)

    @property
    def remote_workspace_id(self):
        """Get the UUID associated with this job to be used as a workspace id.

        Returns:
            str: Remote workspace ID
        """
        if not self._remote_workspace_id:
            self._remote_workspace_id = self.pbs_job.remote_workspace_id
        return self._remote_workspace_id

    @property
    def remote_workspace_suffix(self):
        """Get the job specific suffix.

        Made up of a combination of label, name, and remote workspace ID.

        Returns:
            str: Suffix
        """
        if not self._remote_workspace:
            self._remote_workspace = self.pbs_job.remote_workspace_suffix
        return self._remote_workspace

    @database_sync_to_async
    def get_token(self):
        try:
            social = self.user.social_auth.get(provider="UITPlus")
            self._token = social.get_access_token(load_strategy())
        except (KeyError, AttributeError):
            self._token = None

    @property
    def token(self):
        """Get the user access token.

        Returns:
            str: Access Token
        """
        if self._token is None:
            raise RuntimeError('The "get_token" method must be awaited before retreiving the token.')
        return self._token

    @property
    def working_dir(self):
        """Get the job work directory from the HPC.

        Returns:
            str: Work Directory
        """
        return self.pbs_job.working_dir

    async def is_job_archived(self):
        archive_filename = f"job_{self.remote_workspace_id}.run_files.tar.gz"
        archive_files = self.client.list_dir(await self.get_archive_dir()).get("files", [])
        return archive_filename in [file["name"] for file in archive_files]

    @_ensure_connected
    async def get_logs(self):
        if isinstance(self.pbs_job, PbsArrayJob):
            logs = {}
            for sub_job in self.pbs_job.sub_jobs:
                name = f"{sub_job.name}_{sub_job.job_index}"
                logs[name] = {}
                logs[name]["stdout"] = sub_job.get_stdout_log
                logs[name]["stderr"] = sub_job.get_stderr_log
                logs[name].update(
                    {
                        log_type: partial(sub_job.get_cached_file_contents, path, bytes=100_000)
                        for log_type, path in self.custom_logs.items()
                    }
                )
            return logs

        return {
            "stdout": self.pbs_job.get_stdout_log,
            "stderr": self.pbs_job.get_stderr_log,
            **{
                log_type: partial(self.pbs_job.get_cached_file_contents, path, bytes=100_000)
                for log_type, path in self.custom_logs.items()
            },
        }

    async def get_environment_variable(self, variable):
        """Get the value of an environment variable from the HPC.

        Args:
            variable (str): Name of environment variable (e.g.: "WORKDIR").

        Returns:
            str: value of environment variable.
        """
        return await self.client.env.get_environmental_variable(variable)

    @_ensure_connected
    async def execute(self, *args, **kwargs):
        """
        executes the job
        """
        try:
            await self._execute(*args, **kwargs)
            self.execute_time = timezone.now()
            self._status = "SUB"
        except Exception:
            self._status = "ERR"
        await self._safe_save()

    async def _execute(self, remote_name=None):
        """Execute the job using the UIT Plus Python client."""
        try:
            # Submit job with PbsScript object and remote workspace
            self.job_id = await self.pbs_job.submit(remote_name=remote_name)
            self.extended_properties["post_processing_job_id"] = self.pbs_job.post_processing_job_id
        except UITError as e:
            if "allocation" in str(e):
                self.status_message = (
                    "Submission failed because subproject allocation has expired or there are insufficient hours."
                )
            else:
                self.status_message = str(e)
            log.exception(e)
            raise e
        except Exception as e:
            try:
                await self.client.call(f"ls {self.working_dir}/*.pbs")
            except Exception:
                self.status_message = "No PBS script created. Contact web site administrator for resolution."
            else:
                self.status_message = f'Error submitting job on "{self.system}": {e}'
            log.exception(e)
            raise e

    @_ensure_connected
    async def resubmit(self, *args, **kwargs):
        await self._resubmit(*args, **kwargs)

    async def _resubmit(self, *args, **kwargs):
        self.pbs_job._job_id = None
        self.qstat = None
        await self.execute(*args, **kwargs)

    # duplicate from Tethys to make it async
    async def update_status(self, status=None, *args, **kwargs):
        """
        Updates the status of a job. If ``status`` is passed then it will manually update the status. Otherwise,
            it will determine if ``_update_status`` should be called.

        Args:
            status (str, optional): The value to manually set the status to. It may be either the display name or the
                three letter database code for defined statuses. If it is not one of the defined statuses, then the
                status will be set to ``OTH`` and the ``status`` value will be saved in ``extended_properties``
                using the ``OTHER_STATUS_KEY``.
            *args: positional arguments that are passed through to ``_update_status``.
            **kwargs: key-word arguments that are passed through to ``_update_status``.

        """
        old_status = self._status
        update_needed = old_status in self.NON_TERMINAL_STATUS_CODES
        # Set status from status given
        if status:
            if status not in self.VALID_STATUSES:
                if status in self.DISPLAY_STATUSES:
                    status = self.REVERSE_STATUSES[status]
                else:
                    self.extended_properties[self.OTHER_STATUS_KEY] = status
                    status = "OTH"
            if status != "OTH":
                self.extended_properties.pop(self.OTHER_STATUS_KEY, None)
            self._status = status
            await self._safe_save()

        # Update status if status not given and still pending/running
        elif update_needed and self.is_time_to_update():
            await self._update_status(*args, **kwargs)
            self._last_status_update = timezone.now()

        # Post-process status after update if old status was pending/running
        if update_needed:
            if self._status == "RUN" and (old_status in ("PEN", "SUB")):
                self.start_time = timezone.now()
            if self._status in ["COM", "VCP", "RES"]:
                await self.process_results()
            elif self._status == "ERR" or self._status == "ABT":
                self.completion_time = timezone.now()

        await self._safe_save()

    @_ensure_connected
    async def _update_status(self):
        """Retrieve a job’s status using the UIT Plus Python client.

        Translates UitJob status to TethysJob status and saves to the database
        """
        try:
            status = await self.pbs_job.update_status()
        except UITError as e:
            if "qstat: Unknown Job Id" in str(e):
                status = "F"
                self.status_message = f"Job ID was not found on {self.client.system}. Unable to get status information."
            else:
                raise e
        new_status = self.UIT_TO_TETHYS_STATUSES.get(status, "ERR")

        if new_status == "COM":
            if "cleanup_job_id" in self.extended_properties:
                if self.job_id != self.extended_properties["cleanup_job_id"]:
                    new_status = "SUB"
                    self.job_id = self.extended_properties["cleanup_job_id"]

            self.set_archived_status(True)

        self._status = new_status
        self.qstat = self.pbs_job.qstat
        await self._safe_save()

        # Get intermediate results, if applicable
        if self.transfer_intermediate_files:
            if self.intermediate_transfer_interval_exceeded:
                self.last_intermediate_transfer = timezone.now()  # move this to get_intermediate_results
                await self.get_intermediate_results()
        await self._safe_save()

    @database_sync_to_async
    def set_archived_status(self, value):
        archived_job_id = self.extended_properties.get("archived_job_id")
        if archived_job_id:
            try:
                archived_job = self.__class__.objects.get(job_id=archived_job_id)
                archived_job.archived = value
                archived_job.save()
            except self.DoesNotExist:
                pass

    @property
    def intermediate_transfer_interval_exceeded(self):
        if self.intermediate_transfer_interval == 0:
            return True
        delta_time = timezone.now() - self.last_intermediate_transfer
        minutes = delta_time.days * 24 * 60 + delta_time.seconds / 60
        return minutes > self.intermediate_transfer_interval

    async def process_results(self):
        """Process the results using the UIT Plus Python client."""
        log.debug("Started processing results for job: {}".format(self))
        await self.get_remote_files(self.transfer_output_files)
        self.completion_time = timezone.now()
        self._status = "COM"
        await self._safe_save()
        log.debug("Finished processing results for job: {}".format(self))

    async def get_intermediate_results(self):
        """Retrieve intermediate result files from the supercomputer."""
        if await self.get_remote_files(self.transfer_intermediate_files):
            if self.process_intermediate_results_function:
                self.process_intermediate_results_function()

    def resolve_paths(self, paths):
        resolved_paths = []
        for p in paths:
            if "$JOB_INDEX" in p or "$RUN_DIR" in p:
                for sub_job in self.pbs_job.sub_jobs:
                    resolved_paths.append(sub_job.resolve_path(p))
            else:
                resolved_paths.append(self.pbs_job.resolve_path(p))
        return resolved_paths

    async def get_remote_files(self, remote_filenames):
        """Transfer files from a directory on the super computer.

        Args:
            remote_filenames (List[str]): Files to retrieve from remote_dir

        Returns:
            bool: True if all file transfers succeed.
        """

        # Ensure the local transfer directory exists
        workspace = Path(self.workspace)
        success = True
        remote_paths = self.resolve_paths(remote_filenames)

        for remote_path in remote_paths:
            rel_path = remote_path.relative_to(self.working_dir)
            local_path = workspace / rel_path
            local_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                await self.client.get_file(remote_path=remote_path, local_path=local_path)
                if not os.path.exists(local_path):
                    success = False
            except RuntimeError as e:
                success = False
                log.error("Failed to get remote file: {}".format(str(e)))

        return success

    @_ensure_connected
    async def stop(self):
        """Stops/cancels this job.

        Returns:
            bool: True if job was deleted.
        """
        result = await self.pbs_job.terminate()
        if result:
            await self.update_status("ABT")
        else:
            await self.update_status("ERR")
        return result

    @_ensure_connected
    async def pause(self):
        """Pauses this job.

        Returns:
            bool: True if job was paused.
        """
        return await self.pbs_job.hold()

    @_ensure_connected
    async def resume(self):
        """Resumes this job if paused.

        Returns:
            bool: True if job was resumed.
        """
        return await self.pbs_job.release()

    @_ensure_connected
    async def delete(self, using=None, keep_parents=False):
        """Stops the job and cleans up workspaces in order to delete the job."""
        try:
            archive = bool(self.extended_properties.get("archived_job_id"))
            if not self._system_decommissioned:
                stop_result = await self.stop()
                if stop_result is False:
                    raise Exception("Delete failed while performing job cleanup.")

            clean_remote = not self._system_decommissioned
            try:
                if self.clean_on_delete:
                    await self.clean(archive=archive, remote=clean_remote)
            except AttributeError:
                await self.clean(archive=archive, remote=clean_remote)
        except Exception as e:
            log.exception(f"Error during job delete: {e}")
            raise  # Let Django know to display the error message to the user
        await self._safe_delete(using, keep_parents)

    async def clean(self, archive=False, remote=True):
        """Remove all files and directories associated with the job.

        Removal takes place on unmonitored background thread so as not to disturb the user (as deletes on the HPC can take a long time). This means that we will always return True even if the files were not deleted.

        Args:
            archive (bool): Flag to indicate whether files should be removed from the archive as well.

        Returns:
            bool: True. Always.
        """  # noqa: E501
        # Remove local workspace
        async with asyncio.TaskGroup() as tg:
            if self.workspace:
                log.warning(f"Removing local workspace {self.workspace}")
                tg.create_task(asyncio.to_thread(shutil.rmtree, self.workspace, True))

            if remote:
                # Remove remote locations
                if archive:
                    path = await self.get_archive_dir()
                    cmd = f"archive rm -rf {path} || true"
                    tg.create_task(self.client.call(command=cmd, working_dir="/"))
                    self.set_archived_status(False)
                else:
                    for path in (self.working_dir, self.home_dir):
                        cmd = f"rm -rf {path} || true"
                        tg.create_task(self.client.call(command=cmd, working_dir="/"))
                        log.info(f"Executing command '{cmd}' on {self.system}")
        return True

    @property
    def archive_filename(self):
        return f"job_{self.remote_workspace_id}.run_files.tar.gz"

    @_ensure_connected
    async def archive(self):
        """Archive all files associated with this job.

        This job is compressed into a tar file and then pushed
        to the archive directory.

        Returns:
            bool: True. Always.
        """
        await self._archive()

    async def _archive(self, *args, **kwargs):
        # Check archive status and store system name
        try:
            archive_stat = await self.client.call("archive stat")
            archive_name = archive_stat.split()[2]
        except UITError as e:
            log.exception(e)
            self.status_message = e.message
            return

        archive_filename = f"job_{self.remote_workspace_id}.run_files.tar.gz"
        pbs_script = PbsScript(
            name="archive",
            project_id=self.pbs_job.script.project_id,
            num_nodes=1,
            processes_per_node=1,
            max_time="48:00:00",
            queue="transfer",
            node_type="transfer",
            system=self.system,
        )
        pbs_script.execution_block = (
            f"tar -czf {archive_filename} *\n"
            f"archive put -p -C {await self.get_archive_dir()} {archive_filename}\n"
            f"rm {archive_filename}\n"
        )

        # Create PBS job
        job = PbsJob(pbs_script, client=self.client, label="archive_" + self.label)
        job._remote_workspace = self._remote_workspace
        job._remote_workspace_id = self._remote_workspace_id

        job.description = f"Archive job: {self.name} ({self.job_id})"
        job_model = await database_sync_to_async(self.instance_from_pbs_job)(job, self.user)
        # Put job id in extended properties
        save_script_attrs = [
            "name",
            "project_id",
            "num_nodes",
            "processes_per_node",
            "queue",
            "system",
            "_array_indices",
        ]

        self.metadata = self.extended_properties
        save_job_attrs = ["label", "workspace", "description", "metadata"]

        job_model.extended_properties.update(
            {
                "archived_job_id": self.job_id,
                "archived_to": archive_name,
                "archived_job_script": {attr: getattr(self, attr) for attr in save_script_attrs},
                "archived_job_attrs": {attr: getattr(self, attr) for attr in save_job_attrs},
            }
        )
        # Add max_time
        max_time_json = {"days": self.max_time.days, "seconds": self.max_time.seconds}
        job_model.extended_properties["archived_job_script"]["max_time"] = max_time_json
        job_model.workspace = ""

        # Submit job
        await job_model.execute()

    @_ensure_connected
    async def restore(self):
        """Restore the job work directory from to archive server.
        NOTE: This is meant to be called only on an "archive" job.
        This method replaces the jobs details with that of the
        new transfer job.

        Returns:
            bool: True. Always.
        """
        archive_filename = f"job_{self.remote_workspace_id}.run_files.tar.gz"

        # Create transfer script
        self.execution_block = (
            f"archive get -p -C {await self.get_archive_dir()} {archive_filename}\n"
            f"tar -xzf {archive_filename}\n"
            f"rm {archive_filename}\n"
        )
        self.name = "unarchive"

        # Submit job
        await self.resubmit()

        # Check database for archived job
        await self.update_job_after_restore(self.extended_properties.get("archived_job_id"))

    @database_sync_to_async
    def update_job_after_restore(self, job_id):
        """After restoring from the archive, recreate job in the main jobs_table if it does not already exist"""
        if job_id is not None:
            try:
                self.__class__.objects.get(job_id=job_id)
            except self.DoesNotExist:
                # Recreate job
                script_kwargs = self.extended_properties["archived_job_script"]
                array_indices = script_kwargs.pop("_array_indices")
                script_kwargs["max_time"] = dt.timedelta(**script_kwargs["max_time"])
                # Setup PbsScript
                restored_job_script = PbsScript(**script_kwargs, array_indices=array_indices)

                Job = PbsJob if array_indices is None else PbsArrayJob
                job_kwargs = self.extended_properties["archived_job_attrs"]
                pbs_job = Job(restored_job_script, client=self.client, **job_kwargs)
                pbs_job._remote_workspace = self._remote_workspace
                pbs_job._remote_workspace_id = self._remote_workspace_id
                pbs_job._job_id = job_id
                restored = self.instance_from_pbs_job(pbs_job, self.user)
                restored._status = "COM"
                restored.save()


class EnvironmentProfile(models.Model):
    """
    Model that stores modules and environment
    variables for a specific run profile.

    Attributes:
        user (foreign key): The user to whom the profile belongs
        name (str): The name of the profile
        hpc_system (str): The name of the hpc system the profile was created for (e.g. "onyx")
        environment_variables (str): A Json string of the environment variables
        modules (str): A Json string of the modules to load and unload
        last_used (datetime): The time the profile was last loaded (for sorting)
    """

    user = models.ForeignKey(User, on_delete=models.CASCADE)
    name = models.CharField(max_length=64)
    hpc_system = models.CharField(max_length=64)
    software = models.CharField(max_length=1024, null=True)
    email = models.CharField(max_length=1024, null=True)
    environment_variables = models.CharField(max_length=2048, null=True)
    modules = JSONField(default=dict, null=True)
    last_used = models.DateTimeField(auto_now_add=True)
    user_default = models.BooleanField(default=False)
    default_for_versions = JSONField(blank=True, default=list, null=True)

    @classmethod
    def set_default_for_version(cls, usr, profile, version):
        """Set profile as the default for the selected version.

        Args:
            usr:
            version:
            profile:

        Returns:

        """
        # Find current default for version
        ver_default = cls._get_default_for_version(usr, profile.hpc_system, profile.software, version)
        if ver_default:
            # Remove version from its list of defaults
            ver_default.default_for_versions.remove(version)
            # Save
            ver_default.save()
        # Add version to current profile default
        profile.default_for_versions.append(version)
        # Save
        profile.save()

    @classmethod
    def set_general_default(cls, usr, profile):
        """Set the provided profile as the general default

        Args:
            usr:
            profile:

        Returns:

        """
        # Get current default
        old_default = cls._get_general_default(usr, profile.hpc_system, profile.software)
        if old_default:
            # Remove the old default as general default
            old_default.user_default = False
            # Save
            old_default.save()

        # Set profile as default
        profile.user_default = True
        # Save
        profile.save()

    @classmethod
    def get_default(cls, usr, hpc_system, software, version=None, use_general_default=True):
        """Get the default for this version. Return the general default if it doesn't exist.

        Args:
            usr:
            hpc_system:
            software:
            version:
            use_general_default:

        Returns:

        """
        if version:
            default = cls._get_default_for_version(usr, hpc_system, software, version)
            if default or not use_general_default:
                return default

        return cls._get_general_default(usr, hpc_system, software)

    @classmethod
    def _get_default_for_version(cls, usr, hpc_system, software, version):
        """
        Get the profile listed as default for the specified version

        Args:
            usr:
            hpc_system:
            software:
            version:

        Returns:

        """
        profiles = cls.objects.filter(user=usr, hpc_system=hpc_system, software=software).exclude(
            default_for_versions=[]
        )
        for profile in profiles:
            if version in profile.default_for_versions:
                return profile

    @classmethod
    def _get_general_default(cls, usr, hpc_system, software):
        """Return the general default

        Args:
            system:
            software:
            usr:

        Returns:

        """
        try:
            profiles = cls.objects.get(user=usr, hpc_system=hpc_system, software=software, user_default=True)
        except cls.DoesNotExist:
            return None
        except cls.MultipleObjectsReturned:
            return cls.objects.filter(user=usr, hpc_system=hpc_system, software=software, user_default=True)[0]

        return profiles

    def is_default_for_version(self, version):
        """Return True if this profile is the default profile for the included version.

        Args:
            version:

        Returns:

        """
        return version in self.default_for_versions

    def remove_default_for_version(self, version):
        """

        Args:
            version:

        Returns:

        """
        if version in self.default_for_versions:
            self.default_for_versions.remove(version)
