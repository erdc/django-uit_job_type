from collections import OrderedDict
import json
import logging
import re

import param
import panel as pn

from django.contrib.auth.models import User
from channels.db import database_sync_to_async
from uit_plus_job.models import UitPlusJob, EnvironmentProfile
from uit.gui_tools.submit import HpcSubmit, PbsScriptAdvancedInputs
from uit.gui_tools import FileSelector, HpcFileBrowser, get_js_loading_code


log = logging.getLogger(__name__)


class TethysProfileManagement(PbsScriptAdvancedInputs):
    tethys_user = param.ClassSelector(class_=User)
    environment_profile = param.Selector(label="Load Environment Profile")
    environment_profile_delete = param.Selector(label="Environment Profile to Delete")
    environment_profile_version = param.Selector(allow_None=True, precedence=2)
    initializing_environment_profile_version = param.Boolean()
    save_name = param.String(label="Save As:")
    profiles = param.List()
    version = param.Selector(label="Set Version Default", precedence=1)
    show_save_panel = param.Boolean()
    show_delete_panel = param.Boolean()
    delete_profile_btn = param.Action(lambda self: self.update_delete_panel(True), label="Delete Selected Profile")
    software = param.String()
    notification_email = param.String(label="Notification E-mail")
    selected_version = param.String()
    load_type = param.Selector(
        default="Load Saved Profile",
        objects=[
            "Create New Profile",
            "Load Saved Profile",
            "Load Profile from PBS Script",
        ],
    )
    pbs_body = param.String()
    _software_versions = param.List()

    # Parameters to override in subclass
    version_environment_variable = "VERSION"

    def __init__(self, *args, **kwargs):
        self.revert_btn = pn.widgets.Button(name="Revert", button_type="primary", width=100)
        self.revert_btn.on_click(self.revert)
        self.overwrite_request = None
        self.cb = None
        self.count = 0
        self.progress_bar = pn.widgets.misc.Progress(width=250, active=False, visible=False)
        self.alert = pn.pane.Alert(visible=False)
        self.close_alert_button = pn.widgets.Button(name="X", button_type="danger", margin=25, width=50, visible=False)
        self.revert_btn.on_click(self.revert)
        self.profile_panel = pn.Column()
        self.no_version_profiles_alert = pn.pane.Alert(
            "No profiles have been created for the selected version",
            alert_type="warning",
            visible=False,
            margin=(0, 5, 20, 5),
        )
        super().__init__(*args, **kwargs)
        self.profile_management_card = pn.Card(
            self.profile_management_panel(),
            title="Manage Profiles",
            collapsed=False,
            sizing_mode="stretch_width",
            margin=(10, 0),
        )
        self._advanced_options_layout = pn.Column(name="Environment", loading=True)
        self._layout = pn.Column(
            "# Environment Profiles",
            self._advanced_options_layout,
        )

    def get_versions(self):
        """Override this method to provide a list of versions for software.

        Returns: A list of software versions

        """
        return []

    @property
    def versions(self):
        if not self._software_versions:
            self._software_versions = self.get_versions()
        return self._software_versions

    async def get_cached_versions(self, update_cache=False):
        if not self._software_versions or update_cache:
            self._software_versions = await self.await_if_async(self.get_versions())
        return self._software_versions

    @param.depends(
        "notification_email",
        "environment_variables",
        "modules_to_load",
        "modules_to_unload",
        watch=True,
    )
    def update_revert(self):
        self.revert_btn.disabled = False

    def load_profile_column(self):
        load_type = pn.widgets.RadioButtonGroup.from_param(self.param.load_type, width=300)
        environment_profile = pn.widgets.Select.from_param(self.param.environment_profile, width=300, visible=True)
        pbs_script_type = pn.widgets.RadioButtonGroup(
            options=["Upload Local Script", "Select Script on HPC"],
            width=300,
            visible=False,
        )
        file_upload = pn.widgets.FileInput(accept=".sh,.pbs", visible=False)
        file_upload.param.watch(self._parse_local_pbs, "value")

        select_pbs = FileSelector(
            help_text="Load environment from a PBS file",
        )
        select_pbs.param.watch(self._parse_remote_pbs, "file_path")
        select_pbs.file_browser = HpcFileBrowser(self.uit_client, delayed_init=False, patterns=["*.pbs", "*.sh"])
        select_pbs.show_browser = True
        fbp = select_pbs.panel
        fbp.visible = False

        args = {
            "prof_col": self.profile_panel,
            "profile_select": environment_profile,
            "pbs_script_type": pbs_script_type,
            "file_upload": file_upload,
            "fbp": fbp,
        }

        code = get_js_loading_code("prof_col")
        environment_profile.jscallback(args=args, value=code)

        load_type.jscallback(
            args=args,
            value=f"""
        if(this.active==0){{
            profile_select.visible = false;
            pbs_script_type.visible = file_upload.visible = fbp.visible = false;
            {code}
        }}else if(this.active==1){{
            profile_select.visible = true;
            pbs_script_type.visible = file_upload.visible = fbp.visible = false;
            {code}
        }}else if(this.active==2){{
            fbp.visible = pbs_script_type.active==1;
            file_upload.visible = pbs_script_type.active==0;
            profile_select.visible = false;
            pbs_script_type.visible = true;
            {code}
        }}
        """,
        )
        pbs_script_type.jscallback(
            args=args,
            value="fbp.visible=pbs_script_type.active==1; file_upload.visible=this.active==0;",
        )

        return pn.Column(
            load_type,
            pbs_script_type,
            environment_profile,
            file_upload,
            fbp,
            pn.layout.Divider(),
            width=800,
        )

    @database_sync_to_async
    def get_profiles(self, version=None):
        kwargs = dict(
            user=self.tethys_user,
            hpc_system=self.uit_client.system,
            software=self.software,
        )
        if version is not None:
            kwargs["environment_variables__contains"] = f'"{self.version_environment_variable}": "{version}"'

        return sorted([p.name for p in EnvironmentProfile.objects.filter(**kwargs)])

    @database_sync_to_async
    def get_profile(self, name):
        return EnvironmentProfile.objects.get(
            user=self.tethys_user,
            hpc_system=self.uit_client.system,
            software=self.software,
            name=name,
        )

    @database_sync_to_async
    def get_default_profile(self, version=None, use_general_default=False):
        return EnvironmentProfile.get_default(
            self.tethys_user,
            self.uit_client.system,
            self.software,
            version=version,
            use_general_default=use_general_default,
        )

    @param.depends("uit_client", watch=True)
    async def update_uit_dependant_options(self):
        versions = await self.get_cached_versions()
        self.param.version.objects = ["System Default"] + versions
        self.version = self.version or "System Default"
        await self.create_advanced_options_view()

    @param.depends("version", watch=True)
    async def update_version_profiles(self):
        version = None if self.version == "System Default" else self.version
        profiles = await self.get_profiles(version=version)
        version_default = await self.get_default_profile(version=self.version, use_general_default=version is None)

        self.param.environment_profile_version.objects = profiles
        if version_default:
            self.initializing_environment_profile_version = True
            trigger = self.environment_profile_version == version_default.name
            self.environment_profile_version = version_default.name
            if trigger:  # always trigger event even if value doesn't change
                self.param.trigger("environment_profile_version")
        if profiles:
            self.param.environment_profile_version.precedence = 2
            self.no_version_profiles_alert.visible = False
        else:
            self.param.environment_profile_version.precedence = -1
            self.no_version_profiles_alert.visible = True

    def update_save_panel(self, e):
        self.save_name = self.environment_profile if self.load_type == self.param.load_type.objects[1] else ""
        self.show_save_panel = True

    def update_delete_panel(self, should_show):
        self.show_delete_panel = should_show

    def cancel_save(self, e=None):
        self.save_name = ""
        self.show_save_panel = False
        self.reset_loading()
        if self.overwrite_request:
            self._clear_alert()
            self.overwrite_request = None

    @param.depends("load_type", watch=True)
    async def revert(self, e=None):
        if self.load_type == self.param.load_type.objects[0]:
            await self.update_configurable_hpc_parameters(reset=True)
        elif self.load_type == self.param.load_type.objects[1]:
            await self.select_profile()
        elif self.load_type == self.param.load_type.objects[2]:
            self._populate_from_pbs()
        self.reset_loading()
        self.param.trigger("show_save_panel")

    @database_sync_to_async
    def _set_profile_default(self, profile):
        if self.version == "System Default":
            EnvironmentProfile.set_general_default(self.tethys_user, profile)
        else:
            EnvironmentProfile.set_default_for_version(self.tethys_user, profile, self.version)

    @param.depends("environment_profile_version", watch=True)
    async def set_default(self):
        if self.initializing_environment_profile_version or not self.environment_profile_version:
            self.initializing_environment_profile_version = False
            return
        profile = await self.get_profile(name=self.environment_profile_version)
        await self._set_profile_default(profile)
        self._alert(f"Default profile for version {self.version} is now set to {self.environment_profile_version}")
        self.update_version_profiles()

    @param.depends("environment_profile", watch=True)
    async def select_profile(self):
        if self.environment_profile and not self.environment_profile == "default":
            await self._populate_profile_from_saved(self.environment_profile)

    @database_sync_to_async
    def _save_profile(self, **kwargs):
        profile = EnvironmentProfile(**kwargs)
        profile.save()
        return profile

    @database_sync_to_async
    def _delete_profile(self, profile):
        profile.delete()

    async def _load_profiles(self):
        """
        Get a list of profiles from the database
        that belong to this user
        """
        profiles = await self.get_profiles()

        # Create default profile for user if one does not exist
        if len(profiles) == 0:
            log.info("Creating default profile")
            self.update_configurable_hpc_parameters(reset=True)
            env_var_json = json.dumps(self.environment_variables)
            modules = {
                "modules_to_load": self.modules_to_load,
                "modules_to_unload": self.modules_to_unload,
            }

            saving_profile = await self._save_profile(
                user=self.tethys_user,
                environment_variables=env_var_json,
                modules=modules,
                hpc_system=self.uit_client.system,
                software=self.software,
                name="system-default",
                default_for_versions=[],
                user_default=True,
            )
            profiles = [saving_profile.name]

        self.profiles = profiles
        self.param.environment_profile.objects = self.param.environment_profile_delete.objects = self.profiles
        for attr in ["environment_profile", "environment_profile_delete"]:
            if getattr(self, attr) not in self.profiles:
                setattr(self, attr, self.profiles[0])
        self.update_version_profiles()

    async def _delete_selected_profile(self, e=None):
        log.info("Deleting profile {}".format(self.environment_profile_delete))

        del_profile = await self.get_profile(name=self.environment_profile_delete)

        await self._delete_profile(del_profile)
        self._alert("Removed {}".format(self.environment_profile_delete), alert_type="danger")

        await self._load_profiles()
        self.revert()
        self.update_delete_panel(False)

    async def _save_current_profile(self, e=None):
        log.info("Saving profile")

        env_var_json = json.dumps(self.environment_variables)
        modules = {
            "modules_to_load": self.modules_to_load,
            "modules_to_unload": self.modules_to_unload,
        }

        # Check to see if we have already loaded this model to overwrite
        # and were just asking for confirmation

        if not self.save_name:
            self.overwrite_request = 1
            self._alert(
                "You must enter a profile name before you can save.",
                alert_type="danger",
            )
            self.param.trigger("show_save_panel")
            return

        if self.overwrite_request not in (1, None) and self.overwrite_request.name == self.save_name:
            saving_profile = self.overwrite_request
            saving_profile.modules = modules
            saving_profile.environment_variables = env_var_json
            saving_profile.email = self.notification_email
            self.overwrite_request = None
        else:
            # Check to see if a profile already exists for this user with the same name
            try:
                self.overwrite_request = await self.get_profile(name=self.save_name)
                # Ask for confirmation before continuing
                self._alert(
                    "Are you sure you want to overwrite profile {}? Press save again to confirm.".format(
                        self.overwrite_request.name
                    ),
                    alert_type="danger",
                    timeout=False,
                )
                self.param.trigger("show_save_panel")
                return
            except EnvironmentProfile.DoesNotExist:
                # Creating a new one
                self.overwrite_request = None
                version = self.environment_variables[self.version_environment_variable]
                version_default = await self.get_default_profile(version)
                default_for_versions = [version] if version_default is None else []

                saving_profile = await self._save_profile(
                    user=self.tethys_user,
                    environment_variables=env_var_json,
                    modules=modules,
                    hpc_system=self.uit_client.system,
                    software=self.software,
                    name=self.save_name,
                    email=self.notification_email,
                    default_for_versions=default_for_versions,
                )
        await self._load_profiles()
        self.environment_profile = self.save_name
        self.load_type = self.param.load_type.objects[1]
        self._alert("Successfully saved.", alert_type="success")
        self.cancel_save()

    def _alert(self, message, alert_type="info", timeout=True):
        self._clear_alert()
        self.alert.visible = True
        self.close_alert_button.visible = True
        self.alert.alert_type = alert_type
        self.alert.object = message

    def _clear_alert(self, e=None):
        self.alert.visible = False
        self.close_alert_button.visible = False
        self.alert.object = ""

    def _parse_pbs_body(self):
        """
        return the modules and environment
        variables parsed from pbs file contents.
        """
        tokenize = [line.rstrip().split() for line in self.pbs_body.splitlines()]

        modules_to_load = []
        modules_to_unload = []

        env_vars = {}

        for line in tokenize:
            # Get modules
            if len(line) > 2 and line[0] == "module":

                if line[1] == "load":
                    modules_to_load.extend(line[2:])
                elif line[1] == "unload":
                    modules_to_unload.extend(line[2:])
                elif line[1] == "swap" and len(line) > 3:
                    modules_to_unload.append(line[2])
                    modules_to_load.append(line[3])

            # Get environment variables
            if len(line) > 1:
                # parse BASH scripts
                if line[0] == "export":
                    # Add environment variable
                    var_name = line[1].split("=")[0]
                    # Refuse everything to the right of the first equals sign
                    value = "=".join(line[1].split("=")[1:])
                    env_vars[var_name] = value

                # parse CSH scripts
                if line[0] == "setenv":
                    # Add environment variable
                    var_name = line[1]
                    env_vars[var_name] = line[2]

        return {
            "modules_to_load": modules_to_load,
            "modules_to_unload": modules_to_unload,
            "environment_variables": env_vars,
        }

    def _parse_pbs_directives(self):
        """
        Returns a dictionary of the directives
        specified in a PBS script
        """
        # Get general directives
        matches = re.findall("#PBS -(.*)", self.pbs_body)
        directives = {k: v for k, v in [(i.split() + [""])[:2] for i in matches]}
        # Get l directives
        l_matches = re.findall("#PBS -l (.*)", self.pbs_body)
        d = dict()
        for match in l_matches:
            if "walltime" in match:
                d["walltime"] = match.split("=")[1]
            else:
                d.update({k: v for k, v in [i.split("=") for i in l_matches[0].split(":")]})

        directives["l"] = d
        return directives

    async def _populate_profile_from_saved(self, name):
        """
        Load profile from db and populate params
        """
        profile = await self.get_profile(name=name)

        if not profile:
            raise ValueError("Trying to load profile that doesn't exist.")

        self.environment_profile = profile.name
        modules = profile.modules
        self.modules_to_load = modules["modules_to_load"]
        self.modules_to_unload = modules["modules_to_unload"]
        self.environment_variables = OrderedDict(json.loads(profile.environment_variables))
        self.notification_email = profile.email or ""
        self.reset_loading()

    def _parse_local_pbs(self, e):
        self.pbs_body = str(e.new.decode("ascii"))
        self._populate_from_pbs()

    def _parse_remote_pbs(self, e):
        pbs_file_path = e.obj.file_path or ""
        if pbs_file_path.endswith(".pbs") or pbs_file_path.endswith(".sh"):
            self.pbs_body = self.uit_client.call(f"cat {pbs_file_path}")
            self._populate_from_pbs()
            e.obj.show_browser = False

    def _populate_from_pbs(self):
        parsed_pbs = self._parse_pbs_body()
        self.modules_to_load = self._validate_modules(self.param.modules_to_load.objects, parsed_pbs["modules_to_load"])
        self.modules_to_unload = self._validate_modules(
            self.param.modules_to_unload.objects, parsed_pbs["modules_to_unload"]
        )

        new_env_vars = OrderedDict()
        for k, v in parsed_pbs["environment_variables"].items():
            new_env_vars[k] = v.strip('"')
        self.environment_variables = new_env_vars
        self.reset_loading()

    def reset_loading(self):
        self.revert_btn.disabled = True
        self.profile_panel.css_classes = ["temp"]
        self.profile_panel.css_classes = []

    def profile_management_panel(self):
        return pn.Row(
            pn.Column(
                pn.Param(
                    self,
                    parameters=[
                        "version",
                        "environment_profile_version",
                    ],
                    widgets={
                        "version": {"width": 200},
                        "environment_profile_version": pn.widgets.RadioBoxGroup,
                    },
                    show_name=False,
                ),
                self.no_version_profiles_alert,
            ),
            self.delete_panel,
        )

    @param.depends("show_delete_panel")
    def delete_panel(self):
        if self.show_delete_panel:
            delete_btn = pn.widgets.Button(name="Delete", button_type="danger", width=100)
            delete_btn.on_click(self._delete_selected_profile)
            cancel_btn = pn.widgets.Button(name="Cancel", button_type="primary", width=100)
            cancel_btn.on_click(lambda e: self.update_delete_panel(False))

            code = f"o.disabled=true; {get_js_loading_code('btn')}"

            delete_btn.js_on_click(args={"btn": delete_btn, "o": cancel_btn}, code=code)
            cancel_btn.js_on_click(args={"btn": cancel_btn, "o": delete_btn}, code=code)

            return pn.Column(
                self.param.environment_profile_delete,
                pn.pane.Alert(
                    "Are you sure you want to delete the selected profile? This action cannot be undone.",
                    alert_type="danger",
                ),
                pn.Row(delete_btn, cancel_btn, align="end"),
            )
        else:
            delete_btn = pn.widgets.Button(name="Delete Selected Profile", button_type="danger", width=200)
            delete_btn.on_click(lambda e: self.update_delete_panel(True))
            code = get_js_loading_code("btn")
            delete_btn.js_on_click(args={"btn": delete_btn}, code=code)

            return pn.Column(self.param.environment_profile_delete, pn.Row(delete_btn, align="end"))

    @param.depends("show_save_panel")
    def save_panel(self):
        if self.show_save_panel:
            save_btn = pn.widgets.Button(name="Save", button_type="success", width=100)
            save_btn.on_click(self._save_current_profile)
            cancel_btn = pn.widgets.Button(name="Cancel", button_type="danger", width=100)
            cancel_btn.on_click(self.cancel_save)

            code = f"o.disabled=true; {get_js_loading_code('btn')}"

            save_btn.js_on_click(args={"btn": save_btn, "o": cancel_btn}, code=code)
            cancel_btn.js_on_click(args={"btn": cancel_btn, "o": save_btn}, code=code)

            return pn.Column(
                pn.Column(
                    self.param.save_name,
                    pn.Row(save_btn, cancel_btn, align="end"),
                    align="end",
                ),
                sizing_mode="stretch_width",
            )
        else:
            self.revert_btn.css_classes = ["temp"]
            self.revert_btn.css_classes = []

            save_btn = pn.widgets.Button(name="Save Current Profile", button_type="success", width=200)
            save_btn.on_click(self.update_save_panel)

            code = get_js_loading_code("btn")

            save_btn.js_on_click(args={"btn": save_btn, "o": self.revert_btn}, code=code)
            self.revert_btn.js_on_click(args={"btn": self.revert_btn, "o": save_btn}, code=code)

            return pn.Column(
                pn.Row(save_btn, self.revert_btn, align="end"),
                sizing_mode="stretch_width",
            )

    async def create_advanced_options_view(self):
        """
        Overrides HpcSubmit function in order to
        add a panel to select environment profiles.
        """

        if not self.profiles:
            await self._load_profiles()

        # Load default profile
        default = await self.get_default_profile(self.selected_version, use_general_default=True)
        # ensure that profile isn't reloaded if it was previously set (which would override any changes made).
        if default is not None and default.name != self.environment_profile:
            await self._populate_profile_from_saved(default.name)

        self.profile_panel = super().advanced_options_view()
        self.profile_panel.insert(0, self.param.notification_email)
        self.profile_panel.append(self.save_panel)
        self.profile_panel.sizing_mode = "stretch_width"

        self._advanced_options_layout[:] = [
            self.load_profile_column(),
            self.profile_panel,
            pn.Row(
                self.alert,
                self.close_alert_button,
                pn.bind(self._clear_alert, self.close_alert_button.param.clicks),
            ),
            pn.layout.Divider(),
            self.profile_management_card,
        ]
        self._advanced_options_layout.loading = False

    def advanced_options_view(self):
        return self._advanced_options_layout

    def panel(self):
        return self._layout


class TethysHpcSubmit(HpcSubmit, TethysProfileManagement):
    custom_logs = None
    redirect_url = "/"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pbs_options_pane = None
        self.profile_management_card.collapsed = True

    def set_pbs_options_alert(self, msg, alert_type="warning"):
        self.pbs_options_pane[1] = pn.pane.Alert(msg, alert_type=alert_type) if msg else None

    def validate_version(self):
        if self.environment_variables.get(self.version_environment_variable) != self.selected_version:
            self.set_pbs_options_alert(
                f"The selected profile does not match the selected version ({self.selected_version}). "
                f'Please select a compatible profile, or go the the "Environment" tab to create a new profile.'
            )
        else:
            self.set_pbs_options_alert(None)

    async def _populate_profile_from_saved(self, name):
        await super()._populate_profile_from_saved(name)
        self.validate_version()

    def _populate_from_pbs(self):
        super()._populate_from_pbs()

        # Load directives
        directives = self._parse_pbs_directives()
        self.hpc_subproject = directives.get("A") or self.hpc_subproject
        if directives.get("l"):
            self.nodes = int(directives["l"]["select"])
            self.processes_per_node = int(directives["l"]["ncpus"])
            self.wall_time = directives["l"]["walltime"]
        self.queue = directives.get("q") or self.queue
        self.notification_email = directives.get("M") or self.notification_email
        if directives.get("m"):
            self.notify_start = "b" in directives["m"]
            self.notify_end = "e" in directives["m"]

    def pbs_options_view(self):
        self.pbs_options_pane = super().pbs_options_view()
        self.pbs_options_pane.insert(0, pn.widgets.Select.from_param(self.param.environment_profile, width=300))
        self.pbs_options_pane.insert(1, None)
        self.pbs_options_pane.insert(2, pn.layout.Divider(width=300))
        self.pbs_options_pane.sizing_mode = "stretch_width"
        self.pbs_options_pane.max_width = 800

        return self.pbs_options_pane

    @param.depends("disable_validation", "validated")
    def action_button(self):
        row = super().action_button()
        for btn in row:
            if btn.name in ["Submit", "Cancel"]:
                btn.js_on_click(code=f'setTimeout(function(){{window.location.href="{self.redirect_url}";}}, 5000)')

        return row

    @property
    def transfer_output_files(self):
        return None

    async def submit(self, custom_logs=None):
        self.job.script = self.pbs_script  # update script to ensure it reflects any UI updates
        job = await database_sync_to_async(UitPlusJob.instance_from_pbs_job)(self.job, self.tethys_user)
        job.custom_logs = custom_logs or self.custom_logs
        job.transfer_output_files = self.transfer_output_files
        await job.execute()
