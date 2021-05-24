from collections import OrderedDict
import json
import logging
import re

import param
import panel as pn

from django.contrib.auth.models import User
from uit_plus_job.models import UitPlusJob, EnvironmentProfile
from uit.gui_tools import HpcSubmit, FileSelector, HpcFileBrowser


log = logging.getLogger(__name__)


class TethysHpcSubmit(HpcSubmit):
    tethys_user = param.ClassSelector(User)
    environment_profile = param.ObjectSelector(label="Load Environment Profile")
    environment_profile_delete = param.ObjectSelector(label="Environment Profile to Delete")
    environment_profile_version = param.ObjectSelector(allow_None=True, precedence=2)
    save_name = param.String(label='Save As:')
    profiles = param.List()
    version = param.ObjectSelector(label='Set Version Default', precedence=1)
    show_save_panel = param.Boolean()
    show_delete_panel = param.Boolean()
    save_profile_btn = param.Action(lambda self: self.update_save_panel(), label='Save Current Profile', precedence=1)
    save_btn = param.Action(lambda self: self._save_current_profile(), label='Save')
    cancel_save_btn = param.Action(lambda self: self.cancel_save(), label='Cancel')
    delete_profile_btn = param.Action(lambda self: self.update_delete_panel(True), label='Delete Selected Profile')
    delete_btn = param.Action(lambda self: self._delete_current_profile(), label='Delete')
    cancel_delete_btn = param.Action(lambda self: self.update_delete_panel(False), label='Cancel')
    local_pbs_content = param.ClassSelector(bytes)

    # Parameters to override in subclass
    get_versions = param.Action(lambda uit_client: [], precedence=-1)
    version_environment_variable = 'VERSION'
    custom_logs = None
    redirect_url = '/'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.overwrite_request = None
        self.cb = None
        self.progress_bar = pn.widgets.misc.Progress(width=250, active=False, css_classes=["hidden"])
        self.alert = pn.pane.Alert(css_classes=['hidden'])
        self.no_version_profiles_alert = pn.pane.Alert(
            'No profiles have been created for the selected version',
            alert_type='warning', css_classes=['hidden'], margin=(0, 5, 20, 5))

        # Remote pbs selector
        self.select_pbs = FileSelector(
            help_text='Load environment from a PBS file',
        )

        self.select_pbs.param.watch(self.parse_remote_pbs, 'file_path')

        # Local pbs selector
        self.local_select_pbs = pn.widgets.FileInput(accept=".sh,.pbs")

        self.pbs_options_pane = None

    def get_profiles(self, version=None):
        kwargs = dict(
            user=self.tethys_user,
            hpc_system=self.uit_client.system,
            software=self.software,
        )
        if version is not None:
            kwargs['environment_variables__contains'] = f'"{self.version_environment_variable}": "{version}"'

        return sorted([p.name for p in EnvironmentProfile.objects.filter(**kwargs)])

    def get_profile(self, name):
        return EnvironmentProfile.objects.get(
            user=self.tethys_user,
            hpc_system=self.uit_client.system,
            software=self.software,
            name=name,
        )

    def get_default_profile(self, version=None, use_general_default=False):
        return EnvironmentProfile.get_default(
            self.tethys_user,
            self.uit_client.system,
            self.software,
            version=version,
            use_general_default=use_general_default
        )

    @param.depends('uit_client', watch=True)
    def initialize_versions(self):
        self.param.version.objects = ['System Default'] + self.get_versions(self.uit_client)
        self.version = 'System Default'

    @param.depends('version', watch=True)
    def update_version_profiles(self):
        version = None if self.version == 'System Default' else self.version
        profiles = self.get_profiles(version=version)
        version_default = self.get_default_profile(version=self.version, use_general_default=version is None)

        self.param.environment_profile_version.objects = profiles
        with param.discard_events(self):
            self.environment_profile_version = version_default
        if profiles:
            self.param.environment_profile_version.precedence = 2
            if 'hidden' not in self.no_version_profiles_alert.css_classes:
                self.no_version_profiles_alert.css_classes.append('hidden')
        else:
            self.param.environment_profile_version.precedence = -1
            if 'hidden' in self.no_version_profiles_alert.css_classes:
                self.no_version_profiles_alert.css_classes.remove('hidden')

    def update_save_panel(self):
        self.save_name = self.environment_profile
        self.show_save_panel = True

    def update_delete_panel(self, should_show):
        self.show_delete_panel = should_show

    def cancel_save(self):
        self.save_name = ''
        self.show_save_panel = False
        if self.overwrite_request:
            self._clear_alert()

    def set_pbs_options_alert(self, msg, alert_type='warning'):
        self.pbs_options_pane[1] = pn.pane.Alert(msg, alert_type=alert_type) if msg else None

    def validate_version(self):
        if self.environment_variables.get(self.version_environment_variable) != self.selected_version:
            self.set_pbs_options_alert(
                f'The selected profile does not match the selected version ({self.selected_version}). '
                f'Please select a compatible profile, or go the the "Environment" tab to create a new profile.')
        else:
            self.set_pbs_options_alert(None)

    @param.depends('environment_profile_version', watch=True)
    def set_default(self):
        if not self.environment_profile_version:
            return
        profile = self.get_profile(name=self.environment_profile_version)
        if self.version == 'System Default':
            EnvironmentProfile.set_general_default(self.tethys_user, profile)
        else:
            EnvironmentProfile.set_default_for_version(self.tethys_user, profile, self.version)
        self._alert(
            f'Default profile for version {self.version} is now set to {self.environment_profile_version}'
        )
        self.update_version_profiles()

    @param.depends('disable_validation', 'validated')
    def action_button(self):
        row = super().action_button()
        for btn in row:
            if btn.name in ['Submit', 'Cancel']:
                btn.js_on_click(code=f'setTimeout(function(){{window.location.href="{self.redirect_url}";}}, 1000)')

        return row

    def submit(self, custom_logs=None):
        job = UitPlusJob.instance_from_pbs_job(self.job, self.tethys_user)
        job.custom_logs = custom_logs or self.custom_logs
        job.execute()

    @param.depends("environment_profile", watch=True)
    def select_profile(self):
        if self.environment_profile and not self.environment_profile == "default":
            self._populate_profile_from_saved(self.environment_profile)

    def _load_profiles(self):
        """
        Get a list of profiles from the database
        that belong to this user
        """
        profiles = self.get_profiles()

        # Create default profile for user if one does not exist
        if len(profiles) == 0:
            log.info("Creating default profile")
            self.load_config_file()
            env_var_json = json.dumps(self.environment_variables)
            modules = {
                    "modules_to_load": self.modules_to_load,
                    "modules_to_unload": self.modules_to_unload
            }

            saving_profile = EnvironmentProfile(
                    user=self.tethys_user,
                    environment_variables=env_var_json,
                    modules=modules,
                    hpc_system=self.uit_client.system,
                    software=self.software,
                    name="system-default",
                    default_for_versions=[],
                    user_default=True)
            saving_profile.save()
            profiles = [saving_profile.name]

        self.profiles = profiles
        self.param.environment_profile.objects = \
            self.param.environment_profile_delete.objects = self.profiles
        for attr in ['environment_profile', 'environment_profile_delete']:
            if getattr(self, attr) not in self.profiles:
                setattr(self, attr, self.profiles[0])
        self.update_version_profiles()

    def _delete_current_profile(self, event=None):
        log.info("Deleting profile {}".format(self.environment_profile_delete))

        del_profile = self.get_profile(name=self.environment_profile_delete)

        del_profile.delete()
        self._alert("Removed {}".format(self.environment_profile_delete),
                    alert_type="danger")

        self._load_profiles()
        self._populate_profile_from_saved(self.profiles[0])
        self.update_delete_panel(False)

    def _save_current_profile(self, event=None):
        log.info("Saving profile")

        env_var_json = json.dumps(self.environment_variables)
        modules = {
                "modules_to_load": self.modules_to_load,
                "modules_to_unload": self.modules_to_unload
        }

        # Check to see if we have already loaded this model to overwrite
        # and were just asking for confirmation

        if self.overwrite_request is not None and self.overwrite_request.name == self.save_name:
            saving_profile = self.overwrite_request
            saving_profile.modules = modules
            saving_profile.environment_variables = env_var_json
            saving_profile.email = self.notification_email
            self.overwrite_request = None
        else:
            # Check to see if a profile already exists for this user with the same name
            try:
                overwrite_profile = self.get_profile(name=self.save_name)

                self.overwrite_request = overwrite_profile
                # Ask for confirmation before continuing
                self._alert("Are you sure you want to overwrite profile {}? Press save again to confirm.".format(
                        self.overwrite_request.name), alert_type="danger", timeout=False)
                return
            except EnvironmentProfile.DoesNotExist:
                # Creating a new one
                self.overwrite_request = None
                version = self.environment_variables[self.version_environment_variable]
                version_default = self.get_default_profile(version)
                default_for_versions = [version] if version_default is None else []

                saving_profile = EnvironmentProfile(
                    user=self.tethys_user,
                    environment_variables=env_var_json,
                    modules=modules,
                    hpc_system=self.uit_client.system,
                    software=self.software,
                    name=self.save_name,
                    email=self.notification_email,
                    default_for_versions=default_for_versions
                )

        saving_profile.save()
        self._load_profiles()
        self.environment_profile = self.save_name
        self._alert("Successfully saved.", alert_type="success")
        self.cancel_save()

    def _alert(self, message, alert_type="info", timeout=True):
        self._clear_alert()
        if 'hidden' in self.alert.css_classes:
            self.alert.css_classes.remove('hidden')
        self.alert.alert_type = alert_type
        self.alert.object = message
        if timeout:
            # Clear the alert after 3 seconds
            if self.cb is not None and self.cb.running:
                self.cb.stop()
            self.cb = pn.state.add_periodic_callback(self._clear_alert, period=10000, count=1)

    def _clear_alert(self, event=None):
        if 'hidden' not in self.alert.css_classes:
            self.alert.css_classes.append('hidden')
        self.alert.object = ''
        # Stop clear timer
        if self.cb is not None and self.cb.running:
            self.cb.stop()
        self.cb = None

    def _parse_pbs_body(self, body):
        """
        return the modules and environment
        variables parsed from pbs file contents.
        """
        tokenize = [line.rstrip().split() for line in body.split("\n")]

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
            if len(line) > 1 and line[0] == "export":
                # Add environment variable
                var_name = line[1].split("=")[0]
                # Refuse everything to the right of the first equals sign
                value = '='.join(line[1].split("=")[1:])
                env_vars[var_name] = value

        return {"modules_to_load": modules_to_load,
                "modules_to_unload": modules_to_unload,
                "environment_variables": env_vars}

    def _parse_pbs_directives(self, body):
        """
        Returns a dictionary of the directives
        specified in a PBS script
        """
        # Get general directives
        matches = re.findall('#PBS -(.*)', body)
        directives = {k: v for k, v in [(i.split() + [''])[:2] for i in matches]}
        # Get l directives
        l_matches = re.findall('#PBS -l (.*)', body)
        d = dict()
        for match in l_matches:
            if 'walltime' in match:
                d['walltime'] = match.split('=')[1]
            else:
                d.update({k: v for k, v in [i.split('=') for i in l_matches[0].split(':')]})

        directives['l'] = d
        return directives

    def _populate_profile_from_saved(self, name):
        """
        Load profile from db and populate params
        """
        profile = self.get_profile(name=name)

        if not profile:
            raise ValueError("Trying to load profile that doesn't exist.")

        self.environment_profile = profile.name
        modules = profile.modules
        self.modules_to_load = modules["modules_to_load"]
        self.modules_to_unload = modules["modules_to_unload"]
        self.environment_variables = OrderedDict(json.loads(profile.environment_variables))
        self.notification_email = profile.email or ''
        self.validate_version()

    @param.depends("uit_client", watch=True)
    def update_pbs_select(self):
        self.select_pbs.file_browser = HpcFileBrowser(self.uit_client, delayed_init=False,
                patterns=['*.pbs', '*.sh'])
        self.select_pbs.show_browser = False

    @param.depends("local_pbs_content", watch=True)
    def parse_local_pbs(self):
        self.loading = True
        pbs_body = str(self.local_pbs_content.decode('ascii'))
        self.populate_from_pbs(pbs_body)
        self.loading = False

    def parse_remote_pbs(self, e):
        pbs_file_path = e.obj.file_path or ''
        if pbs_file_path.endswith('.pbs') or pbs_file_path.endswith('.sh'):
            self.loading = True
            pbs_body = self.uit_client.call(f'cat {pbs_file_path}')
            self.populate_from_pbs(pbs_body)
            self.loading = False
            self.select_pbs.show_browser = False

    def populate_from_pbs(self, pbs_body):
        parsed_pbs = self._parse_pbs_body(pbs_body)
        for module_prefix in parsed_pbs["modules_to_load"]:
            for found_module in self.param.modules_to_load.objects:
                if found_module.startswith(module_prefix):
                    self.modules_to_load = self.modules_to_load + [found_module,]
                    break
        for module_prefix in parsed_pbs["modules_to_unload"]:
            for found_module in self.param.modules_to_unload.objects:
                if found_module.startswith(module_prefix):
                    self.modules_to_unload = self.modules_to_unload + [found_module,]
                    break

        new_env_vars = self.environment_variables.copy()
        for k, v in parsed_pbs["environment_variables"].items():
            new_env_vars[k] = v
        self.environment_variables = OrderedDict(new_env_vars)

        # Load directives
        directives = self._parse_pbs_directives(pbs_body)
        self.hpc_subproject = directives.get('A') or self.hpc_subproject
        if directives.get('l'):
            self.nodes = int(directives['l']['select'])
            self.processes_per_node = int(directives['l']['ncpus'])
            self.wall_time = directives['l']['walltime']
        self.queue = directives.get('q') or self.queue
        self.notification_email = directives.get('M') or self.notification_email
        if directives.get('m'):
            self.notify_start = 'b' in directives['m']
            self.notify_end = 'e' in directives['m']

    def pbs_options_view(self):
        self.pbs_options_pane = super().pbs_options_view()
        self.pbs_options_pane.insert(0, pn.Param(self.param.environment_profile, widgets={'environment_profile': {'width': 300}}))
        self.pbs_options_pane.insert(1, None)
        self.pbs_options_pane.insert(2, pn.layout.Divider(width=300))
        self.pbs_options_pane.sizing_mode = 'stretch_width'
        self.pbs_options_pane.max_width = 800

        return self.pbs_options_pane

    def advanced_options_view(self):
        """
        Overrides HpcSubmit function in order to
        add a panel to select environment profiles.
        """

        if not self.profiles:
            self._load_profiles()

        # Load default profile
        default = self.get_default_profile(self.selected_version, use_general_default=True)
        if default is not None:
            self._populate_profile_from_saved(default.name)

        options = super().advanced_options_view()
        # Insert profile panel into view
        options.insert(0, pn.Row(
            pn.Param(self,
                     parameters=["environment_profile"],
                     show_name=False),
            pn.Card(self.pbs_select_panel, title="From PBS Script",
                    collapsed=True, margin=(20, 0, 0, 20))
        ))

        options.extend((self.save_panel, self.alert, pn.Card(
            self.profile_management_panel, title='Manage Profiles',
            collapsed=True,
            sizing_mode='stretch_width',
        )))
        return options

    def profile_management_panel(self):
        return pn.Row(
            pn.Column(
                pn.Param(
                    self,
                    parameters=['version', 'environment_profile_version', 'set_default_btn'],
                    widgets={
                        'set_default_btn': {'button_type': 'primary', 'width': 200, 'margin': (23, 0, 0, 0)},
                        'version': {'width': 200},
                        'environment_profile_version': pn.widgets.RadioBoxGroup,
                    },
                    show_name=False,
                ),
                self.no_version_profiles_alert,
            ),
            pn.Column(
                self.param.environment_profile_delete,
                self.delete_panel,
            ),
        )

    @param.depends('show_delete_panel')
    def delete_panel(self):
        if self.show_delete_panel:
            return pn.Column(
                pn.pane.Alert('Are you sure you want to delete the selected profile? This action cannot be undone.',
                              alert_type='danger'),
                pn.Param(
                    self,
                    parameters=['delete_btn', 'cancel_delete_btn'],
                    widgets={'delete_btn': {'button_type': 'danger', 'width': 100},
                             'cancel_delete_btn': {'button_type': 'success', 'width': 100}},
                    default_layout=pn.Row,
                    show_name=False,
                )
            )
        else:
            return pn.Param(
                self.param.delete_profile_btn,
                widgets={'delete_profile_btn': {'button_type': 'danger', 'width': 200, 'margin': (18, 0, 0, 0)}},
            )

    @param.depends('show_save_panel')
    def save_panel(self):
        if self.show_save_panel:
            return pn.Column(
                pn.pane.Alert('The notification e-mail address from the the '
                              'PBS Options tab will also be saved as part of this profile.', alert_type='info'),
                self.param.save_name,
                pn.Param(
                    self,
                    parameters=['save_btn', 'cancel_save_btn'],
                    widgets={'save_btn': {'button_type': 'success', 'width': 100},
                             'cancel_save_btn': {'button_type': 'danger', 'width': 100}},
                    default_layout=pn.Row,
                    show_name=False,
                )
            )
        else:
            return pn.Param(
                self.param.save_profile_btn,
                widgets={'save_profile_btn': {'button_type': 'success', 'width': 200}}
            )

    def pbs_select_panel(self):
        return pn.Column(
            pn.Row(
                pn.pane.HTML("<label class=\"bk\">Upload local script:</label>"),
                pn.Param(self.param.local_pbs_content,
                         widgets={"local_pbs_content": self.local_select_pbs}),
                ),
            pn.pane.HTML("<label class=\"bk\">Select remote script:</label>"),
            pn.layout.WidgetBox(self.select_pbs.panel),
        )
