import mock
import datetime
from uit_plus_job.models import UitPlusJob
from django.contrib.auth.models import User
from datetime import timedelta
from pytz import timezone
from django.core.exceptions import ValidationError
from django.test import TransactionTestCase
from social_django.models import UserSocialAuth


class TestUitPlusJob(TransactionTestCase):

    def setUp(self):
        self.tz = timezone('America/Denver')

        self.user = User.objects.create_user('tethys1', 'user@example.com', 'pass')

        self.social_auth = UserSocialAuth.create_social_auth(self.user, 'username', 'UITPlus')

        self.uitplusjob = UitPlusJob(
            name='uit_job',
            user=self.user,
            description='test_description',
            label='test_label',
            workspace='test_ws',
            node_type='compute',
            system='topaz',
            job_id='J0001',
            project_id='P001',
            num_nodes=10,
            processes_per_node=5,
            max_time=timedelta(hours=10, seconds=42),
            max_cleanup_time=timedelta(hours=10, seconds=60),
            job_script='PBSScript',
            transfer_input_files=['file1.xml', 'file10.xml'],
            archive_input_files=['file2.xml', 'file3.txt'],
            home_input_files=['file3.xml', 'file4.xml'],
            transfer_output_files=['transfer_out.out', 'transfer_out2.out'],
            archive_output_files=['archive_out.out', 'archive_out2.out'],
            home_output_files=['home.out', 'test_home.out'],
            _modules={'OpenGL': 'load'})

        self.uitplusjob.save()

    def tearDown(self):
        self.uitplusjob.delete()
        self.user.delete()

    def test_init(self):
        self.assertEqual('uit_job', self.uitplusjob.name)
        self.assertEqual('test_description', self.uitplusjob.description)
        self.assertEqual('test_label', self.uitplusjob.label)
        self.assertEqual('uit_job', self.uitplusjob.name)
        self.assertEqual('P001', self.uitplusjob.project_id)
        self.assertEqual(10, self.uitplusjob.num_nodes)
        self.assertEqual(5, self.uitplusjob.processes_per_node)
        self.assertEqual(timedelta(0, 36042), self.uitplusjob.max_time)
        self.assertEqual('debug', self.uitplusjob.queue)
        self.assertEqual('compute', self.uitplusjob.node_type)
        self.assertEqual('topaz', self.uitplusjob.system)
        self.assertTrue(self.uitplusjob.transfer_job_script)

    def test_init_args(self):
        uitplusjob_args = UitPlusJob(
            100,  # id
            'uit_job',  # name
            'test_description',  # description
            self.user.id,  # user
            'test_label',  # label
            datetime.datetime(2018, 1, 1),  # create_time
            datetime.datetime(2018, 1, 1),  # execute_time
            datetime.datetime(2018, 1, 1),  # start_time
            datetime.datetime(2018, 1, 1),  # complete_time
            'test_ws',  # workspace
            {},  # extended_properties
            'test2',  # process_results_function
            'R',  # status
            'test3',  # tethysjob_ptr
            'J0001',  # job_id
            'P001',  # project_id
            'topaz',  # system
            'compute',  # node_type
            10,  # num_nodes
            5,  # process_per_node
            timedelta(hours=10, seconds=42),  # max_time
            timedelta(hours=10, seconds=60),  # max_cleanup_time
            'debug',  # queue
            'PBSScript',  # job_script
            True,  # transfer_job_script
            ['file1.xml', 'file10.xml'],  # transfer_input_files
            ['file2.xml', 'file3.txt'],  # archive_input_files
            ['file3.xml', 'file4.xml'],  # home_input_files
            ['transfer_out.out', 'transfer_out2.out'],  # transfer_output_files
            ['archive_out.out', 'archive_out2.out'],  # archive_output_files
            ['home.out', 'test_home.out'],  # home_output_files
            {'OpenGL': 'load'},  # _modules
            '-j',  # _optional_directives
            '12345',  # _remote_workspace_id
            'workspace')  # _remote_workspace

        self.assertEqual('uit_job', uitplusjob_args.name)
        self.assertEqual('test_description', uitplusjob_args.description)
        self.assertEqual('PBSScript', uitplusjob_args.job_script)

    def test_node_type(self):
        self.uitplusjob.node_type = 'compute'

        # Field Validation
        self.uitplusjob.clean_fields()

    def test_node_type_error(self):
        self.uitplusjob.node_type = 'wrong'

        # Field Validation
        self.assertRaises(ValidationError, self.uitplusjob.clean_fields)

    def test_system(self):
        self.uitplusjob.system = 'topaz'

        # Field Validation
        self.uitplusjob.clean_fields()

    def test_system_error(self):
        self.uitplusjob.system = 'wrong'

        # Field Validation
        self.assertRaises(ValidationError, self.uitplusjob.clean_fields)

    def test_job_script_name_prop(self):
        job_script_name = self.uitplusjob.job_script_name
        self.assertEqual('PBSScript', job_script_name)

    def test_job_script_name_prop_attribute_error(self):
        self.uitplusjob.job_script = None
        ret = self.uitplusjob.job_script_name
        self.assertEqual('', ret)

    def test_token(self):
        mock_social = mock.MagicMock()
        self.user.social_auth.get = mock_social
        self.uitplusjob.user = self.user

        self.social_auth.extra_data = {'access_token': 'foo'}
        self.social_auth.save()

        ret_token = self.uitplusjob.token
        self.assertEqual('foo', ret_token)

    def test_token_error(self):
        self.social_auth.extra_data = {}
        self.social_auth.save()
        self.assertIsNone(self.uitplusjob.token)

    def test_remote_workspace_suffix_prop(self):
        remote_workspace = self.uitplusjob.remote_workspace_suffix
        self.assertIn('test_label/uit_job', remote_workspace)

    @mock.patch('uit_plus_job.models.UitPlusJob.get_environment_variable')
    def test_work_dir_prop(self, mock_env_work_dir):
        mock_env_work_dir.return_value = '{WORK_DIR}'

        # calling the property
        work_dir = self.uitplusjob.work_dir

        # test the results
        mock_env_work_dir.assert_called_with('WORKDIR')
        self.assertIn('{WORK_DIR}/test_label/uit_job/', work_dir)

    @mock.patch('uit_plus_job.models.UitPlusJob.get_environment_variable')
    def test_archive_dir_prop(self, mock_env_arc_dir):
        mock_env_arc_dir.return_value = '{ARCH_DIR}'
        archive_dir = self.uitplusjob.archive_dir
        mock_env_arc_dir.assert_called_with('ARCHIVE_HOME')
        self.assertIn('{ARCH_DIR}/test_label/uit_job', archive_dir)

    @mock.patch('uit_plus_job.models.UitPlusJob.get_environment_variable')
    def test_home_dir_prop(self, mock_env_home_dir):
        mock_env_home_dir.return_value = '{HOME_DIR}'
        home_dir = self.uitplusjob.home_dir
        mock_env_home_dir.assert_called_with('HOME')
        self.assertIn('{HOME_DIR}/test_label/uit_job', home_dir)

    @mock.patch('uit_plus_job.models.Client')
    @mock.patch('uit_plus_job.models.UitPlusJob.token')
    def test_client_prop(self, mock_token, mock_client):
        mock_client_ret = mock.MagicMock()
        mock_client.return_value = mock_client_ret

        # Execute
        ret = self.uitplusjob.client

        # check return value
        self.assertEqual(mock_client_ret, ret)

        # check assert call
        mock_client.assert_called_with(token=mock_token)
        mock_client_ret.connect.assert_called_with(system='topaz')

    @mock.patch('uit_plus_job.models.UitPlusJob.client')
    def test_get_environment_variable(self, mock_client):
        mock_client.call.return_value = "test_return"

        ret = self.uitplusjob.get_environment_variable('WORKDIR')

        # test results
        call_args = mock_client.call.call_args_list
        self.assertEqual('echo $WORKDIR', call_args[0][1]['command'])
        self.assertEqual('/tmp', call_args[0][1]['work_dir'])
        self.assertEqual('test_return', ret)

    # @mock.patch('uit_plus_job.models.UitPlusJob.get_environment_variable')
    # def test_render_execution_block(self, mock_get_env):
    #     mock_get_env.side_effect = ['{WORKDIR}', '{ARCHIVE_HOME}', '{HOME_DIR}']
    #
    #     ret = self.uitplusjob.render_script_templates()
    #     self.assertIn("mkdir -p {WORKDIR}/test_label/uit_job", ret)
    #     self.assertIn('cd {WORKDIR}/test_label/uit_job', ret)
    #     self.assertIn('archive get -C ${ARCHIVE_HOME}/file2.xml', ret)
    #     self.assertIn('cp ${HOME}/file3.xml .', ret)
    #     self.assertIn('cp ${HOME}/file4.xml .', ret)
    #     self.assertIn('chmod +x PBSScript', ret)
    #     self.assertIn('./PBSScript', ret)
    #     self.assertIn('cd {WORKDIR}/test_label/uit_job', ret)
    #     self.assertIn('#PBS -l walltime=10:01:00', ret)
    #     self.assertIn('#PBS -A P001', ret)
    #     self.assertIn('archive mkdir -p {ARCHIVE_HOME}/test_label/uit_job/', ret)
    #     self.assertIn('archive put -C {ARCHIVE_HOME}/test_label/uit_job/', ret)
    #     self.assertIn('archive ls {ARCHIVE_HOME}/test_label/uit_job', ret)
    #     self.assertIn('mkdir -p {HOME_DIR}/test_label/uit_job', ret)
    #     self.assertIn('cp home.out {HOME_DIR}/test_label/uit_job', ret)
    #     self.assertIn('cp home.out {HOME_DIR}/test_label/uit_job', ret)
    #     self.assertIn('cp transfer_out.out {HOME_DIR}/test_label/uit_job', ret)
    #     self.assertIn('rm -rf {WORKDIR}/test_label/uit_job', ret)

    @mock.patch('uit_plus_job.models.UitPlusJob.get_environment_variable')
    @mock.patch('uit_plus_job.models.UitPlusJob.client')
    @mock.patch('django.db.models.base.Model.save')
    def test_execute(self, mock_save, mock_client, mock_env):
        mock_env.return_value = '{WORKDIR}'
        mock_client.put_file.side_effect = 'success'
        mock_client.submit.return_value = 'J001'

        # call the method
        self.uitplusjob._execute()

        # testing the client call input arguments
        call_args = mock_client.call.call_args_list
        self.assertIn('mkdir -p {WORKDIR}/test_label/uit_job', call_args[0][1]['command'])
        self.assertIn('/tmp', call_args[0][1]['work_dir'])

        put_call_args = mock_client.put_file.call_args_list
        self.assertEqual('file1.xml',  put_call_args[0][1]['local_path'])
        self.assertIn('{WORKDIR}/test_label/uit_job',  put_call_args[0][1]['remote_path'])
        self.assertEqual('file10.xml',  put_call_args[1][1]['local_path'])
        self.assertIn('{WORKDIR}/test_label/uit_job',  put_call_args[1][1]['remote_path'])
        self.assertEqual('PBSScript',  put_call_args[2][1]['local_path'])
        self.assertIn('{WORKDIR}/test_label/uit_job',  put_call_args[2][1]['remote_path'])

        submit_call_args = mock_client.submit.call_args_list
        self.assertIsInstance(submit_call_args[0][0][0], UitPlusJob)
        self.assertIn('{WORKDIR}/test_label/uit_job/', submit_call_args[0][0][1])

        mock_client.put_file.assert_called()
        self.assertEqual('J001', mock_client.submit())
        mock_save.assert_called()

    @mock.patch('uit_plus_job.models.UitPlusJob.get_environment_variable')
    @mock.patch('uit_plus_job.models.UitPlusJob.client')
    @mock.patch('django.db.models.base.Model.save')
    def test_execute_client_call_runtime_error(self, mock_save, mock_client, mock_env):
        mock_env.return_value = '{WORKDIR}'
        mock_client.call.side_effect = RuntimeError
        # call the method
        self.assertRaises(RuntimeError, self.uitplusjob._execute)
        mock_save.assert_called()

    @mock.patch('uit_plus_job.models.UitPlusJob.get_environment_variable')
    @mock.patch('uit_plus_job.models.UitPlusJob.client')
    @mock.patch('django.db.models.base.Model.save')
    def test_execute_transfer_input_file_runtime_error(self, mock_save, mock_client, mock_env):
        mock_env.return_value = '{WORKDIR}'
        mock_client.put_file.return_value = {'success': 'false', 'error': 'test error'}

        # call the method
        self.assertRaises(RuntimeError, self.uitplusjob._execute)
        mock_save.assert_called()

        # testing the client call input arguments
        call_args = mock_client.call.call_args_list
        self.assertIn('mkdir -p {WORKDIR}/test_label/uit_job', call_args[0][1]['command'])
        self.assertIn('/tmp', call_args[0][1]['work_dir'])

    @mock.patch('uit_plus_job.models.UitPlusJob.get_environment_variable')
    @mock.patch('uit_plus_job.models.UitPlusJob.client')
    @mock.patch('django.db.models.base.Model.save')
    def test_execute_transfer_script_runtime_error(self, mock_save, mock_client, mock_env):
        mock_env.return_value = '{WORKDIR}'
        mock_client.put_file.side_effect = [{'success': 'true'}, {'success': 'true'}, {'success': 'false',
                                                                                       'error': 'test error'}]
        # call the method
        self.assertRaises(RuntimeError, self.uitplusjob._execute)
        mock_save.assert_called()

        # testing the client call input arguments
        call_args = mock_client.call.call_args_list
        self.assertIn('mkdir -p {WORKDIR}/test_label/uit_job', call_args[0][1]['command'])
        self.assertIn('/tmp', call_args[0][1]['work_dir'])

    def test_parse_status(self):
        status_string = '\n' \
                        'topaz10: \n'\
                        '                                                            Reqd  Reqd   Elap\n'\
                        'Job ID          Username Queue    Jobname    SessID NDS TSK Memory Time  S Time\n' \
                        ' --------------- -------- -------- ---------- ------ --- --- ------ ----- - -----\n' \
                        ' 3101546.topaz10 user     transfer cleanup.pb --     1   1   --     00:05 R --\n'

        ret = self.uitplusjob._parse_status(status_string)

        self.assertEqual('RUN', ret)

    def test_parse_status_index_error(self):
        ret = self.uitplusjob._parse_status(None)

        self.assertEqual('ERR', ret)

    @mock.patch('uit_plus_job.models.UitPlusJob.client')
    def test_get_remote_file_no_local_path(self, mock_client):
        remote_files_names = ['file1.xml']
        remote_dir = 'WORKDIR'
        mock_client.get_file.return_value = {'success': True}
        self.assertFalse(self.uitplusjob.get_remote_files(remote_dir=remote_dir, remote_filenames=remote_files_names))

    @mock.patch('uit_plus_job.models.log')
    @mock.patch('uit_plus_job.models.UitPlusJob.client')
    def test_get_remote_file_io_error(self, mock_client, mock_log):
        remote_files_names = ['file1.xml']
        remote_dir = 'WORKDIR'
        mock_client.get_file.side_effect = IOError

        # call the method
        ret = self.uitplusjob.get_remote_files(remote_dir=remote_dir, remote_filenames=remote_files_names)

        # test results
        self.assertFalse(ret)
        self.assertEqual('Failed to get remote file: ', mock_log.error.call_args_list[0][0][0])

    @mock.patch('uit_plus_job.models.os')
    @mock.patch('uit_plus_job.models.UitPlusJob.client')
    def test_get_remote_file(self, mock_client, mock_os):
        remote_files_names = ['file1.xml']
        remote_dir = 'WORKDIR'
        mock_os.path.join.side_effect = ['local_path', 'remote_path']
        mock_client.get_file.return_value = {'success': True}
        mock_os.path.exists.return_value = True
        ret = self.uitplusjob.get_remote_files(remote_dir=remote_dir, remote_filenames=remote_files_names)

        # test results
        self.assertTrue(ret)
        call_args = mock_client.get_file.call_args
        self.assertEqual('local_path', call_args[1]['local_path'])
        self.assertEqual('remote_path', call_args[1]['remote_path'])

    @mock.patch('uit_plus_job.models.UitPlusJob.client')
    def test_stop(self, mock_client):
        # call the method
        self.assertTrue(self.uitplusjob.stop())

        # test results
        call_args = mock_client.call.call_args_list
        self.assertDictEqual({'command': 'echo $WORKDIR', 'work_dir': '/tmp'},
                             call_args[0][1])
        self.assertEqual('qdel J0001', call_args[1][1]['command'])

    @mock.patch('uit_plus_job.models.UitPlusJob.client')
    def test_stop_runtime_error(self, mock_client):
        mock_client.call.side_effect = RuntimeError

        # call the method
        self.assertFalse(self.uitplusjob.stop())

        # test results
        call_args = mock_client.call.call_args_list
        self.assertDictEqual({'command': 'echo $WORKDIR', 'work_dir': '/tmp'},
                             call_args[0][1])

    @mock.patch('uit_plus_job.models.UitPlusJob.client')
    def test_pause(self, mock_client):
        # call the method
        self.assertTrue(self.uitplusjob.pause())

        # test results
        call_args = mock_client.call.call_args_list
        self.assertDictEqual({'command': 'echo $WORKDIR', 'work_dir': '/tmp'},
                             call_args[0][1])
        self.assertEqual('qhold J0001', call_args[1][1]['command'])

    @mock.patch('uit_plus_job.models.UitPlusJob.client')
    def test_pause_runtime_error(self, mock_client):
        mock_client.call.side_effect = RuntimeError

        # call the method
        self.assertFalse(self.uitplusjob.pause())

        # test results
        call_args = mock_client.call.call_args_list
        self.assertDictEqual({'command': 'echo $WORKDIR', 'work_dir': '/tmp'},
                             call_args[0][1])

    @mock.patch('uit_plus_job.models.UitPlusJob.client')
    def test_resume(self, mock_client):
        # call the method
        self.assertTrue(self.uitplusjob.resume())

        # test results
        call_args = mock_client.call.call_args_list

        self.assertDictEqual({'command': 'echo $WORKDIR', 'work_dir': '/tmp'},
                             call_args[0][1])
        self.assertEqual('qrls J0001', call_args[1][1]['command'])

    @mock.patch('uit_plus_job.models.UitPlusJob.client')
    def test_resume_runtime_error(self, mock_client):
        mock_client.call.side_effect = RuntimeError

        # call the method
        self.assertFalse(self.uitplusjob.resume())

        # test results
        call_args = mock_client.call.call_args_list

        self.assertDictEqual({'work_dir': '/tmp', 'command': 'echo $WORKDIR'},
                             call_args[0][1])

    @mock.patch('uit_plus_job.models.UitPlusJob.render_clean_script')
    @mock.patch('uit_plus_job.models.UitPlusJob.client')
    def test_clean(self, mock_client, mock_pbs):
        mock_pbs.return_value = 'test_pbs_render_out'
        self.assertTrue(self.uitplusjob.clean(archive=True))
        client_submit_args = mock_client.submit.call_args_list
        self.assertEqual('test_pbs_render_out', client_submit_args[0][0][0])
        self.assertDictEqual({'remote_name': 'clean.pbs'},  client_submit_args[0][1])

    @mock.patch('uit_plus_job.models.UitPlusJob.render_clean_script')
    @mock.patch('uit_plus_job.models.UitPlusJob.client')
    def test_clean_runtime_error(self, mock_client, mock_pbs):
        mock_client.submit.side_effect = RuntimeError
        mock_pbs.return_value = 'test_pbs_render_out'
        self.assertFalse(self.uitplusjob.clean(archive=True))
        client_submit_args = mock_client.submit.call_args_list
        self.assertEqual('test_pbs_render_out', client_submit_args[0][0][0])
        self.assertDictEqual({'remote_name': 'clean.pbs'}, client_submit_args[0][1])

    @mock.patch('uit_plus_job.models.UitPlusJob.get_environment_variable')
    def test_render_clean_script(self, mock_get_env):
        mock_get_env.side_effect = ['{WORKDIR}', '{ARCHIVE_HOME}', '{HOME_DIR}']

        # call the method
        res = self.uitplusjob.render_clean_script()

        # test the results
        self.assertIn('#PBS -l walltime=10:01:00', res)
        self.assertIn('#PBS -A P001', res)
        self.assertIn('rm -rf {WORKDIR}/test_label/uit_job/', res)
        self.assertIn('rm -rf {HOME_DIR}/test_label/uit_job/', res)

    @mock.patch('uit_plus_job.models.UitPlusJob.get_remote_files')
    @mock.patch('uit_plus_job.models.UitPlusJob.get_environment_variable')
    def test_process_results(self, mock_get_env, mock_remote_files):
        mock_get_env.side_effect = ['{WORKDIR}', '{HOME_DIR}']

        # call the method
        self.uitplusjob._process_results()

        # test results
        call_args = mock_remote_files.call_args_list
        self.assertListEqual(['transfer_out.out', 'transfer_out2.out'], call_args[0][0][1])
        self.assertListEqual(['log.stdout', 'log.stderr'], call_args[1][0][1])

    @mock.patch('uit_plus_job.models.UitPlusJob.client')
    def test_update_status_no_cleanup_job_id(self, mock_client):
        mock_client.call.return_value = \
            '\n' \
            'topaz10: \n'\
            '                                                            Reqd  Reqd   Elap\n'\
            'Job ID          Username Queue    Jobname    SessID NDS TSK Memory Time  S Time\n' \
            ' --------------- -------- -------- ---------- ------ --- --- ------ ----- - -----\n' \
            ' 3101546.topaz10 user     transfer cleanup.pb --     1   1   --     00:05 F --\n'

        # test the method
        self.assertRaises(RuntimeError, self.uitplusjob._update_status)

    @mock.patch('uit_plus_job.models.UitPlusJob.client')
    @mock.patch('django.db.models.base.Model.save')
    def test_update_status_running(self, mock_save, mock_client):
        mock_client.call.return_value = \
            '\n' \
            'topaz10: \n' \
            '                                                            Reqd  Reqd   Elap\n' \
            'Job ID          Username Queue    Jobname    SessID NDS TSK Memory Time  S Time\n' \
            ' --------------- -------- -------- ---------- ------ --- --- ------ ----- - -----\n' \
            ' 3101546.topaz10 user     transfer cleanup.pb --     1   1   --     00:05 R --\n'

        # call the method
        self.uitplusjob._update_status()

        # test results
        mock_save.assert_called()
        self.assertEqual('RUN', self.uitplusjob._status)

    @mock.patch('uit_plus_job.models.log')
    @mock.patch('uit_plus_job.models.UitPlusJob.client')
    def test_update_status_runtime_error(self, mock_client, mock_logging):
        mock_client.call.side_effect = RuntimeError

        # call the method
        self.assertIsNone(self.uitplusjob._update_status())

        # test results
        call_args = mock_client.call.call_args_list
        self.assertDictEqual({'work_dir': '/tmp', 'command': 'qstat -H J0001'},
                             call_args[0][1])

        self.assertEqual('Attempt to get status for job %s failed: %s', mock_logging.error.call_args_list[0][0][0])
        self.assertEqual('J0001', mock_logging.error.call_args_list[0][0][1])

    @mock.patch('uit_plus_job.models.log')
    @mock.patch('uit_plus_job.models.UitPlusJob.client')
    def test_update_status_dproute_error(self, mock_client, mock_logging):
        from uit.exceptions import DpRouteError
        mock_client.call.side_effect = DpRouteError("DP_Route_Error")

        # call the method
        self.assertIsNone(self.uitplusjob._update_status())

        # test results
        call_args = mock_client.call.call_args_list
        self.assertDictEqual({'command': 'qstat -H J0001', 'work_dir': '/tmp'},
                             call_args[0][1])

        self.assertEqual('qstat -H J0001', call_args[0][1]['command'])
        self.assertEqual('/tmp', call_args[0][1]['work_dir'])

        self.assertEqual('Ignoring DP_Route error: DP_Route_Error', mock_logging.info.call_args[0][0])

    @mock.patch('uit_plus_job.models.UitPlusJob.client')
    @mock.patch('django.db.models.base.Model.save')
    def test_update_status_submitted(self, mock_save, mock_client):
        mock_client.call.return_value = \
            '\n' \
            'topaz10: \n' \
            '                                                            Reqd  Reqd   Elap\n' \
            'Job ID          Username Queue    Jobname    SessID NDS TSK Memory Time  S Time\n' \
            '--------------- -------- -------- ---------- ------ --- --- ------ ----- - -----\n' \
            '3101546.topaz10 user     transfer cleanup.pb --     1   1   --     00:05 F --\n'

        self.uitplusjob.extended_properties = {'cleanup_job_id': 'C0001'}

        # call the method
        self.uitplusjob._update_status()

        # test results
        mock_save.assert_called()
        self.assertEqual('SUB', self.uitplusjob._status)
        self.assertEqual('C0001', self.uitplusjob.job_id)
