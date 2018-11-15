import unittest
import mock
from tethysapp.uit_plus_job.models import UitPlusJob
from django.contrib.auth.models import User
from datetime import timedelta
from pytz import timezone
from django.core.exceptions import ValidationError


class TestUitPlusJob(unittest.TestCase):

    def setUp(self):
        self.tz = timezone('America/Denver')

        self.user = User.objects.create_user('tethys1', 'user@example.com', 'pass')

        self.uitplusjob = UitPlusJob(
            name='test_tethysjob',
            description='test_description',
            user=self.user,
            label='test_label',
            job_name='uit_job',
            project_id='P001',
            num_nodes=10,
            processes_per_node=5,
            max_time=timedelta(hours=10, seconds=42),
            _optional_directives=[('J', 'OpenMP')],
            _modules={'OpenGL': 'load'},
            job_script='PBSScript',
            transfer_input_files=['file1.xml', 'file10.xml'],
            archive_input_files=['file2.xml', 'file3.txt'],
            home_input_files=['file3.xml', 'file4.xml'],
            transfer_output_files=['transfer_out.out', 'transfer_out2.out'],
            archive_output_files=['archive_out.out', 'archive_out2.out'],
            home_output_files=['home.out', 'test_home.out'],
            job_id='J0001',
            workspace='test_ws')

        self.uitplusjob.save()

    def tearDown(self):
        self.uitplusjob.delete()
        self.user.delete()

    def test_init(self):
        self.assertEqual('test_tethysjob', self.uitplusjob.name)
        self.assertEqual('test_description', self.uitplusjob.description)
        self.assertEqual('test_label', self.uitplusjob.label)
        self.assertEqual('uit_job', self.uitplusjob.job_name)
        self.assertEqual('P001', self.uitplusjob.project_id)
        self.assertEqual(10, self.uitplusjob.num_nodes)
        self.assertEqual(5, self.uitplusjob.processes_per_node)
        self.assertEqual(timedelta(0, 36042), self.uitplusjob.max_time)
        self.assertEqual('debug', self.uitplusjob.queue)
        self.assertEqual('compute', self.uitplusjob.node_type)
        self.assertEqual('topaz', self.uitplusjob.system)
        self.assertTrue(self.uitplusjob.transfer_job_script)

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

    def test_remote_workspace_prop(self):
        remote_workspace = self.uitplusjob.remote_workspace
        self.assertIn('test_label/uit_job', remote_workspace)

    def test_job_work_dir_prop(self):
        work_dir = self.uitplusjob.job_work_dir
        self.assertIn('${WORKDIR}/test_label/uit_job', work_dir)

    def test_job_archive_dir_prop(self):
        archive_dir = self.uitplusjob.job_archive_dir
        self.assertIn('${ARCHIVE_HOME}/test_label/uit_job', archive_dir)

    def test_job_home_dir_prop(self):
        home_dir = self.uitplusjob.job_home_dir
        self.assertIn('${HOME}/test_label/uit_job', home_dir)

    @mock.patch('tethysapp.uit_plus_job.models.Client')
    def test_get_client(self, mock_client):
        mock_client_ret = mock.MagicMock()
        mock_client.return_value = mock_client_ret

        # Execute
        ret = self.uitplusjob.get_client(token='test_token')

        # check return value
        self.assertEqual(mock_client_ret, ret)

        # check assert call
        mock_client.assert_called_with(token='test_token')
        mock_client_ret.connect.assert_called()

    def test_set_directive(self):
        # Call the method
        self.uitplusjob.set_directive('M', 'OpenGL')

        # Get the _directives
        ret = self.uitplusjob.get_directive('-M')

        # Test the results
        self.assertEqual('OpenGL', ret)

    def test_get_directive(self):
        # Call the method
        res = self.uitplusjob.get_directive('-J')

        # Test the result
        self.assertEqual('OpenMP', res)

    def test_get_directives(self):
        # Call the method
        res = self.uitplusjob.get_directives()

        # Test the result
        self.assertEqual('OpenMP', res[0].options)

    def test_load_module(self):
        # load anaconda module
        self.uitplusjob.load_module('anaconda')

        # get all the modules
        ret = self.uitplusjob.get_modules()

        self.assertEqual('load', ret['anaconda'])

    def test_unload_module(self):
        # load anaconda module
        self.uitplusjob.unload_module('C++')

        # get all the modules
        ret = self.uitplusjob.get_modules()

        self.assertEqual('unload', ret['C++'])

    def test_swap_module(self):
        # load modules
        self.uitplusjob.swap_module('OpenMP', 'C++')

        # get all the modules
        ret = self.uitplusjob.get_modules()

        self.assertEqual('C++', ret['OpenMP'])

    def test_render_execution_block(self):
        ret = self.uitplusjob.render_execution_block()
        self.assertIn('mkdir -p $${WORKDIR}/test_label/uit_job', ret)
        self.assertIn('mkdir -p J0001', ret)
        self.assertIn('archive get -C ${ARCHIVE_HOME}/test_label/uit_job', ret)
        self.assertIn('archive get -C ${HOME}/test_label/uit_job/', ret)
        self.assertIn('PBSScript', ret)
        self.assertIn('cd ${WORKDIR}/test_label/uit_job', ret)
        self.assertIn('#PBS -l walltime=10:00:42', ret)
        self.assertIn('archive mv archive_out2.out ${ARCHIVE_HOME}/test_label/uit_job', ret)
        self.assertIn('archive mv transfer_out2.out ${HOME}/test_label/uit_job', ret)
        self.assertIn('#rm -rf J0001', ret)

    def test_generate_pbs_script(self):
        # set directives and modules
        self.uitplusjob.set_directive('J', 'OpenMP')
        self.uitplusjob.set_directive('T', 'OpenGL')
        self.uitplusjob.load_module('test_load')
        self.uitplusjob.unload_module('test_unload')
        self.uitplusjob.swap_module('c++', 'C#')

        # call the method
        ret = self.uitplusjob.generate_pbs_script()

        # test results
        self.assertIn('#PBS -N uit_job', ret)
        self.assertIn('#PBS -A P001', ret)
        self.assertIn('#PBS -l walltime=10:00:42', ret)
        self.assertIn('#PBS -l select=10:ncpus=36:mpiprocs=5', ret)
        self.assertIn('#PBS -J OpenMP', ret)
        self.assertIn('module load test_load', ret)
        self.assertIn('PBSScript', ret)

    @mock.patch('tethysapp.uit_plus_job.models.UitPlusJob.generate_pbs_script')
    @mock.patch('tethysapp.uit_plus_job.models.UitPlusJob.get_client')
    @mock.patch('django.db.models.base.Model.save')
    def test__execute(self, mock_save, mock_client, mock_pbs):
        mock_client_return = mock.MagicMock()
        mock_client.return_value = mock_client_return
        mock_pbs.return_value = 'test_pbs_script'

        # call the method
        self.uitplusjob._execute('test_token')
        mock_client_return.submit.return_value = 'test_job_id'

        # testing the client call input arguments
        call_args = mock_client_return.call.call_args_list
        self.assertIn('mkdir -p test_label/uit_job/', call_args[0][1]['command'])
        self.assertIn('${WORKDIR}', call_args[0][1]['work_dir'])

        # testing the client submit input arguments
        submit_call_args = mock_client_return.submit.call_args_list
        self.assertEqual('test_pbs_script', submit_call_args[0][0][0])
        self.assertIn('${WORKDIR}/test_label/uit_job/', submit_call_args[0][0][1])

        mock_client_return.put_file.assert_called()

        mock_pbs.assert_called()

        mock_save.assert_called()

    @mock.patch('tethysapp.uit_plus_job.models.UitPlusJob.get_client')
    def test_stop(self, mock_client):
        mock_client_return = mock.MagicMock()
        mock_client.return_value = mock_client_return
        # call the method
        self.uitplusjob.stop('test_token')

        # test results
        call_args = mock_client_return.call.call_args_list
        self.assertIn('${WORKDIR}/test_label/uit_job/', call_args[0][1]['work_dir'])
        self.assertIn('qdel J0001', call_args[0][1]['command'])

    @mock.patch('tethysapp.uit_plus_job.models.UitPlusJob.get_client')
    def test_pause(self, mock_client):
        mock_client_return = mock.MagicMock()
        mock_client.return_value = mock_client_return

        # call the method
        self.uitplusjob.pause('test_token')

        # test results
        call_args = mock_client_return.call.call_args_list
        self.assertIn('${WORKDIR}/test_label/uit_job/', call_args[0][1]['work_dir'])
        self.assertIn('qhold J0001', call_args[0][1]['command'])

    @mock.patch('tethysapp.uit_plus_job.models.UitPlusJob.get_client')
    def test_resume(self, mock_client):
        mock_client_return = mock.MagicMock()
        mock_client.return_value = mock_client_return

        # call the method
        self.uitplusjob.resume('test_token')

        # test results
        call_args = mock_client_return.call.call_args_list
        self.assertIn('${WORKDIR}/test_label/uit_job/', call_args[0][1]['work_dir'])
        self.assertIn('qrls J0001', call_args[0][1]['command'])

    @mock.patch('tethysapp.uit_plus_job.models.UitPlusJob.render_clean_block')
    @mock.patch('tethysapp.uit_plus_job.models.UitPlusJob.get_client')
    def test_clean(self, mock_client, mock_pbs):
        mock_client_return = mock.MagicMock()
        mock_client.return_value = mock_client_return
        mock_pbs.return_value = 'test_pbs_render_out'

        self.uitplusjob.clean('test_token', archive=True)

        client_submit_args = mock_client_return.submit.call_args_list

        self.assertEqual('test_pbs_render_out', client_submit_args[0][0][0])
        self.assertIn('${WORKDIR}/test_label/uit_job/', client_submit_args[0][0][1])

    def test_render_clean_block(self):
        # call the method
        res = self.uitplusjob.render_clean_block()

        # test the results
        self.assertIn('cd ${WORKDIR}/test_label/uit_job/', res)
        self.assertIn('#PBS -l walltime=10:00:42', res)
        self.assertIn('#PBS -A P001', res)
        self.assertIn('archive rm ${WORKDIR}/test_label/uit_job/', res)
        self.assertIn('archive rm ${HOME}/test_label/uit_job/', res)

    @mock.patch('tethysapp.uit_plus_job.models.UitPlusJob.get_client')
    def test_process_results(self, mock_client):
        mock_client_return = mock.MagicMock()
        mock_client.return_value = mock_client_return

        self.uitplusjob._process_results(token='token')

        # TODO: Need to finish this. after we can test on the super computer.
