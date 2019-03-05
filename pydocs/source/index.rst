***********************
UIT+ Job Type |version|
***********************


Tethys job type for DoD HPCMP UIT+ rest interface

Contents
========

.. toctree::
   :maxdepth: 1

   modules


Example Usage
=============

.. code-block:: python
   :caption: Example.py
   :name: example-py
   :linenos:

   def run_job(request):
      """
      Controller that creates and submits a UIT job.
      """
      # Get Needed App Settings
      project_id = app.get_custom_setting('project_id')

      # Get Paths to Files
      app_workspace = app.get_app_workspace()
      test_job_in = os.path.join(app_workspace.path, 'test_job.in')

      # Job workspace
      job_id = uuid.uuid4()
      job_workspace = os.path.join(app_workspace.path, 'jobs', str(job_id))

      uit_plus_tutorial_dir = os.path.dirname(__file__)
      job_script = os.path.join(uit_plus_tutorial_dir, 'job_scripts', 'job_script.py')

      # Get Job Manager
      job_manager = app.get_job_manager()

      job = job_manager.create_job(
	 name='TestUitJob-{}'.format(job_id),
	 user=request.user,
	 job_type=UitPlusJob,
	 project_id=project_id,
	 system='topaz',
	 node_type='compute',
	 num_nodes=1,
	 processes_per_node=1,
	 queue='debug',
	 workspace=job_workspace,
	 max_time=dt.timedelta(minutes=5),
	 max_cleanup_time=dt.timedelta(minutes=5),
	 job_script=job_script,
	 home_input_files=['getMe.home'],
	 home_output_files=['test_job.out'],
	 archive_input_files=['getMe.archive'],
	 archive_output_files=['test_job.out'],
	 transfer_input_files=[test_job_in, ],
	 transfer_intermediate_files=['interim.out'],
	 transfer_output_files=['test_job.out', 'getMe.home', 'getMe.archive', 'nonexistant.file']
      )
      try:
	 job.execute()
      except RuntimeError as e:
	 messages.add_message(request, messages.ERROR, 'Failed to Run Job: {}'.format(e))
	 job.delete()
	 return redirect(reverse('uit_plus_tutorial:home'))

      return redirect(reverse('uit_plus_tutorial:status'))


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
