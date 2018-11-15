from tethys_sdk.base import TethysAppBase, url_map_maker


class UitPlusJob(TethysAppBase):
    """
    Tethys app class for Uit Plus Job.
    """

    name = 'Uit Plus Job'
    index = 'uit_plus_job:home'
    icon = 'uit_plus_job/images/icon.gif'
    package = 'uit_plus_job'
    root_url = 'uit-plus-job'
    color = '#c0392b'
    description = 'Place a brief description of your app here.'
    tags = ''
    enable_feedback = False
    feedback_emails = []

    def url_maps(self):
        """
        Add controllers
        """
        UrlMap = url_map_maker(self.root_url)

        url_maps = (
            UrlMap(
                name='home',
                url='uit-plus-job',
                controller='uit_plus_job.controllers.home'
            ),
        )

        return url_maps
