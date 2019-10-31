# UIT Plus Extension for Tethys

## Installation

1. Conda install `pyuit`.

  ```bash
  conda install -c erdc/label/dev -c conda-forge pyuit
  ```
  
1. Install the extension.
    
    ```bash
    cd django-uit_plus_job
    python pip install -e .
    ```
    
1. Add `"uit_plus_job"` to `INSTALLED_APPS` in `settings.py`.

1. Add ``UitPlusOAuth2`` backend to ``AUTHENTICATION_BACKENDS`` in ``settings.py``. Order matters! Be sure to add it before the ``ModelBackend``:
    
    ```python
    AUTHENTICATION_BACKENDS = (
        'uit_plus_job.oauth2.UitPlusOAuth2',
        'django.contrib.auth.backends.ModelBackend',
        'guardian.backends.ObjectPermissionBackend',
    )
    ```
    
1. Register a new client on https://www.uitplus.hpc.mil. Set `Return URL` to `http(s)://<yourtethysportal>/oauth2/complete/UITPlus/`.

1. Add the generated client id and client secret to ``settings.py``:

    ```python
    SOCIAL_AUTH_UITPLUS_KEY = '<client-id>'
    SOCIAL_AUTH_UITPLUS_SECRET = '<client-secret>'
    ```

1. Run migrations:

    ```bash
    tethys db migrate
    ```

## Tests

```bash
. test.sh <path_to_tethys_manage.py>
```
