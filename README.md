# UIT Plus Extension for Tethys

## Installation

1. Install the extension.
    
    ```bash
    cd django-uit_plus_job
    python setup.py install
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
    tethys manage syncdb
    ```

1. Install DOD Certificates:

    ```bash
    Option 1. Install certs in system/with Conda and set environment variable to tell Python requests module to use the system certs instead of the built-in certs file: 
       export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt.
    
    Option 2. Append DOD certs to site-packages/certifi/cacert.pem
    
    Option 3. Use the cert conda package that Kevin is building (preferred)
    ```

## Tests

```bash
. test.sh <path_to_tethys_manage.py>
```
