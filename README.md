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

1. Register a new client on https://www.uitplus.hpc.mil. Set `Return URL` to `http(s)://<yourtethysportal>/oauth2/complete/UITPlus/`.

  Note the generated client id and client secret to add to the Tethys configuration file.
    
1. Add the following to the `portal_config.yml` file:

  ```yaml
  INSTALLED_APPS:
    - uit_plus_job
  
  AUTHENTICATION_BACKENDS:
    - uit_plus_job.oauth2.UitPlusOAuth2
  
  OAUTH_CONFIGS:
    SOCIAL_AUTH_UITPLUS_KEY: <client-id>
    SOCIAL_AUTH_UITPLUS_SECRET: <client-secret>
  ```

1. Run migrations:

    ```bash
    tethys db migrate
    ```

## Tests

```bash
. test.sh <path_to_tethys_manage.py>
```
