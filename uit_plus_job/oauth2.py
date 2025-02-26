"""
********************************************************************************
* Name: uit_plus.py
* Author: nswain
* Created On: November 09, 2018
* Copyright: (c) Aquaveo 2018
********************************************************************************
"""

from datetime import datetime, UTC, timedelta

from social_core.backends.oauth import BaseOAuth2
from uit.uit import DEFAULT_CA_FILE


class UitPlusOAuth2(BaseOAuth2):
    """
    UIT+ OAuth2 authentication backend.
    """

    # backend name
    name = "UITPlus"

    http_scheme = "https"
    auth_server_hostname = "www.uitplus.hpc.mil"
    auth_server_full_url = "{}://{}".format(http_scheme, auth_server_hostname)  # noqa: E222
    AUTHORIZATION_URL = "{}/uapi/authorize".format(auth_server_full_url)  # noqa: E222
    ACCESS_TOKEN_URL = "{}/uapi/token".format(auth_server_full_url)  # noqa: E222
    USER_DATA_URL = "{}/uapi/userinfo".format(auth_server_full_url)  # noqa: E222

    ACCESS_TOKEN_METHOD = "POST"
    REFRESH_TOKEN_METHOD = "POST"
    DEFAULT_SCOPE = ["UIT"]
    ID_KEY = "USERNAME"

    EXTRA_DATA = [
        ("USERNAME", "email"),
        ("USERNAME", "id"),
        ("SYSTEMS", "systems"),
        ("expires_on", "expires_on"),
        ("expires_in", "expires_in"),
        ("expires_in", "expires"),
        ("refresh_token", "refresh_token"),
        ("refresh_token_expires_on", "refresh_token_expires_on"),
        ("access_token_expires_on", "access_token_expires_on"),
    ]

    def extra_data(self, user, uid, response, details=None, *args, **kwargs):
        # convert date string to timestamp
        expires_time = datetime.fromisoformat(response["access_token_expires_on"])
        auth_time = datetime.now(UTC)
        response["expires_on"] = datetime.timestamp(expires_time - timedelta(days=2))
        response["expires_in"] = (expires_time - auth_time).seconds
        return super().extra_data(user, uid, response, details, *args, **kwargs)

    def get_user_details(self, response):
        """
        Extract HPC account details from the given API response.
        """
        # Build user details from HPC username
        hpc_username = response.get("USERNAME", None)

        if hpc_username:
            return {
                "username": hpc_username.split("@")[0],
                "email": hpc_username,
            }
        else:
            return {}

    def user_data(self, access_token, *args, **kwargs):
        """
        Map user data from service to appropriate user attributes.
        """
        # Data returned by the get token call
        response = kwargs.get("response", {})

        try:
            # Get the user data from user data endpoint
            user_data = self.get_json(self.USER_DATA_URL, headers={"x-uit-auth-token": access_token})

            # Pull out the user info
            user_data = user_data.get("userinfo", {})

            # Add user data to the get token response
            user_data.update(response)
            return user_data

        except Exception:
            # Return the get token response if errors occur
            return response

    def request(self, url, method="GET", *args, **kwargs):  # noqa: B026
        return super().request(url, method, *args, verify=DEFAULT_CA_FILE, **kwargs)
