"""
********************************************************************************
* Name: test_oauth2.py
* Author: nswain
* Created On: November 12, 2018
* Copyright: (c) Aquaveo 2018
********************************************************************************
"""

import unittest
from unittest import mock
from uit_plus_job.oauth2 import UitPlusOAuth2


class UitPlusOAuth2Tests(unittest.TestCase):

    def setUp(self):
        self.auth = UitPlusOAuth2(strategy=mock.MagicMock())

    def tearDown(self):
        pass

    def test_attributes(self):
        self.assertEqual("UITPlus", self.auth.name)
        self.assertEqual("https", self.auth.http_scheme)
        self.assertEqual("www.uitplus.hpc.mil", self.auth.auth_server_hostname)
        self.assertIn("/uapi/authorize", self.auth.AUTHORIZATION_URL)
        self.assertIn("/uapi/token", self.auth.ACCESS_TOKEN_URL)
        self.assertIn("/uapi/userinfo", self.auth.USER_DATA_URL)
        self.assertEqual("POST", self.auth.ACCESS_TOKEN_METHOD)
        self.assertEqual("POST", self.auth.REFRESH_TOKEN_METHOD)
        self.assertIn("UIT", self.auth.DEFAULT_SCOPE)
        self.assertEqual("USERNAME", self.auth.ID_KEY)
        self.assertIn(("USERNAME", "email"), self.auth.EXTRA_DATA)
        self.assertIn(("USERNAME", "id"), self.auth.EXTRA_DATA)
        self.assertIn(("SYSTEMS", "systems"), self.auth.EXTRA_DATA)
        self.assertIn(("access_token_expires_on", "expires_in"), self.auth.EXTRA_DATA)
        self.assertIn(("refresh_token", "refresh_token"), self.auth.EXTRA_DATA)
        self.assertIn(
            ("refresh_token_expires_on", "refresh_expires_in"), self.auth.EXTRA_DATA
        )

    def test_get_user_details_with_hpc_username(self):
        hpc_username = "foo@bar.com"
        mock_response = {"USERNAME": hpc_username}
        ret = self.auth.get_user_details(mock_response)
        self.assertIn("username", ret)
        self.assertIn("email", ret)
        self.assertEqual("foo", ret["username"])
        self.assertEqual(hpc_username, ret["email"])

    def test_get_user_details_no_hpc_username(self):
        mock_response = {}
        ret = self.auth.get_user_details(mock_response)
        self.assertEqual({}, ret)

    def test_user_data(self):
        userinfo = {"USERNAME": "fake@mail.com"}
        mock_token = "123abc-456def-1a2b3c4d5e6f-ab12"
        mock_response = {"foo": "bar"}
        self.auth.get_json = mock.MagicMock()
        self.auth.get_json.return_value = {"userinfo": userinfo}

        ret = self.auth.user_data(mock_token, response=mock_response)

        self.auth.get_json.assert_called_with(
            self.auth.USER_DATA_URL, headers={"x-uit-auth-token": mock_token}
        )
        self.assertEqual(2, len(ret))
        self.assertIn("USERNAME", ret)
        self.assertEqual("fake@mail.com", ret["USERNAME"])
        self.assertIn("foo", ret)
        self.assertEqual("bar", ret["foo"])

    def test_user_data_no_response(self):
        userinfo = {"USERNAME": "fake@mail.com"}
        mock_token = "123abc-456def-1a2b3c4d5e6f-ab12"
        self.auth.get_json = mock.MagicMock()
        self.auth.get_json.return_value = {"userinfo": userinfo}

        ret = self.auth.user_data(mock_token)

        self.auth.get_json.assert_called_with(
            self.auth.USER_DATA_URL, headers={"x-uit-auth-token": mock_token}
        )
        self.assertEqual(1, len(ret))
        self.assertIn("USERNAME", ret)
        self.assertEqual("fake@mail.com", ret["USERNAME"])

    def test_user_data_no_userinfo(self):
        mock_token = "123abc-456def-1a2b3c4d5e6f-ab12"
        mock_response = {"foo": "bar"}
        self.auth.get_json = mock.MagicMock()
        self.auth.get_json.return_value = {"goo": "jar"}

        ret = self.auth.user_data(mock_token, response=mock_response)

        self.auth.get_json.assert_called_with(
            self.auth.USER_DATA_URL, headers={"x-uit-auth-token": mock_token}
        )
        self.assertEqual(1, len(ret))
        self.assertIn("foo", ret)
        self.assertEqual("bar", ret["foo"])
        self.assertNotIn("goo", ret)

    def test_user_data_exception(self):
        mock_token = "123abc-456def-1a2b3c4d5e6f-ab12"
        mock_response = {"foo": "bar"}
        self.auth.get_json = mock.MagicMock()
        self.auth.get_json.side_effect = Exception

        ret = self.auth.user_data(mock_token, response=mock_response)

        self.auth.get_json.assert_called_with(
            self.auth.USER_DATA_URL, headers={"x-uit-auth-token": mock_token}
        )
        self.assertEqual(mock_response, ret)
