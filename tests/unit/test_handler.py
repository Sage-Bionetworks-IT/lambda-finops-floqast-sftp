import json
import os
from datetime import date, datetime

import paramiko
import paramiko.sftp_client
import paramiko.transport

import pytest
import boto3
from botocore.stub import Stubber

from floqast_sftp import app


test_ssm_prefix = "test-secrets/"
test_ssm_values = {
    "user": "username",
    "pass": "password",
    "host": "example.com",
    "port": 22,
}
stub_ssm_response = {
    "Parameters": [
        {"Name": test_ssm_prefix + k, "Value": str(v)}
        for k, v in test_ssm_values.items()
    ]
}

test_ssm_values_opt = {
    "user": "username",
    "pass": "password",
    "host": "example.com",
}
stub_ssm_response_opt = {
    "Parameters": [
        {"Name": test_ssm_prefix + k, "Value": str(v)}
        for k, v in test_ssm_values_opt.items()
    ]
}

test_ssm_values_missing = {
    "user": "username",
    "pass": "password",
}
stub_ssm_response_missing = {
    "Parameters": [
        {"Name": test_ssm_prefix + k, "Value": str(v)}
        for k, v in test_ssm_values_missing.items()
    ]
}

test_ssm_values_invalid = {
    "user": "username",
    "pass": "password",
    "host": "example.com",
    "port": "invalid",
}
stub_ssm_response_invalid = {
    "Parameters": [
        {"Name": test_ssm_prefix + k, "Value": str(v)}
        for k, v in test_ssm_values_invalid.items()
    ]
}

test_target_date_iso = "2025-03-01"
test_target_date = date.fromisoformat(test_target_date_iso)
test_current_datetime_iso = "2025-04-04T10:10:10Z"
test_current_datetime = datetime.fromisoformat(test_current_datetime_iso)
test_filename = "Sage-Balances-March-2025-20250404101010.csv"

test_csv_base_url = "https://example.com/balances"
test_csv_full_url = f"https://example.com/balances?show_inactive_codes&target_date={test_target_date_iso}"
test_csv_base_url_extra = "https://example.com/balances?foo"
test_csv_full_url_extra = f"https://example.com/balances?foo&show_inactive_codes&target_date={test_target_date_iso}"

test_csv_data = f"""AccountName,PeriodStart,PeriodEnd,Activity
Test,{test_target_date_iso},{test_target_date_iso},0"""


@pytest.mark.parametrize(
    "stub_response,expected",
    (
        (stub_ssm_response, test_ssm_values),
        (stub_ssm_response_opt, test_ssm_values),
    ),
)
def test_ssm_params(mocker, stub_response, expected):
    mocker.patch.dict(os.environ, {"AWS_DEFAULT_REGION": "test"})
    app.ssm_client = boto3.client("ssm")
    with Stubber(app.ssm_client) as ssm_client:
        ssm_client.add_response("get_parameters_by_path", stub_response)

        found = app.get_ssm_params(test_ssm_prefix)
        assert found == expected


def test_ssm_params_missing(mocker):
    mocker.patch.dict(os.environ, {"AWS_DEFAULT_REGION": "test"})
    app.ssm_client = boto3.client("ssm")
    with Stubber(app.ssm_client) as ssm_client:
        ssm_client.add_response("get_parameters_by_path", stub_ssm_response_missing)

        with pytest.raises(KeyError):
            app.get_ssm_params(test_ssm_prefix)


def test_ssm_params_invalid(mocker):
    mocker.patch.dict(os.environ, {"AWS_DEFAULT_REGION": "test"})
    app.ssm_client = boto3.client("ssm")
    with Stubber(app.ssm_client) as ssm_client:
        ssm_client.add_response("get_parameters_by_path", stub_ssm_response_invalid)

        with pytest.raises(ValueError):
            app.get_ssm_params(test_ssm_prefix)


def test_file_name(mocker):
    mock_datetime = mocker.patch("floqast_sftp.app.datetime")
    mock_datetime.now.return_value = test_current_datetime

    found = app.get_file_name(test_target_date_iso)
    assert found == test_filename


@pytest.mark.parametrize(
    "today,previous",
    [
        (
            date(2025, 4, 4),
            date(2025, 3, 4),
        ),
        (
            date(2025, 1, 4),
            date(2024, 12, 4),
        ),
    ],
)
def test_previous_month(today, previous):
    found = app.get_previous_month(today)
    assert found == previous


def test_balances_csv(mocker, requests_mock):
    requests_mock.get(test_csv_full_url, text=test_csv_data)
    mocker.patch("floqast_sftp.app.get_file_name", return_value=test_filename)

    found_filename, found_fileobj = app.get_balances_csv(test_csv_full_url)
    found_csv_data = found_fileobj.read()
    assert found_csv_data == test_csv_data


@pytest.mark.parametrize(
    "base_url,expected",
    [
        (test_csv_base_url, test_csv_full_url),
        (test_csv_base_url_extra, test_csv_full_url_extra),
    ],
)
def test_csv_url(mocker, base_url, expected):
    mocker.patch("floqast_sftp.app.get_event_param", return_value=base_url)
    found = app.get_csv_url({}, test_target_date_iso)
    assert found == expected


@pytest.mark.parametrize(
    "event,success",
    [
        (
            {
                "ssm_secret_prefix": test_ssm_prefix,
                "period_count": 1,
                "mip_api_balances_url": test_csv_base_url,
            },
            True,
        ),
        (
            {
                "ssm_secret_prefix": test_ssm_prefix,
                "period_count": "1",
                "mip_api_balances_url": test_csv_base_url,
            },
            True,
        ),
        (
            {
                "ssm_secret_prefix": test_ssm_prefix,
                "period_count": "3",
                "mip_api_balances_url": test_csv_base_url,
            },
            True,
        ),
        (
            {
                "ssm_secret_prefix": test_ssm_prefix,
                "period_count": "0",
                "mip_api_balances_url": test_csv_base_url,
            },
            False,
        ),
        (
            {
                "ssm_secret_prefix": test_ssm_prefix,
                "period_count": "invalid",
                "mip_api_balances_url": test_csv_base_url,
            },
            False,
        ),
        (
            {
                "ssm_secret_prefix": test_ssm_prefix,
                "period_count": "1",
            },
            False,
        ),
        (
            {
                "ssm_secret_prefix": test_ssm_prefix,
                "mip_api_balances_url": test_csv_base_url,
            },
            False,
        ),
        (
            {
                "period_count": "1",
                "mip_api_balances_url": test_csv_base_url,
            },
            False,
        ),
    ],
)
def test_handler(mocker, event, success):
    mocker.patch.dict(os.environ, {"AWS_DEFAULT_REGION": "test"})

    mocker.patch(
        "floqast_sftp.app.get_ssm_params",
        autospec=True,
        return_value=test_ssm_values,
    )

    mock_transport = mocker.MagicMock(spec=paramiko.Transport)
    mock_client = mocker.MagicMock(spec=paramiko.SFTPClient)
    mocker.patch(
        "floqast_sftp.app.get_sftp_client",
        autospec=True,
        return_value=(mock_transport, mock_client),
    )

    date_mock = mocker.patch("floqast_sftp.app.date")
    date_mock.today.return_value = test_target_date

    mocker.patch(
        "floqast_sftp.app.get_balances_csv", autospec=True, return_value=("", None)
    )

    mocker.patch(
        "floqast_sftp.app.put_sftp_file",
        autospec=True,
    )

    mocker.patch(
        "floqast_sftp.app.get_previous_month",
        autospec=True,
        return_value=test_target_date,
    )

    if success:
        app.lambda_handler(event, {})
    else:
        with pytest.raises(ValueError):
            app.lambda_handler(event, {})
