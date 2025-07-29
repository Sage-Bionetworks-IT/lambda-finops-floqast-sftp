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

test_target_date_iso = "2025-03-01"
test_target_date = date.fromisoformat(test_target_date_iso)
test_current_datetime_iso = "2025-04-04T10:10:10Z"
test_current_datetime = datetime.fromisoformat(test_current_datetime_iso)
test_filename = "Sage-Balances-March-2025-20250404101010.csv"

test_csv_data = f"""AccountName,PeriodStart,PeriodEnd,Activity
Test,{test_target_date_iso},{test_target_date_iso},0"""

test_result_body_success = {
    "statusCode": 200,
    "body": json.dumps({"Success": True}),
}


def test_ssm_params(mocker):
    mocker.patch.dict(os.environ, {"AWS_DEFAULT_REGION": "test"})
    app.ssm_client = boto3.client("ssm")
    with Stubber(app.ssm_client) as ssm_client:
        ssm_client.add_response("get_parameters_by_path", stub_ssm_response)

        found = app.get_ssm_params(test_ssm_prefix)
        assert found == test_ssm_values


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
    url = app._csv_url + f"&target_date={test_target_date_iso}"
    requests_mock.get(url, text=test_csv_data)
    mocker.patch("floqast_sftp.app.get_file_name", return_value=test_filename)

    found_filename, found_fileobj = app.get_balances_csv(test_target_date_iso)
    found_csv_data = found_fileobj.read()
    assert found_csv_data == test_csv_data


@pytest.mark.parametrize(
    "event,success",
    [
        (
            {
                "ssm_secret_prefix": test_ssm_prefix,
                "period_count": 1,
            },
            True,
        ),
        (
            {
                "ssm_secret_prefix": test_ssm_prefix,
                "period_count": 3,
            },
            True,
        ),
        (
            {
                "ssm_secret_prefix": test_ssm_prefix,
                "period_count": 0,
            },
            False,
        ),
        (
            {
                "ssm_secret_prefix": test_ssm_prefix,
            },
            False,
        ),
        (
            {
                "period_count": 1,
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

    mock_client = mocker.MagicMock(spec=paramiko.SFTPClient)
    mocker.patch(
        "floqast_sftp.app.get_sftp_client",
        autospec=True,
        return_value=mock_client,
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
        result = app.lambda_handler(event, {})
        assert result == test_result_body_success
    else:
        with pytest.raises(ValueError):
            app.lambda_handler(event, {})
