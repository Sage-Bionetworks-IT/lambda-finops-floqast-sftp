import csv
import io
import json
import logging
from datetime import date, datetime

import boto3
import paramiko

import requests


ssm_client = None

_csv_url = "https://mips-api.finops.sageit.org/balances?show_inactive_codes"


def get_ssm_params(prefix):
    """
    Get secure parameters from SSM

    Parameters
    ----------
    prefix: str
        Prefix for the SSM parameter paths.

    Returns
    -------
    dict
        Dictionary of SSM parameter names mapped to their values.

    """

    global ssm_client
    if ssm_client is None:
        ssm_client = boto3.client("ssm")

    response = ssm_client.get_parameters_by_path(
        Path=prefix,
        Recursive=True,
        WithDecryption=True,
    )

    params = {}
    for p in response["Parameters"]:
        # strip prefix from key
        _key = p["Name"][len(prefix) :]
        params[_key] = p["Value"]

    # check for required strings
    for _key in ["user", "pass", "host"]:
        if _key not in params:
            raise KeyError(f"Key '{_key}' not found in SSM.")

    # check for optional integer
    _key = "port"
    if _key in params:
        try:
            params[_key] = int(params[_key])
        except ValueError as exc:
            raise ValueError(f"Parameter '{_key}' is not an integer.") from exc
    else:
        # set a default value
        params[_key] = 22

    return params


def get_sftp_client(auth):
    """
    Create an SFTP client.

    Parameters
    ----------
    auth: dict
        Dictionary providing authentication details:
        'user', 'pass', 'host', 'port'.

    Returns
    -------
    paramiko.SFTPClient

    """
    # From https://medium.com/@geeky_vm/event-based-sftp-using-aws-lambda-python-66c092f41dd9
    transport = paramiko.Transport((auth["host"], auth["port"]))
    transport.connect(username=auth["user"], password=auth["pass"])
    client = paramiko.SFTPClient.from_transport(transport)
    return client


def get_file_name(_period):
    """
    Construct a file name for the given period.

    Parameters
    ----------
    _period: str
        ISO-8601 formatted string specifying a date (YYYY-MM-DD).

    Returns
    -------
    str
        File name.
    """
    # use full month name
    _date = date.fromisoformat(_period)
    _period_month = _date.strftime("%B-%Y")

    # Include creation timestamp in file name
    _timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    name = f"Sage-Balances-{_period_month}-{_timestamp}.csv"
    return name


def get_previous_month(_date):
    """
    Calculate the date one month previous to the given date.

    Parameters
    ----------
    _date: datetime.date
        Date to calculate previous month for.

    Returns
    -------
    datetime.date
        Date one month previous to the given date.

    """
    if _date.month == 1:
        _year = _date.year - 1
        return _date.replace(year=_year, month=12)

    _month = _date.month - 1
    return _date.replace(month=_month)


def get_balances_csv(when):
    """
    Fetch the balances CSV from lambda-mips-api for the given activity period.

    Parameters
    ----------
    when: datetime.date
        Date to pass to lambda-mips-api for calculating the balance activity period.

    Returns
    -------
    (str, file)
        Tuple of file name and a file-like object.

    """
    url = _csv_url + f"&target_date={when}"

    response = requests.get(url, stream=True)
    response.raise_for_status()
    file_obj = io.StringIO(response.text)

    # read target period from CSV
    csv_dict = csv.DictReader(file_obj)
    row = next(csv_dict)
    period = row["PeriodStart"]
    # and reset file position
    file_obj.seek(0)

    filename = get_file_name(period)

    return filename, file_obj


def put_sftp_file(client, name, file_obj):
    """
    Wrapper around SFTPClient.putfo

    Parameters
    ----------
    client: paramiko.SFTPClient
        SFTP client.

    name: str
        Remote file name

    file_obj
        File-like object to upload.

    Returns
    -------
    None

    """
    client.putfo(fl=file_obj, remotepath=name)


def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    # get secrets from SSM
    if "ssm_secret_prefix" not in event:
        raise ValueError("'ssm_secret_prefix' not provided")
    ssm_prefix = event["ssm_secret_prefix"]
    auth = get_ssm_params(ssm_prefix)
    client = get_sftp_client(auth)

    # number of months to get balances for
    if "period_count" not in event:
        raise ValueError("'period_count' not provided")
    period_count = event["period_count"]
    if period_count < 1:
        raise ValueError("'period_count' must be greater than 0")

    # Start with today, and go back N months
    period = date.today()
    for _ in range(period_count):
        when = period.isoformat()
        name, file_obj = get_balances_csv(when)
        put_sftp_file(client, name, file_obj)
        period = get_previous_month(period)

    #
    return {
        "statusCode": 200,
        "body": json.dumps({"Success": True}),
    }
