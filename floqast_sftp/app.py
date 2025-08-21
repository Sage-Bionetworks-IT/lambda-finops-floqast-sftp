import csv
import io
import logging
from datetime import date, datetime

import boto3
import paramiko

import requests

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)

ssm_client = None


def get_event_param(event, param):
    """
    Get the value of a query-string parameter from the EventBridge event.

    Parameters
    ----------
    event: dict
        EventBridge object passed to lambda handler

    Returns
    -------
    str
       The value of the specified query-string parameter

    """
    if param not in event:
        raise ValueError(f"{param} not provided")

    return event[param]


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
        _key = p["Name"].split("/")[-1]
        LOG.debug(f"Found SSM parameter {_key}")
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
    return transport, client


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


def get_period_count(event):
    """
    Parse the period count from the EventBridge event dictionary, ensuring
    the value is an integer.

    Parameters
    ----------
    event: dict
        Event dictionary passed to lambda_handler

    Returns
    -------
    int
        Number of periods requested.

    """

    count_str = get_event_param(event, "period_count")
    try:
        period_count = int(count_str)
    except (TypeError, ValueError) as exc:
        raise ValueError("'period_count' must be an integer.") from exc

    if period_count < 1:
        raise ValueError("'period_count' must be greater than 0")

    return period_count


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


def get_csv_url(event, when):
    """
    Build the query-string parameters for the lambda-mips-api balances endpoint
    from the EventBridge event.

    Parameters
    ----------
    event: dict
        EventBridge object passed to lambda handler

    when: str
        ISO-8601 formatted string specifying a date (YYYY-MM-DD).

    Returns
    -------
    str
        URL to lambda-mips-api balances csv endpoint.

    """

    base_url = get_event_param(event, "mip_api_balances_url")
    url_add = f"show_inactive_codes&target_date={when}"
    if "?" in base_url:
        full_url = f"{base_url}&{url_add}"
    else:
        full_url = f"{base_url}?{url_add}"
    LOG.info(f"Getting balances from {full_url}")
    return full_url


def get_balances_csv(url):
    """
    Fetch the balances CSV from lambda-mips-api for the given activity period.

    Parameters
    ----------
    url: str
        URL to lambda-mips-api balances endpoint

    Returns
    -------
    (str, file)
        Tuple of file name and a file-like object.

    """
    # get url and create file object from response
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
    LOG.info(f"Uploading {name} to SFTP server")
    try:
        client.putfo(fl=file_obj, remotepath=name, confirm=True)
    except Exception as exc:
        LOG.error(f"Failed to upload file '{name}'")
        raise exc


def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        The EventBridge event payload containing the following required keys:
        - ssm_secret_prefix: str
            The prefix for fetching secrets from AWS Systems Manager Parameter Store.
        - period_count: int
            The number of months to retrieve balances for. Must be greater than 0.

    context: object, required
        Lambda Context runtime methods and attributes.
        See AWS Lambda documentation for details:
        https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    -------
    None
    """

    # number of months to get balances for
    period_count = get_period_count(event)
    LOG.info(f"Getting balances for past {period_count} months")

    # get secrets from SSM and authenticate
    ssm_prefix = get_event_param(event, "ssm_secret_prefix")
    auth = get_ssm_params(ssm_prefix)
    LOG.info(f"Logging in to SFTP server")
    transport, client = get_sftp_client(auth)

    # Start with today, and go back N months
    try:
        period = date.today()
        for _ in range(period_count):
            when = period.isoformat()
            url = get_csv_url(event, when)
            name, file_obj = get_balances_csv(url)
            put_sftp_file(client, name, file_obj)
            period = get_previous_month(period)
    finally:
        # Always close the SFTP session and transport
        client.close()
        transport.close()

    LOG.info(f"File uploads complete")
