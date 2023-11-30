import base64
import traceback
import json
import boto3
from io import StringIO
from datetime import datetime, timedelta, timezone
import logging
import sys
import requests


MAX_LAMBDA_CONCURRENCY = 20
MAX_ROW_PER_REQUEST = 5000

USERNAME = "UMP00IT08"
PASSWORD = "Abap@008"

PATH_LOGGER = "fon_test/config-fair-fast/"

# AWS S3
BUCKET_NAME = "mitrphol-sap-mkt-fi"
FOLDER_PATH = "fair_fast_sap_load/{path_name}/json/"
FOLDER_NAME = ["ZCDS_AUFK", "ZCDS_BPEG", "ZCDS_COOI", "ZCDS_COSPD"]

# URL
URL_SCHEME = "https"
URL_AUTHORITY_DOMAINNAME = "mpscdrerap82.mitrphol.com"
URL_AUTHORITY_PORT = "44300"
URL_PATH = "/sap/opu/odata/sap"
URL_SOURCE_LIST = ["ZCDS_AUFK_CDS/ZCDS_AUFK", "ZCDS_BPEG_CDS/ZCDS_BPEG",
                   "ZCDS_COOI_CDS/ZCDS_COOI", "ZCDS_COSPD_CDS/ZCDS_COSPD"]


def main(event=dict(), context=None):

    now_datetime = datetime.now(timezone(timedelta(hours=7)))
    concurrency_number = event["concurrency_number"]
    log_number = event["log_number"]
    log_stream = StringIO()

    logger_name = f"{FOLDER_NAME[3]}/{FOLDER_NAME[3]}_{event['state']}_{concurrency_number:02d}_{log_number:03d}"

    logger = logger_init(logger_name=logger_name,
                         logging_level=logging.INFO, log_stream=log_stream)

    if event["year"] == "none":
        event["year"] = now_datetime.year
        event["month"] = now_datetime.month
        event["day"] = now_datetime.day

    year = event["year"]
    month = event["month"]
    day = event["day"]
    logger.info(f"{year}, {month}, {day}")

    try:
        # Set Up Concurrency
        if event["state"] == 1:
            set_up_concurrency(event=event, context=context, logger=logger)
            logger_str = log_stream.getvalue()
            put_file_to_bucket(content=logger_str, bucket=BUCKET_NAME, folder=PATH_LOGGER,
                               file_name=logger_name, file_extention=".log", logger=logger)
            return

        # Set Up Request
        if event["state"] == 2:
            make_request(event=event, context=context, logger=logger)
            logger_str = log_stream.getvalue()
            put_file_to_bucket(content=logger_str, bucket=BUCKET_NAME, folder=PATH_LOGGER,
                               file_name=logger_name, file_extention=".log", logger=logger)
            return

    except Exception as e:
        logger.error(str(e))
        logger.exception(traceback.format_exc())
        logger_str = log_stream.getvalue()
        put_file_to_bucket(content=logger_str, bucket=BUCKET_NAME, folder=PATH_LOGGER,
                           file_name=logger_name, file_extention=".log", logger=logger)
        return


def logger_init(logger_name='my-app', logging_level=logging.INFO, log_stream=StringIO()):

    def get_console_handler():
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter(
            "%(asctime)s — %(name)s — %(levelname)s — %(message)s"))
        return console_handler

    def get_stringio_handler():
        console_handler = logging.StreamHandler(log_stream)
        console_handler.setFormatter(logging.Formatter(
            "%(asctime)s — %(name)s — %(levelname)s — %(message)s"))
        return console_handler

    def get_logger(logger_name):
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging_level)
        logger.addHandler(get_console_handler())
        logger.addHandler(get_stringio_handler())
        logger.propagate = False
        return logger

    return get_logger(logger_name)


# Set Up Concurrency
def set_up_concurrency(event=dict(), context=None, logger=logger_init()):

    logger.info(f"{URL_SOURCE_LIST[3]} is starting")
    logger.info("Set Up Concerrency")

    event["username"] = USERNAME
    event["password"] = PASSWORD

    # Make Request
    count_url = make_url(url_scheme=URL_SCHEME,
                         url_authority_domain=URL_AUTHORITY_DOMAINNAME,
                         url_authority_port=URL_AUTHORITY_PORT,
                         url_path=f"{URL_PATH}/{URL_SOURCE_LIST[3]}/$count")
    params = dict()
    headers = dict()
    headers = {
        'Authorization': 'Basic ' + base64.b64encode(f"{USERNAME}:{PASSWORD}".encode("ascii")).decode("ascii")
    }

    response = requests.request(
        method="GET", url=count_url, params=params, headers=headers)

    if response.status_code == 200:
        logger.info(f"HTTP Code : {response.status_code}")

        number_of_row = int(response.text)
        logger.info(f"Number of row : {number_of_row}")

        if number_of_row == 0:
            event["state"] = 1
            event["log_number"] = 1
            logger.info("This Is LAST Concerrency")

        # Set number of concurrency
        number_of_use_concurrency = int(number_of_row/MAX_ROW_PER_REQUEST) + 1
        if number_of_use_concurrency > MAX_LAMBDA_CONCURRENCY:
            number_of_use_concurrency = MAX_LAMBDA_CONCURRENCY
        event["number_of_use_concurrency"] = number_of_use_concurrency

        logger.info(f"Number of use concurrency : {number_of_use_concurrency}")

        # Set number of row for each thread
        row_per_thread = int(number_of_row/number_of_use_concurrency)

        # Invoke concurrency
        for t in range(number_of_use_concurrency):
            if t == number_of_use_concurrency - 1:
                last_query_row = (t + 1) * row_per_thread + \
                    (number_of_row % row_per_thread)
                event["is_last_concurrency"] = "True"
            else:
                last_query_row = (t + 1) * row_per_thread
                event["is_last_concurrency"] = "False"

            first_query_row = t * row_per_thread
            number_of_request = int(
                (last_query_row - first_query_row)/MAX_ROW_PER_REQUEST) + 1

            event["concurrency_number"] = t + 1
            event["state"] = 2
            event["first_query_row"] = first_query_row
            event["last_query_row"] = last_query_row
            event["number_of_request"] = number_of_request
            event["request_number"] = 1

            logger.info(f'concurrency number : {event["concurrency_number"]}')
            logger.info(f'state : {event["state"]}')
            logger.info(f'first row : {event["first_query_row"]}')
            logger.info(f'last row: {event["last_query_row"]}')
            logger.info(f'last concurrency : {event["is_last_concurrency"]}')
            logger.info(f'number of request : {event["number_of_request"]}')

            # start thread
            make_recursive_call(event=event, context=context)

        return

    logger.info(f"HTTP Code : {response.status_code}")
    logger.info(response.text)
    return


def make_request(event=dict(), context=None, logger=logger_init()):

    logger.info(f"{FOLDER_NAME[3]} is starting")
    logger.info("make request")

    # year = event["year"]
    # month = event["month"]
    # day = event["day"]

    username = event["username"]
    password = event["password"]

    first_query_row = event["first_query_row"]
    last_query_row = event["last_query_row"]
    is_last_concurrency = event["is_last_concurrency"]
    concurrency_number = event["concurrency_number"]
    number_of_request = event["number_of_request"]
    request_number = event["request_number"]
    file_number = (concurrency_number-1) * number_of_request + request_number

    # Make request
    skip = first_query_row
    top = MAX_ROW_PER_REQUEST
    if last_query_row - first_query_row < MAX_ROW_PER_REQUEST:
        top = last_query_row - skip

    request_url = make_url(url_scheme=URL_SCHEME, url_authority_domain=URL_AUTHORITY_DOMAINNAME,
                           url_authority_port=URL_AUTHORITY_PORT, url_path=f"{URL_PATH}/{URL_SOURCE_LIST[3]}/")
    params = dict()
    params["$skip"] = skip
    params = add_params(params=params, key="$skip", value=skip)
    params = add_params(params=params, key="$top", value=top)
    params = add_params(params=params, key="$format", value="json")
    headers = dict()
    headers = {
        'Authorization': 'Basic ' + base64.b64encode(f"{username}:{password}".encode("ascii")).decode("ascii")
    }

    logger.info(params)

    response = requests.request(
        method="GET", url=request_url, params=params, headers=headers)

    if response.status_code == 200:
        logger.info(f"HTTP Code : {response.status_code}")

        # json file
        put_file_to_bucket(content=response.text, bucket=BUCKET_NAME, folder=FOLDER_PATH.format(path_name=FOLDER_NAME[3]),
                           file_name=f'{FOLDER_NAME[0]}_{file_number:03d}', file_extention='.json', logger=logger)

        # if have more data to query
        if last_query_row - first_query_row > MAX_ROW_PER_REQUEST:
            event["first_query_row"] = first_query_row + top
            event["request_number"] += 1
            event["log_number"] += 1
            make_recursive_call(event=event, context=context)
            return
        return

    logger.info(f"HTTP Code : {response.status_code}")
    logger.info(response.text)
    return


def make_url(url_scheme, url_authority_domain, url_authority_port, url_path):
    return f"{url_scheme}://{url_authority_domain}:{url_authority_port}{url_path}"


def put_file_to_bucket(content="", bucket="", folder="", file_name="", file_extention="", logger=logger_init()):
    s3 = boto3.client('s3')
    logger.info(f"Save to S3 filename : {file_name}{file_extention}")
    s3.put_object(
        Bucket=bucket, Key=f"{folder}{file_name}{file_extention}", Body=content)
    return


def get_file_from_bucket(bucket="", folder="", file_name="", file_extention="", logger=logger_init()):
    s3_client = boto3.client('s3')
    s3_response_object = s3_client.get_object(
        Bucket=bucket, Key=f"{folder}{file_name}{file_extention}")
    s3_object_body = s3_response_object.get('Body')
    return str(s3_object_body.read(), 'utf-8')


def add_params(params=dict(), key="", value=""):
    params[key] = value
    return params


def make_recursive_call(event, context):
    main(event=event, context=context)
    return
    lambda_client = boto3.client('lambda')
    lambda_client.invoke(FunctionName=context.function_name,
                         InvocationType='Event', Payload=json.dumps(event))
    return


def lambda_handler(event, context):
    main(event=event, context=context)
    return {
        'statusCode': 200
    }


if __name__ == '__main__':
    event = dict()
    event["concurrency_number"] = 0
    event["state"] = 1
    event["log_number"] = 1
    event["year"] = "none"
    main(event=event)
