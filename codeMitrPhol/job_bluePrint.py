from sagemaker.processing import Processor, ScriptProcessor, ProcessingInput, ProcessingOutput
import random
import boto3
from datetime import date, datetime, timedelta

from sagemaker import get_execution_role

today = date.today()
year = str(today.year)
month = str(today.month)

print(year, month)
role = get_execution_role()
print(role)

s3_client = boto3.client('s3')

s3_client.upload_file(
    # buffer file on sagemaker
    Filename='/root/FarmFocus/Project_FarmFocus_ProcessRawRaster/code/dev_adjust2band.py',
    Bucket='mitrphol-farmfocus-test-553463144000',
    Key=f'test-processingjob/thanutj/script_dev_blueprint.py'
)

script_processor = ScriptProcessor(
    command=['python'],
    image_uri='553463144000.dkr.ecr.ap-southeast-1.amazonaws.com/mitrphol-ecr-farmfocus-geo-prod:latest',
    role='arn:aws:iam::553463144000:role/mitrphol-aimlprojects-roles',
    instance_count=1,
    instance_type='ml.m5.12xlarge',
    volume_size_in_gb=100,
    max_runtime_in_seconds=172800,
    tags=[
        {"Key": "ou",        "Value": "digit-a"},
        {"Key": "Project",   "Value": "farmfocus"},
        {"Key": "createdBy", "Value": "thanutj"},
        {"Key": "productOwner", "Value": "athikank"},
        {"Key": "env", "Value": "test"},

    ]

)

rand_hash = str("%08x" % random.getrandbits(32))
script_processor.run(
    code='s3://mitrphol-farmfocus-test-553463144000/test-processingjob/thanutj/script_dev_blueprint.py',
    job_name=f"mitrphol-farmfocus-bluprint-test-{rand_hash}",
    # arguments=[
    #     "-year", year,
    #     "-month", month
    # ],
    wait=False,
    logs=False,
)
