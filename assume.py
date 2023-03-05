"""
change roles and assume druid
"""
import boto3
from dms_admin import start_rep_task


sts_session = boto3.Session()
sts_client = sts_session.client('sts')
assumed_role_object = sts_client.assume_role(
    RoleArn="arn:aws:iam::492436075634:role/druid-admin-role-Role-SET2TUJYEJZT",
    RoleSessionName="druid-dev-session2"
)
print(assumed_role_object)
access_key = assumed_role_object['Credentials']['AccessKeyId']
secret_key = assumed_role_object['Credentials']['SecretAccessKey']
token_key = assumed_role_object['Credentials']['SessionToken']

session = boto3.Session(aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key, aws_session_token=token_key)

print(start_rep_task('arn:aws:dms:us-west-2:492436075634:task:FZH6TMWHSD42XA24YEMK4O24ELKNYDIS3SWO4HI',
                         'reload-target', None))

