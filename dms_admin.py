"""
DMS create instance, endpoints, migration task.
Instance types dms.c5.xlarge but dms.c4.large seems large enough
This is specific for performance migration from master to casemanager
Recommend use wizard to get mapping and settings then add json files here.
UI creation starts the task automatic. But we have more control here and can pick our time
"""
# import datetime
# import pprint
import boto3
import json

session = boto3.session.Session()
ssm_client = boto3.client('ssm')
stage = 'dev'

table_map = {
  "rules": [
    {
      "rule-type": "transformation",
      "rule-id": "109398514",
      "rule-name": "109398514",
      "rule-target": "table",
      "object-locator": {
        "schema-name": "master_db_auto",
        "table-name": "tbl_auto_consortium"
      },
      "rule-action": "rename",
      "value": "afm_tbl_auto_consortium_cms",
      "old-value": None
    },
    {
      "rule-type": "transformation",
      "rule-id": "109257422",
      "rule-name": "109257422",
      "rule-target": "schema",
      "object-locator": {
        "schema-name": "master_db_auto"
      },
      "rule-action": "rename",
      "value": "casemanager",
      "old-value": None
    },
    {
      "rule-type": "selection",
      "rule-id": "109130893",
      "rule-name": "109130893",
      "object-locator": {
        "schema-name": "master_db_auto",
        "table-name": "tbl_auto_consortium"
      },
      "rule-action": "include",
      "filters": []
    }
  ]
}

with open('dms_settings.json', 'r') as f:
    settings = json.load(f)


def get_parameter(client, name, with_decryption=True):
    response = client.get_parameter(Name=name, WithDecryption=with_decryption)
    return response['Parameter']['Value']


passwd = get_parameter(ssm_client, f'/apps/aurora_postgres_{stage}/password', with_decryption=True)
host = get_parameter(ssm_client, f'/apps/aurora_postgres_{stage}/hostname', with_decryption=True)


def create_replication(repname):
    client = session.client('dms')
    response = client.create_replication_instance(
        ReplicationInstanceIdentifier=repname,
        AllocatedStorage=50,
        ReplicationInstanceClass='dms.c4.large',
        VpcSecurityGroupIds=[
            'sg-bcc371c6',
            'sg-98dcc7fa'
        ],
        AvailabilityZone='us-west-2a',
        ReplicationSubnetGroupIdentifier='default-vpc-1b322279',
        MultiAZ=False,
        EngineVersion='3.4.6',
        AutoMinorVersionUpgrade=True,
        Tags=[
            {
                'Key': 'cms',
                'Value': 'cms-performance',
                'ResourceArn': 'cms-performance-tool'
            },
        ],

        PubliclyAccessible=False,
    )
    return response


def delete_replication(name):
    client = session.client('dms')
    response = client.delete_replication_instance(
        ReplicationInstanceArn=name
    )
    return response


def describe_endpoint():
    client = session.client('dms')
    response = client.describe_endpoints()
    return response


def del_endpoint(name_arn):
    client = session.client('dms')
    response = client.delete_endpoint(
        EndpointArn=name_arn
    )
    return response


def create_endpoint(name, name_type, server_name, password):
    """
    usage:
        endpoint_name = "cms-dev-master-tbl-auto-consortium-cms"
        print(create_endpoint(endpoint_name, 'source', host, passwd))
        print(create_endpoint(endpoint_name, 'target', host, passwd))
    :param name: str() of identifier
    :param name_type: str() of source or target
    :param server_name: str() of server_name
    :param password: str() of password
    :return:
    """
    client = session.client('dms')
    response = client.create_endpoint(
        EndpointIdentifier=f'{name}-{name_type}',
        EndpointType=name_type,
        EngineName='aurora-postgresql',
        Username='postgres',
        Password=password,
        ServerName=server_name,
        Port=5432,
        DatabaseName='postgres',
        ExtraConnectionAttributes='pluginName=test_decoding; wal_sender_timeout=0;heartbeatenable=Y;'
                                  'heartbeatFrequency=1;heartbeatSchema=public;connectionTimeout=120;'
                                  'Executetimeout=600;',
        PostgreSQLSettings={'DatabaseName': 'postgres',
                            'Port': 5432,
                            'ServerName': server_name,
                            'Username': 'postgres',
                            'Password': password},
        SslMode='none'
    )
    return response


def create_rep_task(src, dst, replication, env):
    """
    Note: we should pass in the ARN values rather than hard code
    CdcStartTime=datetime.datetime(2015, 1, 1),
    CdcStartPosition='string',
    CdcStopPosition='string',
    :return:
    """
    client = session.client('dms')
    response = client.create_replication_task(
        ReplicationTaskIdentifier=f'cms-performance-tbl-auto-consortium-{env}',
        SourceEndpointArn=src,
        TargetEndpointArn=dst,
        ReplicationInstanceArn=replication,
        MigrationType='full-load',
        TableMappings=json.dumps(table_map),
        ReplicationTaskSettings=json.dumps(settings),
        Tags=[
            {
                'Key': 'cms',
                'Value': f'cms-{env}-task',
                'ResourceArn': 'cms-task-tool'
            },
        ],
    )
    return response


def describe_rep_tasks():
    client = session.client('dms')
    response = client.describe_replication_tasks()
    return response


def del_rep_task(name_arn):
    client = session.client('dms')
    response = client.delete_replication_task(ReplicationTaskArn=name_arn)
    return response


def start_rep_task(name_arn, start_type, cdc_start):
    """
    datetime.datetime(2023, 1, 1),  # YY,MM,DD,HH,min,ss
    possible future integration: CdcStartPosition='string',
                                 CdcStopPosition='string'
    :param name_arn: str() of arn
    :param start_type: str() of start-replication, resume-processing, reload-target
    :param cdc_start:
    :return:
    """
    client = session.client('dms')
    response = client.start_replication_task(
        ReplicationTaskArn=name_arn,
        StartReplicationTaskType=start_type,
        CdcStartTime=cdc_start,
    )
    return response


def main(hostname, password, endpoint_name):
    #  print(create_replication('cmsperformance'))  # instance already installed
    print(create_endpoint(endpoint_name, 'source', hostname, password))
    print(create_endpoint(endpoint_name, 'target', hostname, password))


if __name__ == "__main__":
    stage = 'prod'
    endpoint = f"cms-{stage}-master-tbl-auto-consortium-cms"
    rep = 'arn:aws:dms:us-west-2:492436075634:rep:PT3HGTVKFCYFFHAVYITCNIHJ32NURVNXMVK3VNA'
    # main(host, passwd, endpoint)
    # ### dev
    src_end_dev = 'arn:aws:dms:us-west-2:492436075634:endpoint:T6BW4BOKHNKF44YFBYO536V7CMB6CJHRN3WDQDQ'
    dst_end_dev = 'arn:aws:dms:us-west-2:492436075634:endpoint:53S254XSELPT7ENAQM6MGO7KDW6DSEEZHSHEX6Q'
    print(create_rep_task(src_end_dev, dst_end_dev, rep, 'dev'))
    # ### stage
    src_end_stage = 'arn:aws:dms:us-west-2:492436075634:endpoint:X3BJDTTBAWF7ACJYBJGFZ5ROUT3XG4RE6TZXXMA'
    dst_end_stage = 'arn:aws:dms:us-west-2:492436075634:endpoint:HU5OYFH6CKKROPTNDPMA6LN3DSILAOZI6RGXRWQ'
    print(create_rep_task(src_end_stage, dst_end_stage, rep, 'stage'))
    # ### production
    src_end_prod = 'arn:aws:dms:us-west-2:492436075634:endpoint:ZY4MZT2YXWYGXBAXU73P7PPYDKDW3GTWTUJOS5I'
    dst_end_prod = 'arn:aws:dms:us-west-2:492436075634:endpoint:E2K4QB3GYA5XVWNB2OLMUYKECCQOOCHQE2NMBLI'
    print(create_rep_task(src_end_prod, dst_end_prod, rep, 'prod'))
    # ### delete
    # print(del_rep_task('arn:aws:dms:us-west-2:492436075634:task:4ZQ6B63V2LKYGRANYXRQ7AZ2VRQV6GBXISMVVLI'))
