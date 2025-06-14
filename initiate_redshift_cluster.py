import json
import boto3
from logger import get_logger
from load_config import load_config

# Konfiguration des Loggers
logger = get_logger(__name__)

# Laden der Konfiguration
KEY, SECRET, dwh_params = load_config()

# Clients initialisieren
iam = boto3.client('iam', aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name='us-east-1')
redshift = boto3.client('redshift', region_name="us-east-1", aws_access_key_id=KEY, aws_secret_access_key=SECRET)
ec2 = boto3.resource('ec2', region_name="us-east-1", aws_access_key_id=KEY, aws_secret_access_key=SECRET)

# Parameter validieren
required_params = [dwh_params["DWH_CLUSTER_TYPE"], 
                   dwh_params["DWH_NODE_TYPE"], 
                   dwh_params["DWH_NUM_NODES"], 
                   dwh_params["DWH_DB"], 
                   dwh_params["DWH_CLUSTER_IDENTIFIER"], 
                   dwh_params["DWH_DB_USER"], 
                   dwh_params["DWH_DB_PASSWORD"]]
if any(param is None for param in required_params):
    raise ValueError("Einige erforderliche Parameter fehlen!")

# IAM-Rolle erstellen
try:
    logger.info("1.1 Erstelle eine neue IAM-Rolle")
    dwhRole = iam.create_role(
        Path='/',
        RoleName=dwh_params["DWH_IAM_ROLE_NAME"],
        Description='Erlaubt Redshift-Cluster den Zugriff auf AWS-Dienste.',
        AssumeRolePolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "redshift.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        }),
    )
    logger.info(f"IAM-Rolle erfolgreich erstellt, Statuscode: {dwhRole['ResponseMetadata']['HTTPStatusCode']}")
except Exception as e:
    logger.error(f"Fehler beim Erstellen der IAM-Rolle: {e}")

# Policy an IAM-Rolle anhängen
try:
    logger.info("1.2 Füge AmazonRedshiftFullAccess-Policy hinzu")
    response = iam.attach_role_policy(RoleName=dwh_params["DWH_IAM_ROLE_NAME"], 
                                      PolicyArn="arn:aws:iam::aws:policy/AmazonRedshiftFullAccess")
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        logger.info("Policy erfolgreich hinzugefügt")
    else:
        logger.warning(f"Policy konnte nicht hinzugefügt werden: {response}")
except Exception as e:
    logger.error(f"Fehler beim Anhängen der Policy: {e}")

# IAM-Rollen-ARN abrufen
try:
    logger.info("1.3 Hole die IAM-Rollen-ARN")
    roleArn = iam.get_role(RoleName=dwh_params["DWH_IAM_ROLE_NAME"])['Role']['Arn']
    logger.info(f"IAM-Rollen-ARN: {roleArn}")
except Exception as e:
    logger.error(f"Fehler beim Abrufen der IAM-Rollen-ARN: {e}")

# Redshift-Cluster erstellen
try:
    logger.info("2.1 Erstelle Redshift-Cluster")
    response = redshift.create_cluster(
        ClusterType=dwh_params["DWH_CLUSTER_TYPE"],
        NodeType=dwh_params["DWH_NODE_TYPE"],
        NumberOfNodes=int(dwh_params["DWH_NUM_NODES"]),
        DBName=dwh_params["DWH_DB"],
        ClusterIdentifier=dwh_params["DWH_CLUSTER_IDENTIFIER"],
        MasterUsername=dwh_params["DWH_DB_USER"],
        MasterUserPassword=dwh_params["DWH_DB_PASSWORD"],
        IamRoles=[roleArn]
    )
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        logger.info("Redshift-Cluster erfolgreich erstellt")
    else:
        logger.warning(f"Cluster konnte nicht erstellt werden: {response}")
except Exception as e:
    logger.error(f"Fehler beim Erstellen des Redshift-Clusters: {e}")

# Sicherheitsgruppen konfigurieren
try:
    logger.info("2.2 Konfiguriere Sicherheitsgruppen")
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=dwh_params["DWH_CLUSTER_IDENTIFIER"])['Clusters'][0]
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]

    logger.info(f"Sicherheitsgruppe {defaultSg.group_name} aktualisieren")
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(dwh_params["DWH_PORT"]),
        ToPort=int(dwh_params["DWH_PORT"])
    )
    logger.info("Sicherheitsgruppe erfolgreich aktualisiert")
except Exception as e:
    logger.error(f"Fehler beim Konfigurieren der Sicherheitsgruppen: {e}")
