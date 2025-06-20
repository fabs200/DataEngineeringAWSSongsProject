import json
import boto3
import time
import configparser
import psycopg2
from botocore.exceptions import ClientError
from logger import get_logger
from load_config import load_config

# Konfiguration des Loggers
logger = get_logger(__name__)

# Laden der Konfiguration
KEY, SECRET, dwh_params = load_config()

# Konsistente Region definieren (gleich wie in clean_up_cluster.py)
REGION = 'us-west-2'

# Clients initialisieren mit konsistenter Region
iam = boto3.client('iam', aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name=REGION)
redshift = boto3.client('redshift', region_name=REGION, aws_access_key_id=KEY, aws_secret_access_key=SECRET)
ec2 = boto3.resource('ec2', region_name=REGION, aws_access_key_id=KEY, aws_secret_access_key=SECRET)

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

def check_role_exists(role_name):
    """Überprüft, ob eine IAM-Rolle bereits existiert"""
    try:
        iam.get_role(RoleName=role_name)
        return True
    except iam.exceptions.NoSuchEntityException:
        return False

# IAM-Rolle erstellen
roleArn = None
try:
    # Prüfen, ob Rolle bereits existiert
    if check_role_exists(dwh_params["DWH_IAM_ROLE_NAME"]):
        logger.info(f"IAM-Rolle {dwh_params['DWH_IAM_ROLE_NAME']} existiert bereits")
        roleArn = iam.get_role(RoleName=dwh_params["DWH_IAM_ROLE_NAME"])['Role']['Arn']
    else:
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
        roleArn = iam.get_role(RoleName=dwh_params["DWH_IAM_ROLE_NAME"])['Role']['Arn']
except Exception as e:
    logger.error(f"Fehler beim Erstellen der IAM-Rolle: {e}")
    exit(1)  # Beenden bei kritischem Fehler

# S3ReadOnlyAccess Policy an IAM-Rolle anhängen
try:
    logger.info("1.2 Füge S3ReadOnlyAccess-Policy hinzu")
    response = iam.attach_role_policy(
        RoleName=dwh_params["DWH_IAM_ROLE_NAME"], 
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        logger.info("S3ReadOnlyAccess-Policy erfolgreich hinzugefügt")
    else:
        logger.warning(f"Policy konnte nicht hinzugefügt werden: {response}")
except ClientError as e:
    if e.response['Error']['Code'] == 'EntityAlreadyExists' or 'PolicyAlreadyAttached' in str(e):
        logger.info("Policy ist bereits an die Rolle angehängt")
    else:
        logger.error(f"Fehler beim Anhängen der Policy: {e}")

def check_cluster_exists(cluster_identifier):
    """Überprüft, ob ein Redshift-Cluster bereits existiert"""
    try:
        redshift.describe_clusters(ClusterIdentifier=cluster_identifier)
        return True
    except redshift.exceptions.ClusterNotFoundFault:
        return False

# Redshift-Cluster erstellen oder überprüfen
try:
    cluster_exists = check_cluster_exists(dwh_params["DWH_CLUSTER_IDENTIFIER"])
    
    if cluster_exists:
        logger.info(f"Redshift-Cluster {dwh_params['DWH_CLUSTER_IDENTIFIER']} existiert bereits")
    else:
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
    exit(1)  # Beenden bei kritischem Fehler

# Warten, bis der Cluster verfügbar ist
def wait_for_cluster_available():
    """Wartet, bis der Cluster den Status 'available' hat"""
    logger.info("Warte, bis der Cluster verfügbar ist...")
    
    while True:
        try:
            cluster_info = redshift.describe_clusters(
                ClusterIdentifier=dwh_params["DWH_CLUSTER_IDENTIFIER"]
            )['Clusters'][0]
            
            status = cluster_info['ClusterStatus']
            logger.info(f"Cluster-Status: {status}")
            
            if status == 'available':
                logger.info("Cluster ist jetzt verfügbar!")
                return cluster_info
            
            # Warte 30 Sekunden und prüfe erneut
            time.sleep(30)
            
        except Exception as e:
            logger.error(f"Fehler beim Überprüfen des Cluster-Status: {e}")
            raise

# Cluster-Informationen abrufen
try:
    myClusterProps = wait_for_cluster_available()
    
    # Endpoint und DWH_ENDPOINT_ADDRESS für Verbindung
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    logger.info(f"Cluster-Endpoint: {DWH_ENDPOINT}")
    
    # Config-Datei aktualisieren
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # HOST aktualisieren
    config.set('CLUSTER', 'HOST', DWH_ENDPOINT)
    
    # REGION hinzufügen falls nicht vorhanden
    if not config.has_option('CLUSTER', 'REGION'):
        config.set('CLUSTER', 'REGION', REGION)
    
    # IAM_ROLE ARN aktualisieren
    config.set('IAM_ROLE', 'ARN', roleArn)
    
    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile)
    
    logger.info("dwh.cfg wurde mit aktuellen Werten aktualisiert")
    
except Exception as e:
    logger.error(f"Fehler beim Abrufen der Cluster-Informationen: {e}")
    exit(1)

# Sicherheitsgruppen konfigurieren
try:
    logger.info("2.2 Konfiguriere Sicherheitsgruppen")
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    security_groups = list(vpc.security_groups.all())
    
    if len(security_groups) == 0:
        logger.error("Keine Sicherheitsgruppen gefunden")
        exit(1)
        
    defaultSg = security_groups[0]

    logger.info(f"Sicherheitsgruppe {defaultSg.group_name} aktualisieren")
    try:
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(dwh_params["DWH_PORT"]),
            ToPort=int(dwh_params["DWH_PORT"])
        )
        logger.info("Sicherheitsgruppe erfolgreich aktualisiert")
    except ClientError as e:
        if 'InvalidPermission.Duplicate' in str(e):
            logger.info("Zugriff bereits konfiguriert")
        else:
            raise
except Exception as e:
    logger.error(f"Fehler beim Konfigurieren der Sicherheitsgruppen: {e}")

# Verbindung zum Cluster validieren
def validate_connection():
    """Überprüft, ob der Cluster erreichbar ist"""
    try:
        logger.info("Überprüfe Verbindung zum Redshift-Cluster...")
        conn = psycopg2.connect(
            host=DWH_ENDPOINT,
            dbname=dwh_params["DWH_DB"],
            user=dwh_params["DWH_DB_USER"],
            password=dwh_params["DWH_DB_PASSWORD"],
            port=dwh_params["DWH_PORT"]
        )
        
        # Cursor erstellen und eine einfache Abfrage ausführen
        cur = conn.cursor()
        cur.execute("SELECT 1")
        conn.close()
        
        logger.info("Verbindung zum Redshift-Cluster erfolgreich hergestellt!")
        return True
    except Exception as e:
        logger.error(f"Fehler bei der Verbindung zum Redshift-Cluster: {e}")
        return False

# Wiederholtes Überprüfen der Verbindung (bis zu 5 Versuche)
max_attempts = 5
attempt = 0
connection_successful = False

while attempt < max_attempts and not connection_successful:
    attempt += 1
    logger.info(f"Verbindungsversuch {attempt}/{max_attempts}")
    connection_successful = validate_connection()
    
    if not connection_successful and attempt < max_attempts:
        logger.info("Warte 30 Sekunden vor dem nächsten Versuch...")
        time.sleep(30)

if connection_successful:
    logger.info("Redshift-Cluster wurde erfolgreich eingerichtet und ist erreichbar!")
    logger.info(f"Host: {DWH_ENDPOINT}")
    logger.info(f"Datenbank: {dwh_params['DWH_DB']}")
    logger.info(f"Benutzer: {dwh_params['DWH_DB_USER']}")
    logger.info("Führe 'python create_tables.py' aus, um die Tabellen zu erstellen.")
else:
    logger.error("Verbindung zum Redshift-Cluster konnte nicht hergestellt werden.")
    logger.error("Überprüfe die Netzwerkeinstellungen und Sicherheitsgruppen.")