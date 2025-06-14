import boto3
from logger import get_logger
from load_config import load_config
from botocore.exceptions import ClientError

# Logger initialisieren
logger = get_logger(__name__)

# Laden der Konfiguration
KEY, SECRET, dwh_params = load_config()

# Clients initialisieren
redshift = boto3.client('redshift', aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name='us-west-2')
iam = boto3.client('iam', aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name='us-west-2')

# Cluster löschen
try:
    logger.info("Prüfe, ob Redshift-Cluster existiert...")
    cluster_info = redshift.describe_clusters(ClusterIdentifier=dwh_params["DWH_CLUSTER_IDENTIFIER"])
    
    if cluster_info and 'Clusters' in cluster_info:
        logger.info("Redshift-Cluster wird gelöscht...")
        redshift.delete_cluster(ClusterIdentifier=dwh_params["DWH_CLUSTER_IDENTIFIER"], SkipFinalClusterSnapshot=True)
        logger.info("Cluster erfolgreich zum Löschen markiert.")
    else:
        logger.warning("Cluster nicht gefunden. Vielleicht wurde er bereits gelöscht?")
except ClientError as e:
    if "ClusterNotFound" in str(e):
        logger.warning("Cluster ist bereits gelöscht oder existiert nicht.")
    else:
        logger.error(f"Fehler beim Löschen des Clusters: {e}")

# IAM-Rolle und Policy löschen
try:
    logger.info("Trenne die IAM-Policy von der Rolle...")
    iam.detach_role_policy(RoleName=dwh_params["DWH_IAM_ROLE_NAME"], PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    logger.info("Policy erfolgreich entfernt.")

    logger.info("Lösche IAM-Rolle...")
    iam.delete_role(RoleName=dwh_params["DWH_IAM_ROLE_NAME"])
    logger.info("IAM-Rolle erfolgreich gelöscht.")
except ClientError as e:
    if "NoSuchEntity" in str(e):
        logger.warning("IAM-Rolle existiert bereits nicht.")
    else:
        logger.error(f"Fehler beim Löschen der IAM-Rolle: {e}")

logger.info("Alle Ressourcen wurden erfolgreich bereinigt!")
