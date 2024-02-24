import os
import requests
import pandas as pd
from datetime import timedelta, datetime

import configparser

from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.email import EmailOperator

from google.cloud import bigquery

# Lire le fichier de config
dossier_courant = os.getcwd()
print("Le dossier courant est :", dossier_courant)

config = configparser.ConfigParser()
config.read('config.cfg')

# Nom du DAG
DAG_NAME = os.path.basename(__file__).replace(".py", "")  # Le nom du DAG est le nom du fichier

# Arguments du DAG
default_args = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(seconds=5)#,
    # 'email_on_failure': True,
    #'email_on_retry': False,
}

@dag(DAG_NAME, default_args=default_args, schedule_interval="30 2 * * *", start_date=days_ago(2))
def dag_insert_conso_jour_region_in_bq():

    """

    Ce DAG permet la récupération, la transformation et l'insertion 
    dans BigQuery des données quotidiennes (de la veille) de consommations électriques à la région.

    """

    @task(provide_context=True)
    def extract_data(**kwargs):
        
        # Obtenir la date de la veille
        # kwargs = dict()
        # kwargs["date"] = "2023-12-07"
        date_j = datetime.strptime(kwargs["date"], "%Y-%m-%d")
        date_jm1 = date_j - timedelta(days=1)

        # Définir l'URL source
        base_url = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-regional-tr/exports/json"

        # Définir les paramètres de la requête : on prend les données de la veille
        params = {
            "limit" : -1,
            "select" : "code_insee_region, libelle_region, date, date_heure, consommation",
            "where" : "date='{day}'".format(day=date_jm1.strftime('%Y-%m-%d'))
        }

        # Envoyer la requête à l'API source
        response = requests.get(base_url, params=params)
        data = response.json()
        
        # Convertir les données en dataframe
        data = pd.DataFrame(data)
        
        # Pousser vers un XCom
        kwargs["ti"].xcom_push(key="extracted_data", value=data)

    @task(provide_context=True)
    def transform_data(**kwargs):
        
        # Récupérer les données brutes du XCom
        data = kwargs["ti"].xcom_pull(key="extracted_data", task_ids="extract_data")

        # Procéder à l'agrégation
        data = data.groupby(by=['code_insee_region','libelle_region','date'])\
            .agg({'consommation':'mean'})\
            .reset_index()
        
        # Arrondir la consommation à l'entier
        data['conso_mwh'] = data['consommation'].round()
        data = data.drop('consommation',axis=1)

        # Convertir la date en datetime
        data['date'] = pd.to_datetime(data['date'],format = "%Y-%m-%d")

        # Pousser vers un XCom
        kwargs["ti"].xcom_push(key="transformed_data", value=data)


    @task(provide_context=True)
    def load_to_bigquery(**kwargs):

        data = kwargs["ti"].xcom_pull(key="transformed_data", task_ids="transform_data")

        # Instancier le client BigQuery
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials.json'
        client = bigquery.Client(project=config['BigQuery']['project_id'])

        # Définir la table de destination
        dest_table = config['BigQuery']['project_id']+'.tarification_elec.conso_jour_region'

        # Insérer dans la table
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(dataframe = data,destination = dest_table,job_config = job_config)
        job.result()

    extract_data_task = extract_data(date="{{ ds }}")
    transform_data_task = transform_data()
    load_to_bigquery_task = load_to_bigquery()

    # Définir la dépendance entre les tâches
    extract_data_task >> transform_data_task >> load_to_bigquery_task

    # Tâche d'envoie d'e-mail en cas d'échec
    """"
    email_notif = EmailOperator(task_id='email_on_failure',
                                       to = config['AlertEmailReceiver']['email'],
                                       subject='Alerte : échec du DAG {}'.format(DAG_NAME),
                                       html_content="Le DAG a échoué. Veuillez vérifier les logs pour plus d\'informations.")

    load_to_bigquery_task >> email_notif
    """

# Instanciation du DAG
dag_instance = dag_insert_conso_jour_region_in_bq() 

