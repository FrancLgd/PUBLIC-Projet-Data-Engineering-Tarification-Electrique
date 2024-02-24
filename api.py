from flask import Flask, request, jsonify
from google.cloud import bigquery
import os
import pandas as pd
import configparser
from datetime import datetime,timedelta
import numpy as np

app = Flask(__name__)

# Lire le fichier de config
config = configparser.ConfigParser()
config.read('config.cfg')

# Instancier le client BigQuery
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials.json'
client = bigquery.Client(project=config['BigQuery']['project_id'])

# Fonction pour récupérer la date du jour
def get_date_run():
    return datetime.today().date()

# Fonction pour checker les champs
def check_fields(body,required_fields):
    
    # ... stocker required_fields sous formes d'ensemble
    required_fields = set(required_fields)

    # ... idem pour les clés de body
    body_fields = set(body.keys())

    # ... retourner vrai si required_fields est inclut dans body_fields
    return required_fields <= body_fields

# Fonction pour récupérer la conso moyenne des 30 derniers jours d'une région
def get_region_last30days_conso(libelle_region,date_run=get_date_run()):

    # libelle_region, date_run = "Normandie", get_date_run()

    # ... calculer la date d'il y a 30 jours
    date_last30 = date_run - timedelta(days=30)

    # ... définir la table source
    source_table = config['BigQuery']['project_id']+'.tarification_elec.conso_jour_region'

    # ... réaliser la requête SQL
    sql_query = f"""
                SELECT * 
                FROM {source_table}
                WHERE 
                    date <= '{date_run}' AND
                    date >= '{date_last30}' AND
                    libelle_region = '{libelle_region}'
                """.replace('\n','')
    
    # ... envoyer la requête et récupérer le résultat
    query_job = client.query(sql_query)
    df_conso_region = query_job.to_dataframe()

    # ... retourner la conso moyenne 
    return df_conso_region['conso_mwh'].mean()

# Fonction pour récupérer les consos annuelles d'une ville
def get_city_alpha(nom_commune,code_commune,date_run=get_date_run()):

    # ... définir la table source
    source_table = config['BigQuery']['project_id']+'.tarification_elec.conso_an_ville'

    # ... définir l'année n et (n+1)
    n_year = int(date_run.strftime("%Y"))
    np1_year = n_year + 1

    # ... réaliser la requête SQL
    sql_query = f"""
                SELECT * 
                FROM {source_table}
                WHERE 
                    (annee = {n_year} OR
                    annee = {np1_year}) AND
                    nom_commune = '{nom_commune}' AND
                    code_commune = '{code_commune}'
                """.replace('\n','')
    
    # ... envoyer la requête et récupérer le résultat
    query_job = client.query(sql_query)
    df_conso_ville = query_job.to_dataframe().sort_values(by='annee',ascending=False)
    
    # ... calculer alpha
    alpha = df_conso_ville["conso_moyenne_mwh"][1]/df_conso_ville["conso_moyenne_mwh"][0]
    alpha = min(max([1,alpha]),1.3)

    return alpha

# Route racine
@app.route('/price', methods = ['GET'])
def get_price():

    # ... prix du contrat
    C = 6 * 12

    try:

        # ... récupérer la requête 
        body = request.get_json()

        # ... vérifier si tous les champs recquis sont fournis
        if not check_fields(body,{'libelle_region','nom_commune','code_commune','nb_personne','nb_m2'}):
            return jsonify({'error': "Missing fields."}), 400
        
        # ... estimer la consommation annuelle du foyer
        conso_chauffage = 110*body['nb_m2']
        conso_eau_chaude = 800*body['nb_personne']
        conso_cuisson = 200*body['nb_personne']
        conso_eletro = 1100
        conso_foyer = conso_chauffage + conso_eau_chaude + conso_cuisson + conso_eletro

        # ... récupérer la conso moyenne de la ville sur les 30 derniers jours
        conso_region_last30days = get_region_last30days_conso(body['libelle_region'])
        
        # ... calculer le coefficient M associé
        M = min([0.05,0.01*conso_region_last30days/4000])

        # ... coefficient de consommation prévsionnelle annuelle de la ville
        alpha = get_city_alpha(body['nom_commune'],body['code_commune']) 

        # ... calculer le prix associé
        prix = np.round((0.1558 + alpha*M)*conso_foyer + C)

        return jsonify({'price':prix}), 200
    
    except Exception as e:
        return jsonify({'error':str(e)}),500