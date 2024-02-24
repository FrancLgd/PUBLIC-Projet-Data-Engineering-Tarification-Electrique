# Importer les librairies
import requests
import pandas as pd
import os
import configparser
from google.cloud import bigquery
import pyarrow
import time
from sklearn.linear_model import LinearRegression
import numpy as np

# Enregistrer le temps de début
start_time = time.time()

# Lire le fichier de config
config = configparser.ConfigParser()
config.read('config.cfg')

# Définir l'URL source
base_url = "https://data.enedis.fr/api/explore/v2.1/catalog/datasets/consommation-electrique-par-secteur-dactivite-commune/exports/json?timezone=UTC&use_labels=false&epsg=4326"

# Définir les paramètres de la requête : on ne prend ici que les consommations résidentielles de 2022
params = {"limit":-1,
          "select": "nom_commune, code_commune, conso_moyenne_mwh, annee, code_grand_secteur",
          "where": "(annee=date'2022' OR annee=date'2021' OR annee=date'2020') AND code_grand_secteur='RESIDENTIEL'"
          }

# Check fichier local
local_file = os.path.join('data','0_hist_conso_ville.csv')
if(not os.path.isfile(local_file)):
   
   # Envoyer la requête à l'API source
    response = requests.get(base_url, params=params)
    data = response.json()

    # Convertir en dataframe et exclure la colonne 'code_grand_secteur'
    data = pd.DataFrame(data).drop(['code_grand_secteur'],axis=1)

    # Sauvegarder en local
    data.to_csv(local_file, index=False)

# Lire le fichier local
data = pd.read_csv(local_file,dtype={
    'nom_commune': str,
    'code_commune': str,
    'conso_moyenne_mwh' :float,
    'annee' : int})

# Agréger les éventuelles consommations résidentielles moyennes de la commune
data = data.groupby(by=['nom_commune','code_commune','annee'])\
    .agg({'conso_moyenne_mwh':'mean'})\
    .reset_index()

# Incrémenter avec des données prévisionnelles

# ... fonction pour estimer la consommation pour une année donnée pour chaque commune
def estimate_year(group, target_years):

    try:

        model = LinearRegression()
        model.fit(group[['annee']], group['conso_moyenne_mwh'])
        
        # ... utiliser les coefficients calculés pour prédire les valeurs pour les années cibles
        predicted_values = model.predict(pd.DataFrame({'annee': target_years}))

    except Exception as e:
        print("Erreur : {}".format(str(e)))
        predicted_values = [np.nan for y in target_years]
    finally:

        df_target_years = pd.DataFrame({
            'nom_commune': group['nom_commune'].iloc[0],
            'code_commune': group['code_commune'].iloc[0],
            'annee': target_years,
            'conso_moyenne_mwh': predicted_values
        })

    return df_target_years

# ... check cache, si 
local_prev_file = os.path.join('data','conso_ville_prev.csv')
if (not os.path.isfile(local_prev_file)):

    # ... liste des années cibles
    target_years = [2023, 2024, 2025]

    # ... appliquer la fonction d'estimation à chaque groupe de commune pour chaque année cible
    data_prev = data.groupby(['nom_commune','code_commune']).apply(lambda group: estimate_year(group, target_years)).reset_index(drop=True)

    # ... sauvegarder en local
    data_prev.to_csv(local_prev_file, index=False)

prev_data = pd.read_csv(local_prev_file,dtype={
    'nom_commune': str,
    'code_commune': str,
    'conso_moyenne_mwh' :float,
    'annee' : int})

# ... concaténer les prévisions
data = pd.concat([data, prev_data], ignore_index=True)

# Instancier le client BigQuery
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials.json'
client = bigquery.Client(project=config['BigQuery']['project_id'])

# Définir la table de destination
dest_table = config['BigQuery']['project_id']+'.tarification_elec.conso_an_ville'

# Insérer dans la table
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
job = client.load_table_from_dataframe(dataframe = data,destination = dest_table,job_config = job_config)
job.result()

# Enregistrer le temps de fin
end_time = time.time()

# Calculer le temps total écoulé
total_time = end_time - start_time

# Afficher le temps total
print(f"Le script a pris {total_time} secondes.")