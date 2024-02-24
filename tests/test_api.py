import pytest
import requests
import configparser
import concurrent.futures
import time
import numpy as np
import matplotlib.pyplot as plt

# Lire le fichier de config
config = configparser.ConfigParser()
config.read('config.cfg')

# Des paramètres quelconques
json_req = {
        'libelle_region' : 'Auvergne-Rhône-Alpes',
        'nom_commune' : 'Brageac',
        'code_commune' : '15024',
        'nb_personne' : 2,
        'nb_m2' : 100
        }

# Tester sur une requête simple
def test_api_simple_request():

    # La requête de test
    response = requests.get("http://{}/price".format(config['API']['ip_address']), json=json_req)
    
    # Vérifie si la requête a réussi
    assert response.status_code == 200

    # Vérifie si le temps de réponse est inférieur à 6 secondes
    assert response.elapsed.total_seconds() < 6.0 

# Tester sur plusieurs requêtes simultanées
def test_api_multiple_requests():

    test_iteration = 20

    num_simul_requests = 10

    mean_elapsed_times = []

    for _ in range(test_iteration):

        # Début itération
        start_sec_time = time.time()

        # Requêtage simultané
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            
            futures = [executor.submit(lambda j: requests.get("http://{}/price".format(config['API']['ip_address']), json=j), json_req) for _ in range(num_simul_requests)]

        concurrent.futures.wait(futures)

        # Vérifie si les requêtes sont réussis
        status_codes = [f.result().status_code for f in futures]
        assert np.all(np.array(status_codes)==200)

        # Vérifie si les temps de réponse sont inférieurs à 6 secondes
        elapsed_times = [f.result().elapsed.total_seconds() for f in futures]
        elapsed_times = np.array(elapsed_times)
        mean_elapsed_times.append(np.mean(elapsed_times))
        assert np.all(elapsed_times<=6.0)

        # Fin itération
        elapsed_time = time.time() - start_sec_time
        if elapsed_time < 1:
            time.sleep(1-elapsed_time)
    
    # Faire un lineplot du temps moyen sur les itérations
    mean_elapsed_times = np.array(mean_elapsed_times)
    x = np.arange(len(mean_elapsed_times))
    plt.plot(x, mean_elapsed_times)
    plt.axhline(y=6, color='yellow', linestyle='--', label='y = 6')
    plt.title('Evolution du temps moyen de réponse au cours des itérations')
    plt.xlabel('Itération')
    plt.ylabel('Temps de réponse (en secondes)')
    plt.savefig('images/mean_response_time.png')

if __name__ == "__main__":

    # Afficher le résultat d'une requête exemple
    response = requests.get("http://{}/price".format(config['API']['ip_address']), json=json_req)
    print(response.status_code, response.json())