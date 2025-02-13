import requests
import pandas as pd
from xml.etree import ElementTree

# Informations de base pour l'API
API_URL = "https://web-api.tp.entsoe.eu/api"
API_KEY = "85a8c0e0-cef2-47b7-a1b2-e09a6a86a7fa"  # Remplacer par votre clé API

start_date = "20240101"  # Date de début pour tester
end_date = "20240102"    # Date de fin pour tester
process_type = "A16"
area_code = "10YFR-RTE------C"
resolution = "PT60M"

# Création des plages de dates
start_dates = pd.date_range(start=start_date, end=end_date, freq="D")
end_dates = start_dates + pd.Timedelta(days=1)

# Liste pour stocker les résultats
all_data = []

# Fonction pour effectuer la requête API et récupérer les données
def fetch_data_from_api(start_date, end_date):
    # Convertir les dates en format requis pour l'API
    period_start = start_date.strftime("%Y%m%d%H%M")
    period_end = end_date.strftime("%Y%m%d%H%M")

    params = {
        "securityToken": API_KEY,
        "documentType": "A73",
        "processType": process_type,
        "in_Domain": area_code,
        "out_Domain": area_code,
        "periodStart": period_start,
        "periodEnd": period_end
    }

    # Effectuer la requête API
    response = requests.get(API_URL, params=params)

    if response.status_code == 200:
        # Parse le contenu XML
        root = ElementTree.fromstring(response.content)

        # Extraction des données pour chaque TimeSeries
        for timeseries in root.findall(".//{*}TimeSeries"):
            central_code = timeseries.find(".//{*}PowerSystemResources/{*}mRID").text
            central_name = timeseries.find(".//{*}PowerSystemResources/{*}name").text
            start_date = timeseries.find(".//{*}Period/{*}timeInterval/{*}start").text[:10]  # Date au format YYYY-MM-DD

            # Préparer les valeurs horaires (de 1 à 24)
            hourly_data = {i: 0 for i in range(1, 25)}  # Valeur par défaut à 0
            for point in timeseries.findall(".//{*}Period/{*}Point"):
                position = point.find("{*}position")
                quantity = point.find("{*}quantity")
                if position is not None and quantity is not None:
                    hourly_data[int(position.text)] = float(quantity.text)

            # Ajouter les données pour chaque heure
            for hour, value in hourly_data.items():
                all_data.append({
                    "Date": start_date,
                    "Hour": hour,
                    "Central Code": central_code,
                    "Central Name": central_name,
                    "Value (MW)": value,
                })
    else:
        print(f"Erreur {response.status_code} pour la période {period_start} - {period_end}")

# Boucle pour chaque jour
for start, end in zip(start_dates, end_dates):
    print(f"Traitement de la période : {start} - {end}")
    fetch_data_from_api(start, end)

# Convertir les données en DataFrame
df = pd.DataFrame(all_data)

# Sauvegarder au format CSV
output_file = f"production_horaire_{start_date}_to_{end_date}.csv"
df.to_csv(output_file, index=False)
print(f"Les données ont été sauvegardées dans le fichier : {output_file}")
