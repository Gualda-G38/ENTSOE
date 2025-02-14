import requests
import pandas as pd
from xml.etree import ElementTree as ET
from dotenv import load_dotenv
import os

API_URL = "https://web-api.tp.entsoe.eu/api"

# Load API key
load_dotenv()
API_KEY = os.getenv("ENTSOE_API_KEY")

# Features for the GET REQUEST
start_date = "20240101"
end_date = "20241231"
process_type = "A16"
area_code = "10YFR-RTE------C"
resolution = "PT60M"

# Création des plages de dates
start_dates = pd.date_range(start=start_date, end=end_date, freq="D")
end_dates = start_dates + pd.Timedelta(days=1)

# List to stock the data
data = []

# Create a fonction to get the API response
def fetch_data_from_api(start_date, end_date):
    # Convert date to fit the API format
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

    # Request
    response = requests.get(API_URL, params=params)
    print(f"Status Code: {response.status_code}")

    if response.status_code == 200:
        # Reload the XML file to ensure clean parsing
        root = ET.fromstring(response.content)

        # Handle XML namespaces
        namespaces = {
            'ns0': 'urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0'
        }

        # Iterate through each TimeSeries
        for timeseries in root.findall(".//ns0:TimeSeries", namespaces):
            ts_id = timeseries.find("ns0:mRID", namespaces).text if timeseries.find("ns0:mRID", namespaces) is not None else "Unknown"
            bidding_zone_elem = timeseries.find("ns0:inBiddingZone_Domain.mRID", namespaces)
            bidding_zone = bidding_zone_elem.text if bidding_zone_elem is not None else "Unknown"
            resource_elem = timeseries.find("ns0:MktPSRType/ns0:PowerSystemResources/ns0:name", namespaces)
            resource_name = resource_elem.text if resource_elem is not None else "Unknown"

            # Get the start time and resolution, handling missing values
            period = timeseries.find("ns0:Period", namespaces)
            if period is not None:
                start_time_elem = period.find("ns0:timeInterval/ns0:start", namespaces)
                start_time = start_time_elem.text if start_time_elem is not None else "1970-01-01T00:00Z"
                resolution_elem = period.find("ns0:resolution", namespaces)
                resolution = resolution_elem.text if resolution_elem is not None else "PT60M"

                # Convert start time to datetime
                start_dt = pd.to_datetime(start_time)

                # Iterate through all Points in the TimeSeries
                for point in period.findall("ns0:Point", namespaces):
                    position_elem = point.find("ns0:position", namespaces)
                    quantity_elem = point.find("ns0:quantity", namespaces)

                    position = int(position_elem.text) if position_elem is not None else 0
                    quantity = float(quantity_elem.text) if quantity_elem is not None else 0.0
                    timestamp = start_dt + pd.Timedelta(minutes=60 * (position - 1))  # Adjust time based on resolution

                    data.append([ts_id, resource_name, bidding_zone, timestamp, quantity])

    else:
        print(f"Error {response.status_code} for the period {period_start} - {period_end}")

# Boucle pour chaque jour
for start, end in zip(start_dates, end_dates):
    print(f"Traitement de la période : {start} - {end}")
    fetch_data_from_api(start, end)

# Convertir les données en DataFrame
df = pd.DataFrame(data, columns=["TimeSeries_ID", "Resource_Name", "BiddingZone", "Timestamp", "Quantity"])

# Sauvegarder au format CSV
output_file = f"production_horaire_{start_date}_to_{end_date}.csv"
df.to_csv(output_file, index=False)
print(f"Les données ont été sauvegardées dans le fichier : {output_file}")
