import requests
import pandas as pd

class DataRetriever:
    def __init__(self, token):
        self.base_url = "https://api.estadisticasbcra.com/"
        self.headers = {"Authorization": token}
    
    def get_data(self, endpoint):
        url = f"{self.base_url}{endpoint}"
        response = requests.get(url, headers=self.headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()
    
    def to_dataframe(self, data_json, variable_name):
        df = pd.DataFrame(data_json)
        df = df.rename(columns={'d': 'fecha', 'v': 'valor'})
        df['variable'] = variable_name
        return df
    
    def get_all_data(self, endpoints):
        df = pd.DataFrame(columns=['fecha', 'valor', 'variable'])
        for endpoint in endpoints:
            data_json = self.get_data(endpoint)
            endpoint_df = self.to_dataframe(data_json, endpoint)
            df = pd.concat([df, endpoint_df], ignore_index=True)
        return df

# Uso de la clase
token = "tu_token_aqui"  # Asegúrate de reemplazar esto con tu token real
endpoints = [
    'milestones', 'usd', 'usd_of', 'usd_of_minorista', 'base', 'reservas',
    'circulacion_monetaria', 'depositos', 'cuentas_corrientes', 'cajas_ahorro',
    'plazo_fijo', 'cer', 'uva', 'inflacion_mensual_oficial', 'inflacion_interanual_oficial'
]

client = DataRetriever(token)
df = client.get_all_data(endpoints)

print(df)
