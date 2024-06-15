import requests
import pandas as pd

def api_bcra(token, endpoints):
    df = pd.DataFrame(columns=['fecha', 'valor', 'variable'])
    headers = {"Authorization": token}

    for endpoint in endpoints:
        url = "https://api.estadisticasbcra.com/" + endpoint
        response = requests.get(url, headers=headers)
        
        # Comprobar si la solicitud fue exitosa
        if response.status_code == 200:
            data_json = response.json()
            data = pd.DataFrame(data_json)
            data = data.rename(columns={'d': 'fecha', 'v': 'valor'})
            data['variable'] = endpoint
            df = pd.concat([df, data], ignore_index=True)
        else:
            # Si hubo un error en la solicitud, imprimir el código de estado
            print(f"Error {response.status_code} al obtener datos del endpoint: {endpoint}")
    
    return df
