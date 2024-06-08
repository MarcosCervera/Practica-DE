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

token = "BEARER eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NDg3Mjg4NDEsInR5cGUiOiJleHRlcm5hbCIsInVzZXIiOiJtYXJjb3MuY2VydmVyYUBncnVwb3NhbmNyaXN0b2JhbC5jb20ifQ.7BIMFrn8dExn-Vyq8KS275NXlpn3mtOnxWnZowEGrPjBN1b-aYfgW1baMV_-1q0pLmuTmG7K4kPqHnXcZvYdZg"
endpoints = ['milestones','usd','usd_of','usd_of_minorista','base','reservas','circulacion_monetaria',
             'depositos','cuentas_corrientes','cajas_ahorro','plazo_fijo','cer','uva','inflacion_mensual_oficial',
             'inflacion_interanual_oficial']