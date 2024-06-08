
import os
import logging
from dotenv import load_dotenv
from modules import api_bcra , transform_data , DataConn


logging.basicConfig(
    filename='app.log',
    filemode='a',
    format='%(asctime)s ::MainModule-> %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO)

load_dotenv()


def main():
    user_credentials = {
        "REDSHIFT_USERNAME" : os.getenv('REDSHIFT_USERNAME'),
        "REDSHIFT_PASSWORD" : os.getenv('REDSHIFT_PASSWORD'),
        "REDSHIFT_HOST" : os.getenv('REDSHIFT_HOST'),
        "REDSHIFT_PORT" : os.getenv('REDSHIFT_PORT', '5439'),
        "REDSHIFT_DBNAME" : os.getenv('REDSHIFT_DBNAME')
    }

    schema:str = "marcoscervera_coderhouse"
    table:str = "prueba"
    token:str = "BEARER eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NDg3Mjg4NDEsInR5cGUiOiJleHRlcm5hbCIsInVzZXIiOiJtYXJjb3MuY2VydmVyYUBncnVwb3NhbmNyaXN0b2JhbC5jb20ifQ.7BIMFrn8dExn-Vyq8KS275NXlpn3mtOnxWnZowEGrPjBN1b-aYfgW1baMV_-1q0pLmuTmG7K4kPqHnXcZvYdZg"
    endpoints = ['milestones','usd','usd_of','usd_of_minorista','base','reservas','circulacion_monetaria',
             'depositos','cuentas_corrientes','cajas_ahorro','plazo_fijo','cer','uva','inflacion_mensual_oficial',
             'inflacion_interanual_oficial']

    data_conn = DataConn(user_credentials, schema)
    df = api_bcra(token, endpoints)
    df_bcra = transform_data(df)
    
    try:
        data_conn.upload_data(df_bcra, 'prueba')
        logging.info(f"Data uploaded to -> {schema}.{table}")

    except Exception as e:
        logging.error(f"Not able to upload data\n{e}")
        
    finally:
        data_conn.close_conn()

if __name__ == "__main__":
    main()   


