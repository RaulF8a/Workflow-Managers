from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule

import requests
import json
from datetime import timedelta
import os
import dotenv
import random

# @task
# def helloWorld ():
#     print ("Hello World")

# with Flow ("My first flow") as f:
#     r = helloWorld ()

# f.run ()

# # Con visualize creamos un diagrama del flujo de la aplicacion.
# f.visualize () 

# En versiones superiores a la 2.0.0, para crear un flow, se hace con la siguiente sintaxis.
# @flow
# def prefectFlow ():
#     helloWorld ()

# prefectFlow ()

# Temas de las imagenes.
unsplashQueries = ["history", "life", "wisdom", "art", "office", "nature"]

# Obtener la clave de la API.
dotenv.load_dotenv ()
apiKey = os.environ["UNSPLASH_API_KEY"]

# Creamos una agenda de intervalos, que se encargara de llamar al flujo cada cierto tiempo.
scheduleData = IntervalSchedule (interval=timedelta (minutes=1))

# Extraer
@task(max_retries=10, retry_delay=timedelta (seconds=15))
def obtenerDatos ():
    # Solicitamos los datos de la imagen a la API.
    tema = random.choice (unsplashQueries)
    url = f"https://api.unsplash.com/photos/random?query={tema}&client_id={apiKey}"

    peticion = requests.get (url)

    datos = peticion.json()

    return datos

# Transformar
@task
def transformarDatos (datos):
    # Obtenemos el url de la imagen.
    url = datos["urls"]["regular"]

    return url

# Almacenar
@task
def almacenarDatos (imagen):
    # Para evitar tener muchas imagenes descargadas, sobreescribimos el mismo archivo.
    nombreArchivo = "imagen.jpg"
    peticionImagen = requests.get (imagen, stream=True)
    
    if (peticionImagen.status_code != 200):
        raise Exception ("Ocurrio un error al solicitar la imagen a la API.")
    
    with open (nombreArchivo, "wb") as imagenDescarga:
        for chunk in peticionImagen:
            imagenDescarga.write (chunk)
        
    os.system ("start imagen.jpg")

def crearFlow (schedule):
    # Agregamos la agenda como parametro a Flow.
    with Flow ("ETL Flow", schedule) as flow:
        datos = obtenerDatos ()
        imagen = transformarDatos (datos)
        almacenarDatos (imagen)

    return flow

flow = crearFlow (scheduleData)
# flow.visualize ()
flow.run ()
