from kafka import KafkaProducer, KafkaConsumer
import json
import threading
import sys
import mysql.connector
import time

def obtener_productor(servidor_kafka): # Crea un nuevo productor de Kafka
    return KafkaProducer(
        bootstrap_servers=[servidor_kafka], # Dirección del servidor de Kafka
        value_serializer=lambda v: json.dumps(v).encode('utf-8'), # Forma de codificar los mensajes
    )

def obtener_consumidor(topico, grupo_id, servidor_kafka): # Crea un nuevo consumidor de Kafka
    return KafkaConsumer(
        topico,
        bootstrap_servers=[servidor_kafka], # Dirección del servidor de Kafka
        value_deserializer=lambda v: json.loads(v.decode('utf-8')), # Forma de decodificar los mensajes
        group_id = grupo_id, # Identificador del grupo de consumidores
        auto_offset_reset='earliest', # Comenzar a leer desde el principio del tópico
    )

def conectar_bd(servidor_bd):
    try:
        ip_bd, puerto_bd = servidor_bd.split(":")
        puerto_bd = int(puerto_bd)

        conexion = mysql.connector.connect(
            host = ip_bd,
            port = puerto_bd,
            user = "user_remot", # Cambiar
            password = "password", # Cambiar
            database = "nombre_bd" # Cambiar
        )

        print(f"Servidor conectado a la Base de Datos en {servidor_bd}")
        return conexion
    except Exception as e:
        print(f"Error al conectar a la Base de Datos: {e}")
        return None
    


############## CLASE SERVIDOR ##############



class Servidor:
    def __init__(self, servidor_kafka, servidor_bd):
        self.servidor_kafka = servidor_kafka
        self.servidor_bd = servidor_bd
        self.productor = obtener_productor(servidor_kafka)
        self.conexion_bd = conectar_bd(servidor_bd)

    def verificar_usuario(self, usuario, contraseña):
        # Aquí iría la lógica para verificar el usuario en la base de datos
        print() # BORRAR



############# MAIN #############



def main():
    if len(sys.argv) < 3:
        print("ERROR: Argumentos incorrectos")
        print("Argumentos correctos: python servidor.py <IP:puerto_broker> <IP:puerto_BD>")
        sys.exit(1) # Devuelve error
    
    servidor_kafka = sys.argv[1]
    servidor_bd = sys.argv[2]

    print(f"Kafka: {servidor_kafka}. BD: {servidor_bd}") # BORRRARR

    server = Servidor(servidor_kafka, servidor_bd)


if __name__ == "__main__":
    main()