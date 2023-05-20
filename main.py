import csv
import paho.mqtt.client as mqtt
import time

# Configuración del broker MQTT y el canal
broker = "broker.hivemq.com"
puerto = 1883
canal = "rmm/generadormtuonsiteenergy/temp"

# Archivo CSV para guardar los datos
archivo_csv = "datos_conduccion.csv"

# Función para manejar el evento de recepción de mensajes MQTT
def on_message(client, userdata, msg):
    # Decodificar el mensaje recibido (asumiendo que es una cadena en formato CSV)
    mensaje = msg.payload.decode("utf-8")

    # Analizar el mensaje CSV y guardar los datos en el archivo
    with open(archivo_csv, "a", newline="") as archivo:
        escritor_csv = csv.writer(archivo)
        datos = mensaje.split(",")  # Ajusta esto según el formato real de tus datos
        escritor_csv.writerow(datos)

# Configuración del cliente MQTT
cliente = mqtt.Client()

# Asignar la función de manejo de mensajes al cliente MQTT
cliente.on_message = on_message

# Conexión al broker MQTT
cliente.connect(broker, puerto)

#extraLine

# Suscripción al canal MQTT
cliente.subscribe(canal)

# Inicio del bucle para recibir mensajes MQTT
cliente.loop_start()

# Esperar un tiempo suficiente para recibir datos (ajústalo según tus necesidades)
tiempo_espera = 60  # segundos
time.sleep(tiempo_espera)

# Detener el bucle de recepción de mensajes
cliente.loop_stop()

# Desconexión del cliente MQTT
cliente.disconnect()
