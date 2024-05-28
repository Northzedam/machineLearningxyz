import csv
import paho.mqtt.client as mqtt
import time

# Configuración del broker MQTT y el canal
broker = "broker.hivemq.com"
puerto = 1883
canal = "rmm/generadormtuonsiteenergy/temp"

# Archivo CSV para guardar los datos
archivo_csv = "datosDeSaltoConLosPies.csv"

# Función para manejar el evento de recepción de mensajes MQTT
def on_message(client, userdata, msg):
    # Decodificar el mensaje recibido (asumiendo que es una cadena en formato CSV)
    mensaje = msg.payload.decode("utf-8")

    # Analizar el mensaje CSV y guardar los datos en el archivo
    with open(archivo_csv, "a", newline="") as archivo:
        escritor_csv = csv.writer(archivo)
        datos = mensaje.split(",")  # Ajusta esto según el formato real de tus datos
        if(int(datos[1]) > 16):
          escritor_csv.writerow(datos)
          print('accX:'+ str(datos[0]) + ' accY:'+ str(datos[1])+ ' accZ:'+ str(datos[2]) + ' iX:'+ str(datos[3]) + ' iY:'+ str(datos[4]) + ' iZ:'+ str(datos[5]))

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
tiempo_espera = 7200  # segundos
time.sleep(tiempo_espera)

# Detener el bucle de recepción de mensajes
cliente.loop_stop()

# Desconexión del cliente MQTT
cliente.disconnect()
