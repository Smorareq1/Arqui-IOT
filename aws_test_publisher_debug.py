import os
import sys
import json
import time
import random
import argparse
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

# Cargar .env si está disponible
try:
    from dotenv import load_dotenv  # pip install python-dotenv

    load_dotenv()
except Exception:
    pass

HERE = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.getenv("AWS_BASE_DIR", HERE)

CLIENT_ID = os.getenv("AWS_CLIENT_ID", "ArduinoUnoClient-Test")
AWS_ENDPOINT = os.getenv("AWS_IOT_ENDPOINT", "TU_ENDPOINT_AWS")
AWS_PORT = int(os.getenv("AWS_IOT_PORT", "8883"))
ROOT_CA = os.getenv("AWS_IOT_ROOT_CA", os.path.join(BASE_DIR, "Certificado", "root-CA.crt"))
PRIVATE_KEY = os.getenv("AWS_IOT_PRIVATE_KEY", os.path.join(BASE_DIR, "Certificado", "private.pem.key"))
CERTIFICATE = os.getenv("AWS_IOT_CERT", os.path.join(BASE_DIR, "Certificado", "certificate.pem.crt"))
TOPIC_DEF = os.getenv("AWS_IOT_TOPIC", "arduino/telemetry")


def assert_file(label: str, path: str):
    if not os.path.isfile(path):
        raise FileNotFoundError(f"{label} no existe: {path}")
    file_size = os.path.getsize(path)
    print(f"✓ {label}: {path} (tamaño: {file_size} bytes)")

    # Verificar que el archivo no esté vacío y tenga el formato correcto
    with open(path, 'r', encoding='utf-8') as f:
        first_line = f.readline().strip()
        if 'CERTIFICATE' in label and not first_line.startswith('-----BEGIN CERTIFICATE-----'):
            print(f"⚠️ Advertencia: {label} podría no tener el formato correcto")
        elif 'PRIVATE_KEY' in label and not (
                first_line.startswith('-----BEGIN RSA PRIVATE KEY-----') or first_line.startswith(
                '-----BEGIN PRIVATE KEY-----')):
            print(f"⚠️ Advertencia: {label} podría no tener el formato correcto")


def build_client(client_id: str):
    c = AWSIoTMQTTClient(client_id)
    c.configureEndpoint(AWS_ENDPOINT, AWS_PORT)
    c.configureCredentials(ROOT_CA, PRIVATE_KEY, CERTIFICATE)
    c.configureOfflinePublishQueueing(-1)
    c.configureDrainingFrequency(2)
    # Timeouts aumentados para debugging
    c.configureConnectDisconnectTimeout(30)
    c.configureMQTTOperationTimeout(15)
    return c


def connect_with_retry(c, attempts=3):
    delay = 2.0
    for i in range(1, attempts + 1):
        try:
            print(f"📡 Intento de conexión {i}/{attempts}...")
            c.connect()
            print("✅ ¡Conexión exitosa!")
            return
        except Exception as e:
            error_name = type(e).__name__
            error_msg = str(e)
            if i == attempts:
                print(f"❌ Error final de conexión: {error_name}: {error_msg}")
                print("\n🔍 Posibles causas:")
                print("  1. Certificados no configurados en AWS IoT Core")
                print("  2. Política IoT faltante o restrictiva")
                print("  3. Thing no asociado al certificado")
                print("  4. Certificado inactivo en AWS")
                print("  5. Firewall bloqueando puerto 8883")
                raise
            print(f"❌ Conexión fallida (intento {i}/{attempts}): {error_name}: {error_msg}")
            print(f"⏳ Reintentando en {delay:.1f}s...")
            time.sleep(delay)
            delay *= 1.5


def main():
    parser = argparse.ArgumentParser(description="Publicador de prueba a AWS IoT Core (versión debug)")
    parser.add_argument("--topic", default=TOPIC_DEF, help="Tópico MQTT")
    parser.add_argument("--count", type=int, default=5, help="Mensajes a enviar")
    parser.add_argument("--qos", type=int, choices=[0, 1], default=1, help="QoS 0/1")
    parser.add_argument("--interval", type=float, default=2.0, help="Segundos entre mensajes")
    parser.add_argument("--device", default=os.getenv("DEVICE_ID", "test-device"), help="ID del dispositivo")
    args = parser.parse_args()

    print("🔍 === AWS IoT Core Debug Publisher ===")
    print(f"📁 Directorio base: {BASE_DIR}")
    print(f"📋 Cliente ID: {CLIENT_ID}")
    print(f"🌐 Endpoint: {AWS_ENDPOINT}")
    print(f"🚪 Puerto: {AWS_PORT}")
    print(f"📝 Tópico: {args.topic}")
    print()

    # Validar endpoint
    if AWS_ENDPOINT == "TU_ENDPOINT_AWS":
        print("❌ ERROR: Configura AWS_IOT_ENDPOINT en el archivo .env")
        print("   Debe ser algo como: xxxxxx-ats.iot.us-east-1.amazonaws.com")
        sys.exit(1)

    # Verificar certificados
    print("🔐 Verificando certificados...")
    try:
        for label, path in [("ROOT_CA", ROOT_CA), ("PRIVATE_KEY", PRIVATE_KEY), ("CERTIFICATE", CERTIFICATE)]:
            assert_file(label, path)
    except FileNotFoundError as e:
        print(f"❌ ERROR: {e}")
        print("\n💡 Verifica que los archivos estén en la carpeta 'Certificado':")
        print(f"   {os.path.join(BASE_DIR, 'Certificado')}")
        sys.exit(1)
    print()

    # Construir cliente
    print("🏗️ Configurando cliente MQTT...")
    try:
        client = build_client(CLIENT_ID)
    except Exception as e:
        print(f"❌ Error configurando cliente: {e}")
        sys.exit(1)

    # Conectar
    print(f"🔌 Conectando a AWS IoT Core...")
    try:
        connect_with_retry(client, attempts=3)
    except Exception:
        print("\n🆘 No se pudo conectar a AWS IoT Core.")
        print("📝 Revisa la configuración en AWS IoT Console:")
        print("   1. Security > Certificates: ¿Está Active?")
        print("   2. Policies: ¿Hay una política adjunta?")
        print("   3. Things: ¿Está asociado al certificado?")
        sys.exit(1)

    # Publicar mensajes
    print(f"📤 Publicando {args.count} mensajes en '{args.topic}'...")
    print()

    try:
        for i in range(args.count):
            payload = {
                "device": args.device,
                "messageId": i + 1,
                "temperature": round(20 + random.uniform(0, 10), 1),
                "humidity": round(50 + random.uniform(0, 20), 1),
                "timestamp": int(time.time()),
                "status": "active"
            }

            message = json.dumps(payload, ensure_ascii=False)
            print(f"📨 [{i + 1}/{args.count}] {message}")

            client.publish(args.topic, message, args.qos)

            if i < args.count - 1:
                time.sleep(args.interval)

        print()
        print("✅ ¡Todos los mensajes enviados exitosamente!")

    except Exception as e:
        print(f"❌ Error durante el envío: {e}")
    finally:
        try:
            print("🔌 Desconectando...")
            client.disconnect()
            print("✅ Desconectado correctamente.")
        except Exception as e:
            print(f"⚠️ Error al desconectar: {e}")


if __name__ == "__main__":
    main()