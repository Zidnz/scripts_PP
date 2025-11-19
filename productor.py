import json
import time
import random
import datetime
from faker import Faker
from kafka import KafkaProducer

fake = Faker('es_ES')

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

TOPIC_NAME = 'transacciones_bancarias'
print("📡 Enviando transacciones SIN etiqueta de fraude... (Ctrl+C para detener)")

try:
    while True:
        cliente_id = fake.random_int(min=1000, max=9999)
        fecha_reg = fake.past_datetime(start_date="-5y")
        cuenta_id = fake.random_int(min=10000, max=99999)

        # Generar monto realista (sin lógica de fraude)
        monto = round(random.uniform(5.0, 5000.0), 2)

        transaccion = {
            'id_transaccion': fake.random_int(min=1000000, max=9999999),
            'fecha_hora': datetime.datetime.now().isoformat(),
            'monto': monto,
            'tipo': random.choice(['compra', 'retiro', 'transferencia', 'deposito']),
            # ⚠ NO incluimos 'es_fraude' ← esto lo decide el modelo
            'ubicacion': fake.city(),
            'dispositivo': random.choice(['movil_ios', 'movil_android', 'web_chrome', 'cajero_automatico']),
            'id_cuenta': cuenta_id,
            'tipo_cuenta': random.choice(['ahorro', 'corriente', 'nomina']),
            'saldo_actual': round(random.uniform(100.0, 50000.0), 2),
            'estatus': random.choice(['aprobada', 'declinada']),
            'id_cliente': cliente_id,
            'nombre': fake.name(),
            'correo': fake.email(),
            'telefono': int(fake.msisdn()[:10]),
            'fecha_registro': fecha_reg.isoformat(),
            'nivel_riesgo': random.choice(['bajo', 'medio', 'alto'])
        }

        producer.send(TOPIC_NAME, transaccion)
        print(f"Enviado: {transaccion['tipo']} ${transaccion['monto']:.2f}")
        time.sleep(1)

except KeyboardInterrupt:
    print("\n🛑 Deteniendo productor...")
finally:
    producer.flush()
    producer.close()