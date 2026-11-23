import os
import json
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

print("==================================================")
print("     AgroSmart Real-Time Monitoring Dashboard     ")
print("==================================================")
print(f"[INFO] Conectando ao broker Kafka em {BOOTSTRAP_SERVERS}...")

try:
    consumer = KafkaConsumer(
        'agrosmart-batatas-topic',
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for mensagem in consumer:
        dados = mensagem.value
        status = "[CRITICAL]" if "zuada" in dados['diagnostico'].lower() else "[SUCCESS]"
        print(f"[{dados['timestamp']}] {status} Telemetria recebida -> Arquivo: {dados['arquivo']} | Diagnostico: {dados['diagnostico']} | Acuracia: {dados['confianca']:.2%}")

except Exception as e:
    print(f"[ERROR] Erro na execucao do consumidor de dados: {e}")