import json
from kafka import KafkaConsumer

print("==================================================")
print("     AgroSmart Real-Time Monitoring Dashboard     ")
print("==================================================")
print("[INFO] Aguardando transmissao de dados via streaming...")

try:
    consumer = KafkaConsumer(
        'agrosmart-batatas-topic',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for mensagem in consumer:
        dados = mensagem.value
        status = "[CRITICAL]" if "zuada" in dados['diagnostico'].lower() else "[SUCCESS]"
        print(f"[{dados['timestamp']}] {status} Telemetria recebida -> Arquivo: {dados['arquivo']} | Acuracia: {dados['confianca']:.2%}")

except Exception as e:
    print(f"[ERROR] Erro na execucao do consumidor de dados: {e}")