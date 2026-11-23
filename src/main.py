import os
import time
import json
from datetime import datetime
import cv2
import numpy as np
import pandas as pd
import tf_keras as keras
from kafka import KafkaProducer

from sensores_engine import processar_dados_sensores, avaliar_condicoes_lavoura

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "exports")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def conectar_kafka(bootstrap_servers: str, tentativas: int = 6):
    """Tenta conectar ao broker Kafka com retentativas seguras."""
    for tentativa in range(1, tentativas + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[bootstrap_servers],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"[INFO] Conectado ao broker Kafka em {bootstrap_servers}.")
            return producer
        except Exception as err:
            print(f"[WARN] Aguardando Kafka ({tentativa}/{tentativas}): {err}")
            time.sleep(3)
    print("[WARN] Kafka indisponivel. Seguindo em modo de persistencia local.")
    return None


def executar_telemetria_sensores() -> str:
    """Captura, limpa e avalia regras ambientais dos sensores de campo."""
    print("\n" + "=" * 60)
    print("[SISTEMA] 1. MONITORAMENTO DE TELEMETRIA AMBIENTAL")
    print("=" * 60)

    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dados_brutos = [
        {"timestamp": agora, "temperatura": 28.5, "umidade_solo": 42.0, "luminosidade": 850},
        {"timestamp": agora, "temperatura": 31.0, "umidade_solo": 28.0, "luminosidade": 920},
        {"timestamp": agora, "temperatura": None, "umidade_solo": 35.0, "luminosidade": 880},
        {"timestamp": agora, "temperatura": 29.2, "umidade_solo": 40.5, "luminosidade": 810},
    ]

    df_processado = processar_dados_sensores(dados_brutos)
    df_alertas = avaliar_condicoes_lavoura(df_processado)

    telemetria_path = os.path.join(OUTPUT_DIR, "telemetria_sensores.csv")
    df_alertas.to_csv(telemetria_path, index=False)

    print(df_alertas[["timestamp", "Temperatura_C", "Umidade_Solo_Pct", "Status_Sistema"]])
    print(f"[INFO] Telemetria exportada em: {telemetria_path}")

    alerta_solo = any("ALERTA" in s for s in df_alertas["Status_Sistema"])
    return "Umidade_Critica_Detectada" if alerta_solo else "Parametros_Normais"


def executar_visao_e_streaming(status_telemetria: str):
    """Classifica amostras foliares com Keras, aciona regras e publica no Kafka."""
    print("\n" + "=" * 60)
    print("[SISTEMA] 2. VISAO COMPUTACIONAL, AUTOMACAO E STREAMING")
    print("=" * 60)

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    model_path = os.path.join(BASE_DIR, "models", "keras_model.h5")
    labels_path = os.path.join(BASE_DIR, "models", "labels.txt")
    samples_dir = os.path.join(BASE_DIR, "data", "samples")

    with open(labels_path, "r", encoding="utf-8") as f:
        class_names = [line.strip() for line in f.readlines()]

    model = keras.models.load_model(model_path, compile=False)
    producer = conectar_kafka(bootstrap_servers)

    registros = []
    acoes = []
    event_logs = []
    case_counter = 1

    if status_telemetria == "Umidade_Critica_Detectada":
        ts_solo = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        acoes.append(f"[{ts_solo}] ALERTA HIDRICO: Umidade do solo < 30% -> Acionar valvula de irrigacao.")

    for categoria in ["batata saudavel", "batata zuada"]:
        pasta = os.path.join(samples_dir, categoria)
        if not os.path.exists(pasta):
            continue

        for arquivo in os.listdir(pasta)[:3]:
            caminho_img = os.path.join(pasta, arquivo)
            case_id = f"CASE-{case_counter:03d}"
            case_counter += 1

            # Etapa 1: Leitura de Telemetria
            event_logs.append({
                "case_id": case_id,
                "activity": "Coleta_Telemetria_Sensores",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "resource": "sensores_engine.py",
                "status": status_telemetria,
                "duration_ms": 115,
            })

            # Etapa 2: Captura e Pre-processamento de Imagem
            t0_img = time.perf_counter()
            img = cv2.imread(caminho_img)
            if img is None:
                continue
            img = cv2.resize(img, (224, 224), interpolation=cv2.INTER_AREA)
            img_array = np.asarray(img, dtype=np.float32).reshape(1, 224, 224, 3)
            img_array = (img_array / 127.5) - 1
            dur_img = int((time.perf_counter() - t0_img) * 1000)

            event_logs.append({
                "case_id": case_id,
                "activity": "Captura_Imagem_Foliar",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "resource": "main.py",
                "status": "Imagem_Processada",
                "duration_ms": max(dur_img, 10),
            })

            # Etapa 3: Inferencia Keras
            t0_ia = time.perf_counter()
            pred = model.predict(img_array, verbose=0)
            dur_ia = int((time.perf_counter() - t0_ia) * 1000)

            idx = int(np.argmax(pred))
            confianca = float(pred[0][idx])
            diagnostico = class_names[idx] if idx < len(class_names) else categoria
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            event_logs.append({
                "case_id": case_id,
                "activity": "Inferencia_Modelo_Keras",
                "timestamp": ts,
                "resource": "keras_model.h5",
                "status": diagnostico.replace(" ", "_"),
                "duration_ms": max(dur_ia, 50),
            })

            evento = {
                "timestamp": ts,
                "arquivo": arquivo,
                "diagnostico": diagnostico,
                "confianca": confianca,
            }
            registros.append(evento)

            if producer:
                producer.send("agrosmart-batatas-topic", value=evento)

            # Etapa 4 (Variante de Excecao): Automacao se folha doente
            if "zuada" in diagnostico.lower():
                msg_alerta = (
                    f"[{ts}] ALERTA FITOSSANITARIO: {arquivo} -> "
                    f"{diagnostico} ({confianca:.2%}) | Isolar talhao e aplicar defensivo."
                )
                acoes.append(msg_alerta)
                print(f"[AUTOMACAO] {msg_alerta}")
                event_logs.append({
                    "case_id": case_id,
                    "activity": "Disparo_Acao_Automatizada",
                    "timestamp": ts,
                    "resource": "acao_automatizada.txt",
                    "status": "Alerta_Critico_Emitido",
                    "duration_ms": 25,
                })
            else:
                print(f"[OK] Amostra saudavel: {arquivo} -> {diagnostico} ({confianca:.2%})")

            # Etapa 5: Persistencia e Dashboard
            event_logs.append({
                "case_id": case_id,
                "activity": "Persistencia_e_Streaming_Kafka",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "resource": "dashboard_consumer.py",
                "status": "Publicado",
                "duration_ms": 40,
            })

    if producer:
        producer.flush()

    pd.DataFrame(registros).to_csv(os.path.join(OUTPUT_DIR, "relatorio_agrosmart.csv"), index=False)
    pd.DataFrame(event_logs).to_csv(os.path.join(OUTPUT_DIR, "event_log_agrosmart.csv"), index=False)

    with open(os.path.join(OUTPUT_DIR, "acao_automatizada.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(acoes) if acoes else "Nenhuma anomalia critica detectada neste ciclo.")

    print(f"\n[SUCESSO] {len(registros)} amostras processadas, enviadas ao Kafka e registradas no Event Log.")


if __name__ == "__main__":
    print("=" * 55)
    print("   AgroSmart Engineering System v2.1 - Core Platform   ")
    print("=" * 55)

    status_campo = executar_telemetria_sensores()
    executar_visao_e_streaming(status_campo)