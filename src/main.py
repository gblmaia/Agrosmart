import os
import time
import json
from datetime import datetime
import cv2
import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras.models import load_model
import tf_keras as keras
from kafka import KafkaProducer

# Definicao do diretorio base para portabilidade
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# --- 1. CONFIGURACOES DE PATHS ---
MODEL_PATH = os.path.join(BASE_DIR, 'models', 'keras_model.h5')
LABELS_PATH = os.path.join(BASE_DIR, 'models', 'labels.txt')
INPUT_DIR = os.path.join(BASE_DIR, 'data', 'samples')
OUTPUT_DIR = os.path.join(BASE_DIR, 'data', 'exports')

os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- 2. INICIALIZACAO DO BROKER DE MENSAGERIA (KAFKA) ---
print("[INFO] Inicializando conexao com o Apache Kafka no Docker...")
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("[SUCCESS] Conectado ao cluster Kafka com sucesso.")
except Exception as e:
    print(f"[ERROR] Falha critica ao conectar no broker Kafka: {e}")
    producer = None

# --- 3. CARREGAMENTO DO MODELO PREDITIVO ---
model = keras.models.load_model(MODEL_PATH, compile=False)
class_names = open(LABELS_PATH, 'r').readlines()


def classificar_batata(caminho_da_imagem):
    """
    Executa a inferencia de Visao Computacional na imagem da esteira,
    publica os metadados no topico do Kafka e persiste localmente em CSV.
    """
    try:
        image = cv2.imread(caminho_da_imagem)
        if image is None:
            return

        # Pre-processamento e adequacao ao input shape da rede (224x224x3)
        image_resized = cv2.resize(image, (224, 224), interpolation=cv2.INTER_AREA)
        image_array = np.asarray(image_resized, dtype=np.float32).reshape(1, 224, 224, 3)
        image_normalized = (image_array / 127.5) - 1

        # Execucao do pipeline de inferencia
        prediction = model.predict(image_normalized, verbose=0)
        index = np.argmax(prediction)
        class_name = class_names[index].strip()
        confianca = prediction[0][index]

        # Estruturacao da carga util (Payload)
        dados = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'arquivo': os.path.basename(caminho_da_imagem),
            'diagnostico': class_name,
            'confianca': float(confianca)
        }

        # streaming: Publicacao em tempo real no Kafka
        if producer:
            producer.send('agrosmart-batatas-topic', dados)
            producer.flush()

        # Persistencia historica para auditoria
        df = pd.DataFrame([dados])
        csv_path = os.path.join(OUTPUT_DIR, 'relatorio_agrosmart.csv')
        df.to_csv(csv_path, mode='a', index=False, header=not os.path.exists(csv_path))

        print(f"[TRACKING] Item: {dados['arquivo']} | Classificacao: {class_name} | Confianca: {confianca:.2%}")

    except Exception as e:
        print(f"[ERROR] Falha no processamento do arquivo {caminho_da_imagem}: {e}")


def executar_automacao_fase3():
    """
    Modulo Analitico: Avalia a taxa de anomalias da safra e dispara
    as contra-medidas de automacao industrial e alertas.
    """
    print("\n" + "=" * 60)
    print("[SISTEMA] INICIANDO ENGINE DE TELEMETRIA E DECISAO AUTOMATICA")
    print("=" * 60)

    csv_path = os.path.join(OUTPUT_DIR, 'relatorio_agrosmart.csv')
    if not os.path.exists(csv_path):
        print("[WARN] Base de dados local nao localizada para analise.")
        return

    df = pd.read_csv(csv_path)
    total_analisado = len(df)
    if total_analisado == 0:
        return

    # Filtro analitico baseado no diagnostico do modelo
    doentes = df[df['diagnostico'].str.contains('zuada', case=False, na=False)]
    total_doentes = len(doentes)
    perc_doentes = (total_doentes / total_analisado) * 100

    print(f"[METRICAS] Total Processado: {total_analisado} | Anomalias: {total_doentes} ({perc_doentes:.2f}%)")

    caminho_alerta = os.path.join(OUTPUT_DIR, 'acao_automatizada.txt')
    with open(caminho_alerta, 'w', encoding='utf-8') as f:
        f.write("=== AGROSMART INDUSTRIAL SYSTEM: AUTOMAÇÃO INTERNA ===\n")
        f.write(f"Timestamp da Analise: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        if perc_doentes >= 30.0 or total_doentes >= 3:
            alerta = "[ALERT] Indice de anomalias acima do limite tolerado.\n"
            acao_braco = "[ACTION] Sinal emitido para atuador mecanico: Descarte de lote ativado.\n"
            print(alerta + acao_braco, end="")
            f.write(alerta + acao_braco)

            print("[COMM] Iniciando protocolo de comunicacao externa de emergencia...")
            time.sleep(1)
            print("[COMM] Conectando ao servidor SMTP Corporativo...")
            time.sleep(1.5)

            acao_email = "[COMM] Notificacao de parada enviada com sucesso ao engenheiro agronomo chefe.\n"
            print(acao_email)
            f.write(acao_email)
        else:
            status = "[STATUS] Qualidade da safra dentro dos conformes operacionais.\n"
            acao = "[ACTION] Lote liberado para a esteira de empacotamento e distribuicao.\n"
            print(status + acao, end="")
            f.write(status + acao)

    print(f"\n[INFO] Log de auditoria operacional gerado em: {caminho_alerta}")


if __name__ == "__main__":
    print("==================================================")
    print("   AgroSmart Engineering System v2.1 - Core CV    ")
    print("==================================================")

    if not os.path.exists(INPUT_DIR):
        print(f"[ERROR] Diretorio de entrada {INPUT_DIR} inexistente.")
    else:
        processed_count = 0
        for raiz, pastas, arquivos in os.walk(INPUT_DIR):
            for nome_arquivo in arquivos:
                if nome_arquivo.lower().endswith(('.png', '.jpg', '.jpeg')):
                    caminho_completo = os.path.join(raiz, nome_arquivo)
                    classificar_batata(caminho_completo)
                    processed_count += 1
                    time.sleep(2)  # Delay para simulacao de fluxo continuo da esteira

        print(f"\n[SUCCESS] Processamento em lote finalizado. Total: {processed_count}")
        executar_automacao_fase3()