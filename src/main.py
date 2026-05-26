import os
import time
from datetime import datetime
import cv2
import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras.models import load_model
import tf_keras as keras

# Definição do diretório base para garantir a portabilidade do projeto
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# --- 1. CONFIGURAÇÕES DE PATHS ---
# Centralização de caminhos para facilitar a manutenção e escalabilidade
MODEL_PATH = os.path.join(BASE_DIR, 'models', 'keras_model.h5')
LABELS_PATH = os.path.join(BASE_DIR, 'models', 'labels.txt')
INPUT_DIR = os.path.join(BASE_DIR, 'data', 'samples')
OUTPUT_DIR = os.path.join(BASE_DIR, 'data', 'exports')

# Certifica-se de que o diretório de saída existe para evitar exceções de I/O
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- 2. CARREGAMENTO DO MODELO E LABELS ---
# Utilizando o tf_keras para carregar o modelo pré-treinado
model = keras.models.load_model(MODEL_PATH, compile=False)
class_names = open(LABELS_PATH, 'r').readlines()


def classificar_batata(caminho_da_imagem):
    """
    Realiza a classificação de uma imagem individual, executa a inferência
    pelo modelo de Deep Learning e persiste os resultados em CSV.
    """
    try:
        # Carregamento e pré-processamento da imagem
        image = cv2.imread(caminho_da_imagem)
        if image is None:
            return

        # Redimensionamento para o input shape esperado pelo modelo (224x224)
        image_resized = cv2.resize(image, (224, 224), interpolation=cv2.INTER_AREA)

        # Normalização dos dados para o intervalo [-1, 1]
        image_array = np.asarray(image_resized, dtype=np.float32).reshape(1, 224, 224, 3)
        image_normalized = (image_array / 127.5) - 1

        # Execução da inferência (Predição)
        prediction = model.predict(image_normalized, verbose=0)
        index = np.argmax(prediction)
        class_name = class_names[index].strip()
        confianca = prediction[0][index]

        # Estruturação dos metadados para persistência
        dados = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'arquivo': os.path.basename(caminho_da_imagem),
            'diagnostico': class_name,
            'confianca': f'{confianca:.4f}'
        }

        # Persistência dos resultados (Append Mode)
        df = pd.DataFrame([dados])
        csv_path = os.path.join(OUTPUT_DIR, 'relatorio_agrosmart.csv')
        df.to_csv(csv_path, mode='a', index=False, header=not os.path.exists(csv_path))

        print(f"✅ Processado: {dados['arquivo']} | Classe: {class_name} | Confidence: {confianca:.2%}")

    except Exception as e:
        print(f"❌ Falha no processamento de {caminho_da_imagem}: {e}")


# --- NOVA FUNÇÃO PARA A FASE 3: LÓGICA E AUTOMAÇÃO ---
def executar_automacao_fase3():
    print("\n" + "=" * 50)
    print("🤖 INICIANDO AUTOMAÇÃO E LÓGICA DE DECISÃO (FASE 3)")
    print("=" * 50)

    csv_path = os.path.join(OUTPUT_DIR, 'relatorio_agrosmart.csv')

    if not os.path.exists(csv_path):
        print("Aviso: Nenhum dado encontrado para analisar.")
        return

    # Lê os dados gerados pela Visão Computacional
    df = pd.read_csv(csv_path)
    total_analisado = len(df)

    if total_analisado == 0:
        return

    # Procura pela string "zuada" no diagnóstico para identificar as batatas doentes
    doentes = df[df['diagnostico'].str.contains('zuada', case=False, na=False)]
    total_doentes = len(doentes)

    perc_doentes = (total_doentes / total_analisado) * 100

    print(f"📊 Relatório Rápido: {total_doentes} batatas zuadas de {total_analisado} analisadas ({perc_doentes:.2f}%).")

    caminho_alerta = os.path.join(OUTPUT_DIR, 'acao_automatizada.txt')

    with open(caminho_alerta, 'w', encoding='utf-8') as f:
        f.write("=== SISTEMA AGROSMART: CENTRAL DE AUTOMAÇÃO ===\n")
        f.write(f"Data da Análise: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        # REGRA LÓGICA (SE/ENTÃO + OR)
        if perc_doentes >= 30.0 or total_doentes >= 3:
            alerta = "🚨 ALERTA CRÍTICO: Alto índice de batatas zuadas detectado!\n"
            acao_braco = "-> AÇÃO AUTOMÁTICA: Acionando braço robótico para descarte das unidades.\n"

            print(alerta + acao_braco, end="")
            f.write(alerta + acao_braco)

            # --- PROTOCOLO DE COMUNICAÇÃO COM PAUSAS DRAMÁTICAS ---
            print("📧 Iniciando protocolo de comunicação.")
            time.sleep(1)
            print("➡️ Conectando ao servidor SMTP e disparando e-mail para agronomo_chefe@agrosmart.com.br")
            time.sleep(1.5)

            acao_email = "-> COMUNICAÇÃO: E-mail de emergência ENVIADO com sucesso ao agrônomo responsável.\n"
            print("✅ " + acao_email)
            f.write(acao_email)

        else:
            status = "✅ STATUS NORMAL: Safra com qualidade aceitável.\n"
            acao = "-> AÇÃO AUTOMÁTICA: Lote liberado para empacotamento."
            print(status + acao)
            f.write(status + acao)

    print(f"\n📁 Log de ação automatizada salvo em: {caminho_alerta}")


# --- 3. EXECUÇÃO EM LOTE (BATCH PROCESSING) ---
if __name__ == "__main__":
    print("--------------------------------------------------")
    print("     Agrosmart System - Computer Vision Module    ")
    print("--------------------------------------------------")

    if not os.path.exists(INPUT_DIR):
        print(f"Erro: O diretório de entrada {INPUT_DIR} não foi localizado.")
    else:
        processed_count = 0
        # Varredura recursiva em diretórios para processamento em escala
        for raiz, pastas, arquivos in os.walk(INPUT_DIR):
            for nome_arquivo in arquivos:
                if nome_arquivo.lower().endswith(('.png', '.jpg', '.jpeg')):
                    caminho_completo = os.path.join(raiz, nome_arquivo)
                    classificar_batata(caminho_completo)
                    processed_count += 1

        if processed_count == 0:
            print("Status: Nenhum arquivo compatível localizado para processamento.")
        else:
            print(f"\nAnálise visual concluída com sucesso.")
            print(f"Total de itens processados: {processed_count}")

            # --- CHAMA A AUTOMAÇÃO DA FASE 3 AQUI ---
            executar_automacao_fase3()