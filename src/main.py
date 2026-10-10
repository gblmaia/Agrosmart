import os
import time
import json
from datetime import datetime
import cv2
import numpy as np
import pandas as pd
import tf_keras as keras
from kafka import KafkaProducer

# Importação do novo módulo de sensores
from sensores_engine import processar_dados_sensores, avaliar_condicoes_lavoura

# (Mantenha as configurações originais de PATHS e KAFKA aqui...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, 'data', 'exports')

def executar_telemetria_sensores():
    """
    Executa a captura, limpeza e análise de regras dos sensores de campo.
    """
    print("\n" + "=" * 60)
    print("[SISTEMA] INICIANDO MONITORAMENTO DE TELEMETRIA DE CAMPO")
    print("=" * 60)

    # Dados simulados provenientes da porta serial / IoT
    dados_brutos = [
        {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "temperatura": 28.5, "umidade_solo": 42.0, "luminosidade": 850},
        {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "temperatura": 31.0, "umidade_solo": 28.0, "luminosidade": 920},
        {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "temperatura": None, "umidade_solo": 35.0, "luminosidade": 880},
        {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "temperatura": 29.2, "umidade_solo": 40.5, "luminosidade": 810}
    ]

    df_processado = processar_dados_sensores(dados_brutos)
    df_alertas = avaliar_condicoes_lavoura(df_processado)

    # Exportação dos resultados de telemetria
    telemetria_path = os.path.join(OUTPUT_DIR, 'telemetria_sensores.csv')
    df_alertas.to_csv(telemetria_path, index=False)

    print(df_alertas[['timestamp', 'Temperatura_C', 'Umidade_Solo_Pct', 'Status_Sistema']])
    print(f"[INFO] Relatório de telemetria atualizado em: {telemetria_path}")


if __name__ == "__main__":
    print("==================================================")
    print("   AgroSmart Engineering System v2.1 - Core System ")
    print("==================================================")

    # 1. Executa telemetria do solo e ambiente
    executar_telemetria_sensores()

    # 2. Executa a análise de visão computacional da esteira (se houver imagens)
    # (suas chamadas originais para classificar_batata e executar_automacao_fase3)