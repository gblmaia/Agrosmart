import pandas as pd
import numpy as np

def processar_dados_sensores(dados_brutos):
    """
    Carrega os dados brutos recebidos via serial/simulação,
    trata valores nulos, ajusta os tipos de dados e padroniza os nomes das colunas.
    """
    df = pd.DataFrame(dados_brutos)

    # Tratamento de nulos (preenche temperatura ausente pela média)
    if 'temperatura' in df.columns:
        df['temperatura'] = df['temperatura'].fillna(df['temperatura'].mean())

    # Formatação do tipo datetime
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Padronização dos nomes para compatibilidade com o motor de regras
    df = df.rename(columns={
        'temperatura': 'Temperatura_C',
        'umidade_solo': 'Umidade_Solo_Pct',
        'luminosidade': 'Luminosidade_Lux'
    })

    return df


def avaliar_condicoes_lavoura(dataframe):
    """
    Motor de Inferência e Decisão Automática:
    Avalia os parâmetros agronômicos e gera os status operacionais.git
    """
    df_resultado = dataframe.copy()
    status_list = []

    for _, row in df_resultado.iterrows():
        if row['Umidade_Solo_Pct'] < 30.0:
            status = "ALERTA - Solo Seco"
        elif row['Temperatura_C'] > 35.0:
            status = "ALERTA - Calor Excessivo"
        else:
            status = "Normal"

        status_list.append(status)

    df_resultado['Status_Sistema'] = status_list
    return df_resultado


if __name__ == "__main__":
    # Teste de execução isolada do módulo de sensores
    dados_brutos_simulados = [
        {"timestamp": "2026-10-10 12:00:00", "temperatura": 28.5, "umidade_solo": 42.0, "luminosidade": 850},
        {"timestamp": "2026-10-10 12:05:00", "temperatura": 31.0, "umidade_solo": 28.0, "luminosidade": 920},
        {"timestamp": "2026-10-10 12:10:00", "temperatura": None, "umidade_solo": 35.0, "luminosidade": 880},
        {"timestamp": "2026-10-10 12:15:00", "temperatura": 29.2, "umidade_solo": 40.5, "luminosidade": 810}
    ]

    print("=== DADOS BRUTOS ===")
    print(pd.DataFrame(dados_brutos_simulados))

    df_limpo = processar_dados_sensores(dados_brutos_simulados)
    df_alertas = avaliar_condicoes_lavoura(df_limpo)

    print("\n=== RELATÓRIO DO MOTOR DE INFERÊNCIA ===")
    print(df_alertas[['timestamp', 'Temperatura_C', 'Umidade_Solo_Pct', 'Luminosidade_Lux', 'Status_Sistema']])