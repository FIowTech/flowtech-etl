from pathlib import Path
import pandas as pd
import boto3
import os
from dotenv import load_dotenv
from datetime import datetime
from io import StringIO
import mysql.connector
import json

load_dotenv()

conexao = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME"),
)

cursor = conexao.cursor(dictionary=True)

atual = datetime.now()
data_ano = atual.strftime("%Y")
data_mes = atual.strftime("%m")
data_dia = atual.strftime("%d")
data_hora = atual.strftime("%H")
data_minuto = atual.strftime("%M")

s3 = boto3.client("s3")

bucket = os.getenv("BUCKET_NAME")
silver_prefixo = (
    f"02-silver/ano={data_ano}/mes={data_mes}/dia={data_dia}/hora={data_hora}/"
)

mapeamento_componentes = {
    "cpu_uso": "cpu_uso_pct",
    "cpu_freq": "cpu_freq_mhz",
    "ram_uso": "ram_uso_pct",
    "swap_uso": "swap_uso_pct",
    "disco_uso": "disco_uso_pct",
    "rede_download": "download_mb",
    "rede_upload": "upload_mb",
    "rede_latencia": "latencia_ms",
}

grupos = {
    "cpu": ["cpu_uso_pct", "cpu_freq_mhz"],
    "ram": ["ram_uso_pct", "swap_uso_pct"],
    "disco": ["disco_uso_pct"],
    "rede": ["download_mb", "upload_mb", "latencia_ms", "packet_loss"],
}

def buscar_componentes_do_embarcado(mac_geral):

    mac_geral_formatado = ""
    cont = 0

    for letrinha in mac_geral:
        if cont == 2:
            mac_geral_formatado += ":"
            cont = 0
        mac_geral_formatado += letrinha

        cont += 1

    comando = """
        SELECT DISTINCT
            c.nome AS componente,
            p.limite_min,
            p.limite_max
        FROM parametro p
        JOIN embarcado e
            ON e.id_embarcado = p.embarcado_id
           AND e.empresa_id = p.empresa_id
        JOIN componente c
            ON c.id_componente = p.componente_id
        WHERE e.endereco_mac = %s
          AND e.status = 1
          AND p.status = 1;
    """

    cursor.execute(comando, (mac_geral_formatado,))
    return cursor.fetchall()

def verificar_limites(mac_geral, df):
    componentes = buscar_componentes_do_embarcado(mac_geral)

    for item in componentes:
        componente_banco = item["componente"]
        componente = mapeamento_componentes.get(componente_banco)

        if componente is None:
            continue

        if componente not in df.columns:
            continue

        limite_min = item["limite_min"]
        limite_max = item["limite_max"]

        df[f"lim_min_{componente}"] = limite_min
        df[f"lim_max_{componente}"] = limite_max

        if limite_min is not None and limite_max is not None:
            df[f"estado_{componente}"] = (df[componente] >= limite_min) & (
                df[componente] <= limite_max
            )
        else:
            df[f"estado_{componente}"] = None

    return df


def separar_componentes(mac_geral, df):
    colunas_componentes = [
        coluna
        for coluna in df.columns
        if not coluna.startswith(("lim_min_", "lim_max_", "estado_"))
        and coluna not in ("endereco_mac", "timestamp")
    ]

    for _, linha in df.iterrows():
        for nome_grupo, componentes_do_grupo in grupos.items():
            parametros = []

            for componente in componentes_do_grupo:
                if componente not in colunas_componentes:
                    continue

                if f"estado_{componente}" not in df.columns:
                    continue

                parametros.append({
                    "nome": componente,
                    "valor": linha[componente],
                    "estado": linha[f"estado_{componente}"],
                    "limite_min": float(linha[f"lim_min_{componente}"])
                    if linha[f"lim_min_{componente}"] is not None
                    else None,

                    "limite_max": float(linha[f"lim_max_{componente}"])
                    if linha[f"lim_max_{componente}"] is not None
                    else None,
                })

            if not parametros:
                continue

            componente_json = {
                "nome": nome_grupo,
                "mac_address": mac_geral,
                "timestamp": linha["timestamp"],
                "parametros": parametros,
            }

            enviar_json(componente_json)


def enviar_json(componente):
    nome = componente["nome"]
    mac = componente["mac_address"]

    gold_prefixo = (
        f"03-gold/componente={nome}/"
        f"ano={data_ano}/mes={data_mes}/"
        f"dia={data_dia}/hora={data_hora}/"
    )

    chave = (
        f"{gold_prefixo}{mac}_{data_hora}{data_minuto}"
        f"_{nome}.json"
    )

    corpo = json.dumps(
        componente,
        ensure_ascii=False,
        indent=2,
        allow_nan=False,
        default=float,
    ).encode("utf-8")

    s3.put_object(
        Bucket=bucket,
        Key=chave,
        Body=corpo,
        ContentType="application/json",
    )

    print(f"JSON enviado: {chave}")


def processar_csv(dataframes_por_mac):
    for mac_geral, df in dataframes_por_mac.items():
        df = verificar_limites(mac_geral, df)
        separar_componentes(mac_geral, df)


def carregar_csv():
    ultima_leitura = s3.list_objects_v2(Bucket=bucket, Prefix=silver_prefixo)
    if "Contents" not in ultima_leitura:
        print("Nenhum arquivo encontrado na Silver para esta hora.")
        return

    csv_keys = []
    for obj in ultima_leitura["Contents"]:
        nome_csv = obj["Key"]
        if obj["Key"].lower().endswith(".csv"):
            csv_keys.append(nome_csv)

    dataframes_por_mac = {}

    for key in sorted(csv_keys):
        obj = s3.get_object(Bucket=bucket, Key=key)
        nome_arquivo = Path(key).name
        nome_sem_extensao = Path(nome_arquivo).stem
        mac_geral = nome_sem_extensao.split("_", 1)[0]

        df_parcial = pd.read_csv(obj["Body"])
        dataframes_por_mac[mac_geral] = df_parcial
    processar_csv(dataframes_por_mac)


carregar_csv()
