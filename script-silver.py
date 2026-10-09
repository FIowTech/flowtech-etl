from pathlib import Path
import pandas as pd
import boto3
import os
from dotenv import load_dotenv
from datetime import datetime
from io import StringIO

load_dotenv()

# definição data e hora
atual = datetime.now()
data_ano = atual.strftime("%Y")
data_mes = atual.strftime("%m")
data_dia = atual.strftime("%d")
data_hora = atual.strftime("%H")

s3 = boto3.client("s3")

bucket = os.getenv("BUCKET_NAME")
bronze_prefixo = f"bronze/ano={data_ano}/mes={data_mes}/dia={data_dia}/hora={data_hora}/"

ultima_leitura = s3.list_objects_v2(Bucket=bucket, Prefix=bronze_prefixo)

csv_keys = []
for obj in ultima_leitura["Contents"]:
    nome_csv = obj["Key"]
    if obj["Key"].lower().endswith(".csv"):
        csv_keys.append(nome_csv)

dataframes_por_mac = {}

for key in csv_keys:
    obj = s3.get_object(Bucket=bucket, Key=key)
    nome_arquivo = Path(key).name
    nome_sem_extensao = Path(nome_arquivo).stem
    mac_geral = nome_sem_extensao.split("_", 1)[0]

    df_parcial = pd.read_csv(obj["Body"])
    dataframes_por_mac.setdefault(mac_geral, []).append(df_parcial)

for mac_geral, dataframes in dataframes_por_mac.items():
    df = pd.concat(dataframes, ignore_index=True)

    # limpar nulos
    df = df.dropna(axis=1, how="all")

    # unidades de medida
    df["cpu_freq"] = df["cpu_freq"] / 10**3
    df["ram_uso"] = df["ram_uso"] / 10**9
    df["ram_total"] = df["ram_total"] / 10**9
    df["swap_uso"] = df["swap_uso"] / 10**9
    df["swap_total"] = df["swap_total"] / 10**9
    df["disco_uso"] = df["disco_uso"] / 10**9
    df["disco_total"] = df["disco_total"] / 10**9
    df["download"] = df["download"] / 10**6
    df["upload"] = df["upload"] / 10**6

    # renomeação de colunas
    df = df.rename(columns={
            "cpu_freq": "cpu_freq_mhz", "ram_uso": "ram_uso_gb", "ram_total": "ram_total_gb", "swap_uso": "swap_uso_gb", "swap_total": "swap_total_gb",
            "disco_uso": "disco_uso_gb", "disco_total": "disco_total_gb", "download": "download_mb", "upload": "upload_mb"
    })

    # porcentagens
    df["ram_uso_pct"] = df["ram_uso_gb"] / df["ram_total_gb"] * 100
    df["swap_uso_pct"] = df["swap_uso_gb"] / df["swap_total_gb"] * 100
    df["disco_uso_pct"] = df["disco_uso_gb"] / df["disco_total_gb"] * 100
    df["package_loss"] = df["package_loss_received"] - df["package_loss_sent"]

    # ordenação de colunas
    ordenacao = [
            "endereco_mac", "timestamp", "cpu_uso_pct", "qtd_cpu_fisica", "qtd_cpu_logica", "cpu_freq_mhz", "load_avg_1m", "load_avg_5m", "load_avg_15m",
            "ram_uso_pct", "ram_uso_gb", "ram_total_gb", "swap_uso_pct", "swap_uso_gb", "swap_total_gb", "disco_uso_pct", "disco_uso_gb", "disco_total_gb",
            "qtd_processos", "maior_processo_cpu", "maior_processo_ram", "download_mb", "upload_mb", "latencia_ms", "package_loss", "package_loss_sent",
            "package_loss_received"
    ]

    df = df[ordenacao]

    print("CSV tratado com sucesso.")

    # df para csv
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # envio para o silver
    object_key = f"silver/ano={data_ano}/mes={data_mes}/dia={data_dia}/hora={data_hora}/{mac_geral}_{data_hora}_tratamento.csv"
    s3.put_object(
        Bucket=bucket,
        Key=object_key,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv"
    )

    print(f"Arquivo enviado para o bucket {bucket} com sucesso.")