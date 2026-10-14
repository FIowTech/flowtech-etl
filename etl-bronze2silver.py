from pathlib import Path
import pandas as pd
import boto3
import os
from dotenv import load_dotenv
from datetime import datetime
from io import StringIO

load_dotenv()

atual = datetime.now()
data_ano = atual.strftime("%Y")
data_mes = atual.strftime("%m")
data_dia = atual.strftime("%d")
data_hora = atual.strftime("%H")
data_minuto = atual.strftime("%M")

s3 = boto3.client("s3")

bucket = os.getenv("BUCKET_NAME")
bronze_prefixo = (
    f"01-bronze/ano={data_ano}/mes={data_mes}/dia={data_dia}/hora={data_hora}/"
)

def processar_csv(dataframes_por_mac):
    for mac_geral, df in dataframes_por_mac.items():
        df = limpar_valores(df)
        df = padronizar_colunas(df)
        df = remover_duplicatas(df)
        df = converter_unidades(df)
        df = renomear_colunas(df)
        df = calcular_percentuais(df)
        df = calcular_perda_pacotes(df)
        df = ordenar_colunas(df)
        df = remover_colunas_vazias(df)

        enviar_csv(df, mac_geral)

def limpar_valores(df):
    df = df.map(lambda valor: valor.strip() if isinstance(valor, str) else valor)

    return df.replace(r"^\s*$", pd.NA, regex=True)

def padronizar_colunas(df):
    df = df.copy()

    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )

    return df

def remover_duplicatas(df):
    return df.drop_duplicates()

def remover_colunas_vazias(df):
    return df.dropna(axis=1, how="all")

def converter_unidades(df):
    df["cpu_freq"] = df["cpu_freq"] / 10**3
    df["ram_uso"] = df["ram_uso"] / 10**9
    df["ram_total"] = df["ram_total"] / 10**9
    df["swap_uso"] = df["swap_uso"] / 10**9
    df["swap_total"] = df["swap_total"] / 10**9
    df["disco_uso"] = df["disco_uso"] / 10**9
    df["disco_total"] = df["disco_total"] / 10**9
    df["download"] = df["download"] / 10**6
    df["upload"] = df["upload"] / 10**6

    return df

def renomear_colunas(df):
    nomes = {
        "cpu_freq": "cpu_freq_mhz",
        "ram_uso": "ram_uso_gb",
        "ram_total": "ram_total_gb",
        "swap_uso": "swap_uso_gb",
        "swap_total": "swap_total_gb",
        "disco_uso": "disco_uso_gb",
        "disco_total": "disco_total_gb",
        "download": "download_mb",
        "upload": "upload_mb",
    }

    return df.rename(columns=nomes)

def calcular_percentuais(df):
    df["ram_uso_pct"] = df["ram_uso_gb"] / df["ram_total_gb"] * 100
    df["swap_uso_pct"] = df["swap_uso_gb"] / df["swap_total_gb"] * 100
    df["disco_uso_pct"] = df["disco_uso_gb"] / df["disco_total_gb"] * 100

    return df

def calcular_perda_pacotes(df):
    df = df.copy()

    df["packet_loss"] = df["package_loss_sent"] - df["package_loss_received"]

    return df

def ordenar_colunas(df):
    ordenacao = [
        "endereco_mac",
        "timestamp",
        "cpu_uso_pct",
        "qtd_cpu_fisica",
        "qtd_cpu_logica",
        "cpu_freq_mhz",
        "load_avg_1m",
        "load_avg_5m",
        "load_avg_15m",
        "ram_uso_pct",
        "ram_uso_gb",
        "ram_total_gb",
        "swap_uso_pct",
        "swap_uso_gb",
        "swap_total_gb",
        "disco_uso_pct",
        "disco_uso_gb",
        "disco_total_gb",
        "qtd_processos",
        "maior_processo_cpu",
        "maior_processo_ram",
        "download_mb",
        "upload_mb",
        "latencia_ms",
        "packet_loss",
        "package_loss_sent",
        "package_loss_received",
        "cpu_1",
        "cpu_2",
        "cpu_3",
        "cpu_4",
        "cpu_5",
        "cpu_6",
        "cpu_7",
        "cpu_8",
        "cpu_9",
        "cpu_10",
        "cpu_11",
        "cpu_12",
        "cpu_13",
        "cpu_14",
        "cpu_15",
        "cpu_16",
    ]

    print("CSV tratado com sucesso.")

    return df[ordenacao]

def enviar_csv(df, mac_geral):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    object_key = f"02-silver/ano={data_ano}/mes={data_mes}/dia={data_dia}/hora={data_hora}/{mac_geral}_{data_hora}{data_minuto}_tratamento.csv"
    s3.put_object(
        Bucket=bucket,
        Key=object_key,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv",
    )

    print(f"Arquivo enviado para o bucket {bucket} com sucesso.")

def carregar_csv():
    ultima_leitura = s3.list_objects_v2(Bucket=bucket, Prefix=bronze_prefixo)

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
