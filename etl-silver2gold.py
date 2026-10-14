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
    database=os.getenv("DB_NAME")
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

def verificar_limites(mac_geral, df):
    componentes = [
        "cpu_uso_pct",
        "cpu_freq_mhz",
        "ram_uso_pct",
        "swap_uso_pct",
        "disco_uso_pct",
        "download_mb",
        "upload_mb",
        "latencia_ms",
        "packet_loss"
    ]

    for componente in componentes:
        comando = """
            SELECT limite_max, limite_min
            FROM parametro p
            JOIN embarcado e ON p.embarcado_id = e.id_embarcado
            JOIN componente c ON p.componente_id = c.id_componente
            WHERE e.endereco_mac = %s AND c.nome = %s;
        """
        cursor.execute(comando, (mac_geral, componente))

        resultado = cursor.fetchone()

        if resultado is None:
            df[f"lim_max_{componente}"] = None
            df[f"lim_min_{componente}"] = None
            df[f"estado_{componente}"] = None
            continue
        df[f"lim_max_{componente}"] = resultado["limite_max"]
        df[f"lim_min_{componente}"] = resultado["limite_min"]

        df[f"estado_{componente}"] = (
            (df[componente] >= resultado["limite_min"]) &
            (df[componente] <= resultado["limite_max"])
        )

    return df

def separar_componentes(mac_geral, df):
    for indice, linha in df.iterrows():
        cpu = {
            "nome": "cpu",
            "mac_adress": mac_geral,
            "timestamp": linha["timestamp"],
            "parametros": [
                {
                    "nome": "cpu_uso_pct",
                    "valor": linha["cpu_uso_pct"],
                    "estado": linha["estado_cpu_uso_pct"],
                    "limite_min": linha["lim_min_cpu_uso_pct"],
                    "limite_max": linha ["lim_max_cpu_uso_pct"],
                },
                {
                    "nome": "cpu_freq_mhz",
                    "valor": linha["cpu_freq_mhz"],
                    "estado": linha["estado_cpu_freq_mhz"],
                    "limite_min": linha["lim_min_cpu_freq_mhz"],
                    "limite_max": linha ["lim_max_cpu_freq_mhz"],
                },
            ]
        }

        ram = {
            "nome": "ram",
            "mac_adress": mac_geral,
            "timestamp": linha["timestamp"],
            "parametros": [
                {
                    "nome": "ram_uso_pct",
                    "valor": linha["ram_uso_pct"],
                    "estado": linha["estado_ram_uso_pct"],
                    "limite_min": linha["lim_min_ram_uso_pct"],
                    "limite_max": linha ["lim_max_ram_uso_pct"],
                },
                {
                    "nome": "swap_uso_pct",
                    "valor": linha["swap_uso_pct"],
                    "estado": linha["estado_swap_uso_pct"],
                    "limite_min": linha["lim_min_swap_uso_pct"],
                    "limite_max": linha ["lim_max_swap_uso_pct"],
                },
            ]
        }

        disco = {
            "nome": "disco",
            "mac_adress": mac_geral,
            "timestamp": linha["timestamp"],
            "parametros": [
                {
                    "nome": "disco_uso_pct",
                    "valor": linha["disco_uso_pct"],
                    "estado": linha["estado_disco_uso_pct"],
                    "limite_min": linha["lim_min_disco_uso_pct"],
                    "limite_max": linha ["lim_max_disco_uso_pct"],
                },
            ]
        }

        rede = {
            "nome": "rede",
            "mac_adress": mac_geral,
            "timestamp": linha["timestamp"],
            "parametros": [
                {
                    "nome": "download_mb",
                    "valor": linha["download_mb"],
                    "estado": linha["estado_download_mb"],
                    "limite_min": linha["lim_min_download_mb"],
                    "limite_max": linha ["lim_max_download_mb"],
                },
                {
                    "nome": "upload_mb",
                    "valor": linha["upload_mb"],
                    "estado": linha["estado_upload_mb"],
                    "limite_min": linha["lim_min_upload_mb"],
                    "limite_max": linha ["lim_max_upload_mb"],
                },
                                {
                    "nome": "latencia_ms",
                    "valor": linha["latencia_ms"],
                    "estado": linha["estado_latencia_ms"],
                    "limite_min": linha["lim_min_latencia_ms"],
                    "limite_max": linha ["lim_max_latencia_ms"],
                },
                                {
                    "nome": "packet_loss",
                    "valor": linha["packet_loss"],
                    "estado": linha["estado_packet_loss"],
                    "limite_min": linha["lim_min_packet_loss"],
                    "limite_max": linha ["lim_max_packet_loss"],
                },
            ]
        }

        enviar_json(cpu, ram, disco, rede)

def enviar_json(cpu, ram, disco, rede):
    for componente in (cpu, ram, disco, rede):
        nome = componente["nome"]
        mac = componente["mac_adress"]

        gold_prefixo = (
            f"03-gold/componente={nome}/ano={data_ano}/mes={data_mes}/dia={data_dia}/hora={data_hora}/"
        )

        chave = (
            f"{gold_prefixo}componente={nome}/{mac}_{data_hora}{data_minuto}_{nome}.json"
        )

        corpo = json.dumps(
            componente,
            ensure_ascii=False,
            indent=2,
            allow_nan=False
        ).encode("utf-8")

        s3.put_object(
            Bucket=bucket,
            Key=chave,
            Body=corpo,
            ContentType="application/json"
        )

        print("JSON enviado com sucesso")

def processar_csv(dataframes_por_mac):
    for mac_geral, df in dataframes_por_mac.items():
        df = verificar_limites(mac_geral, df)
        separar_componentes(mac_geral, df)

def carregar_csv():
    ultima_leitura = s3.list_objects_v2(Bucket=bucket, Prefix=silver_prefixo)

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