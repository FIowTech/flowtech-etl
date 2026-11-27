import json
from datetime import datetime
from zoneinfo import ZoneInfo
import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ==============================================================================
# CONFIGURAÇÕES DO S3
# ==============================================================================
s3 = boto3.client("s3")
BUCKET = "s3-teste-03261007"

chave_real = "02-silver/trafego/oficial/2026.csv"
prefixo_previsao = "02-silver/trafego/previsao/"

# Prefixos de entrada (gerados pela Lambda silver-to-gold)
prefixo_base_entrada = "03-gold/falhas/"
prefixo_entrada_ativas = f"{prefixo_base_entrada}em_andamento/"
prefixo_entrada_encerradas = f"{prefixo_base_entrada}encerradas/"

# Prefixos de saída (falhas já enriquecidas com o cálculo de tráfego)
prefixo_base_saida = "03-gold/falhas_com_trafego/"
prefixo_saida_ativas = f"{prefixo_base_saida}em_andamento/"
prefixo_saida_encerradas = f"{prefixo_base_saida}encerradas/"

# ==============================================================================
# 1. CARREGAR E UNIR CSV REAL + CSVs MENSAIS DE PREVISÃO DO S3
# ==============================================================================
print(f"Lendo dados oficiais de s3://{BUCKET}/{chave_real}...")
obj_real = s3.get_object(Bucket=BUCKET, Key=chave_real)
dfs_para_unir = [pd.read_csv(obj_real["Body"])]

paginator_prev = s3.get_paginator("list_objects_v2")
for pagina in paginator_prev.paginate(Bucket=BUCKET, Prefix=prefixo_previsao):
    for obj in pagina.get("Contents", []):
        if obj["Key"].endswith(".csv"):
            print(f"Carregando previsão: s3://{BUCKET}/{obj['Key']}...")
            csv_prev = s3.get_object(Bucket=BUCKET, Key=obj["Key"])
            dfs_para_unir.append(pd.read_csv(csv_prev["Body"]))

df = (
    pd.concat(dfs_para_unir, ignore_index=True)
    .drop_duplicates(
        subset=["rodovia", "km_pedagio", "sentido", "endereco_mac", "data", "hora"],
        keep="first",
    )
)

df["endereco_mac"] = df["endereco_mac"].astype(str).str.strip().str.upper()
df["inicio_hora"] = pd.to_datetime(df["data"]) + pd.to_timedelta(df["hora"], unit="h")
df["fim_hora"] = df["inicio_hora"] + pd.Timedelta(hours=1)

df_mac = (
    df.groupby(["endereco_mac", "inicio_hora", "fim_hora"], as_index=False)[
        ["total_moto", "total_passeio", "total_comercial", "total_veiculos"]
    ]
    .sum()
)

# ==============================================================================
# 2. FUNÇÃO PARA CALCULAR TRÁFEGO NO INTERVALO DA FALHA
# ==============================================================================
def calcular_trafego_falha(mac: str, inicio_str: str, fim_str: str | None) -> dict:
    mac_limpo = str(mac).strip().upper()
    dt_inicio = datetime.fromisoformat(inicio_str)

    dt_fim = (
        datetime.fromisoformat(fim_str)
        if fim_str
        else datetime.now(ZoneInfo("America/Sao_Paulo")).replace(tzinfo=None)
    )

    if dt_fim <= dt_inicio:
        return {
            "trafego_moto": 0,
            "trafego_passeio": 0,
            "trafego_comercial": 0,
            "trafego_total": 0,
        }

    filtro = (
        (df_mac["endereco_mac"] == mac_limpo)
        & (df_mac["fim_hora"] > dt_inicio)
        & (df_mac["inicio_hora"] < dt_fim)
    )
    recorte = df_mac.loc[filtro].copy()

    if recorte.empty:
        return {
            "trafego_moto": 0,
            "trafego_passeio": 0,
            "trafego_comercial": 0,
            "trafego_total": 0,
        }

    inicio_efetivo = recorte["inicio_hora"].clip(lower=dt_inicio)
    fim_efetivo = recorte["fim_hora"].clip(upper=dt_fim)
    proporcao_hora = (fim_efetivo - inicio_efetivo).dt.total_seconds() / 3600.0

    moto = int(round((recorte["total_moto"] * proporcao_hora).sum()))
    passeio = int(round((recorte["total_passeio"] * proporcao_hora).sum()))
    comercial = int(round((recorte["total_comercial"] * proporcao_hora).sum()))

    return {
        "trafego_moto": moto,
        "trafego_passeio": passeio,
        "trafego_comercial": comercial,
        "trafego_total": moto + passeio + comercial,
    }


def listar_chaves_json(prefixo: str) -> list[str]:
    paginator = s3.get_paginator("list_objects_v2")
    return [
        obj["Key"]
        for pagina in paginator.paginate(Bucket=BUCKET, Prefix=prefixo)
        for obj in pagina.get("Contents", [])
        if obj["Key"].endswith(".json")
    ]


def enriquecer_documento(dados: dict | list) -> dict | list:
    if isinstance(dados, list):
        for item in dados:
            item.update(
                calcular_trafego_falha(
                    item["endereco_mac"], item["data_inicio"], item.get("data_fim")
                )
            )
    elif isinstance(dados, dict):
        dados.update(
            calcular_trafego_falha(
                dados["endereco_mac"], dados["data_inicio"], dados.get("data_fim")
            )
        )
    return dados


# ==============================================================================
# 3A. PROCESSAR FALHAS EM ANDAMENTO (SEMPRE RECALCULA E SOBRESCREVE)
# ==============================================================================
chaves_ativas = listar_chaves_json(prefixo_entrada_ativas)
print(f"Falhas em andamento encontradas: {len(chaves_ativas)}")

for chave_origem in chaves_ativas:
    resposta = s3.get_object(Bucket=BUCKET, Key=chave_origem)
    dados = json.loads(resposta["Body"].read().decode("utf-8"))
    dados = enriquecer_documento(dados)

    caminho_relativo = chave_origem[len(prefixo_entrada_ativas):]
    chave_destino = f"{prefixo_saida_ativas}{caminho_relativo}"

    s3.put_object(
        Bucket=BUCKET,
        Key=chave_destino,
        Body=json.dumps(dados, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

# ==============================================================================
# 3B. PROCESSAR FALHAS ENCERRADAS (CALCULA SOMENTE UMA VEZ)
# ==============================================================================
chaves_encerradas_origem = listar_chaves_json(prefixo_entrada_encerradas)
# Mapeia em um set o que já foi processado na saída para consulta O(1) sem fazer GET
ja_processadas = set(listar_chaves_json(prefixo_saida_encerradas))

novas_encerradas = 0
for chave_origem in chaves_encerradas_origem:
    caminho_relativo = chave_origem[len(prefixo_entrada_encerradas):]
    chave_destino = f"{prefixo_saida_encerradas}{caminho_relativo}"

    # Se já existe na pasta de encerradas enriquecidas, pula (calcula apenas 1 vez)
    if chave_destino in ja_processadas:
        continue

    resposta = s3.get_object(Bucket=BUCKET, Key=chave_origem)
    dados = json.loads(resposta["Body"].read().decode("utf-8"))
    dados = enriquecer_documento(dados)

    # Grava a versão final fechada
    s3.put_object(
        Bucket=BUCKET,
        Key=chave_destino,
        Body=json.dumps(dados, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    # Remove a versão antiga de "em_andamento" na pasta de saída, caso tenha existido
    chave_antiga_ativa = f"{prefixo_saida_ativas}{caminho_relativo}"
    s3.delete_object(Bucket=BUCKET, Key=chave_antiga_ativa)

    novas_encerradas += 1

print(
    f"Concluído! Ativas atualizadas: {len(chaves_ativas)} | "
    f"Novas encerradas processadas: {novas_encerradas} | "
    f"Encerradas ignoradas (já calculadas): {len(chaves_encerradas_origem) - novas_encerradas}"
)