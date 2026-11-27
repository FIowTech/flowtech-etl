import io
import boto3
import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ==============================================================================
# CONFIGURAÇÕES DO S3 E PARÂMETROS DO MODELO
# ==============================================================================
s3 = boto3.client("s3")
BUCKET = "s3-teste-03261007"

chave_historico = "02-silver/trafego/oficial/2026.csv"
prefixo_previsao = "02-silver/trafego/previsao/"

DIAS_PREVISAO = 30
JANELA_DIAS_TREINO = 90  # Usa os últimos 90 dias para capturar a tendência recente

# ==============================================================================
# 1. LEITURA E PADRONIZAÇÃO DO HISTÓRICO (DIRETO DO S3)
# ==============================================================================
print(f"Lendo histórico de s3://{BUCKET}/{chave_historico}...")
obj_historico = s3.get_object(Bucket=BUCKET, Key=chave_historico)
df = pd.read_csv(obj_historico["Body"])

mapa_colunas = {
    "qtd_moto": "total_moto",
    "qtd_passeio": "total_passeio",
    "qtd_comercial": "total_comercial",
}
df = df.rename(columns=mapa_colunas)

df["data_dt"] = pd.to_datetime(df["data"])
df["dia_semana"] = df["data_dt"].dt.dayofweek  # 0 = Segunda ... 6 = Domingo

data_maxima = df["data_dt"].max()
data_corte = data_maxima - pd.Timedelta(days=JANELA_DIAS_TREINO)
df_recente = df[df["data_dt"] >= data_corte].copy()

dias_atras = (data_maxima - df_recente["data_dt"]).dt.days
df_recente["peso"] = np.exp(-dias_atras / 30.0)

# ==============================================================================
# 2. CÁLCULO DO PERFIL SAZONAL PONDERADO (DIA DA SEMANA + HORA)
# ==============================================================================
chaves_perfil = ["rodovia", "km_pedagio", "sentido", "dia_semana", "hora"]

tem_mac = "endereco_mac" in df_recente.columns
if tem_mac:
    chaves_perfil.insert(3, "endereco_mac")

for col in ["total_moto", "total_passeio", "total_comercial"]:
    df_recente[f"{col}_pond"] = df_recente[col] * df_recente["peso"]

perfil = (
    df_recente.groupby(chaves_perfil, as_index=False)[
        ["total_moto_pond", "total_passeio_pond", "total_comercial_pond", "peso"]
    ]
    .sum()
)

perfil["total_moto"] = (perfil["total_moto_pond"] / perfil["peso"]).round().astype(int)
perfil["total_passeio"] = (perfil["total_passeio_pond"] / perfil["peso"]).round().astype(int)
perfil["total_comercial"] = (perfil["total_comercial_pond"] / perfil["peso"]).round().astype(int)
perfil["total_veiculos"] = (
    perfil["total_moto"] + perfil["total_passeio"] + perfil["total_comercial"]
)

colunas_perfil_limpas = chaves_perfil + [
    "total_moto", "total_passeio", "total_comercial", "total_veiculos"
]
perfil = perfil[colunas_perfil_limpas]

# ==============================================================================
# 3. GERAÇÃO DA GRADE FUTURA DOS PRÓXIMOS 30 DIAS
# ==============================================================================
datas_futuras = pd.date_range(
    start=data_maxima + pd.Timedelta(days=1),
    periods=DIAS_PREVISAO,
    freq="D"
)

# Define o nome do arquivo pelo ano-mês inicial da previsão (ex: "2026-12.csv")
# Dica: se quiser por extenso em inglês ("december.csv"), use datas_futuras[0].strftime("%B").lower()
mes_previsao = datas_futuras[0].strftime("%Y-%m")
chave_previsao = f"{prefixo_previsao}{mes_previsao}.csv"

df_datas = pd.DataFrame({
    "data_dt": datas_futuras,
    "data": datas_futuras.strftime("%Y-%m-%d"),
    "dia_semana": datas_futuras.dayofweek,
})

df_previsto = df_datas.merge(perfil, on="dia_semana", how="inner")

colunas_saida = ["rodovia", "km_pedagio", "sentido"]
if tem_mac:
    colunas_saida.append("endereco_mac")
colunas_saida += ["data", "hora", "total_moto", "total_passeio", "total_comercial", "total_veiculos"]

df_previsto = df_previsto[colunas_saida].sort_values(
    by=[c for c in ["rodovia", "km_pedagio", "sentido", "endereco_mac", "data", "hora"] if c in colunas_saida]
)

# ==============================================================================
# 4. EXPORTAÇÃO DIRETA PARA O BUCKET AWS S3
# ==============================================================================
csv_buffer = io.StringIO()
df_previsto.to_csv(csv_buffer, index=False)

s3.put_object(
    Bucket=BUCKET,
    Key=chave_previsao,
    Body=csv_buffer.getvalue(),
    ContentType="text/csv",
)

print(f"Previsão gerada de {df_previsto['data'].min()} até {df_previsto['data'].max()}!")
print(f"Total de linhas: {len(df_previsto)} | Salvo em: s3://{BUCKET}/{chave_previsao}")