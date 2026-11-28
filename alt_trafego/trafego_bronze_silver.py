import io
import os
import boto3
import mysql.connector
import pandas as pd
from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env (Banco de dados e AWS S3)
load_dotenv()

# ==============================================================================
# 1. CONEXÃO E CONSULTA AO BANCO DE DADOS
# ==============================================================================
conexao = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME"),
)

comando = """
    SELECT DISTINCT
        e.rodovia,
        e.km,
        e.sentido,
        emb.endereco_mac
    FROM endereco e
    JOIN embarcado emb
        ON emb.endereco_id = e.id_endereco;
"""
df_banco = pd.read_sql_query(comando, conexao)
conexao.close()

# Padroniza tipos e formatação para evitar falhas no cruzamento de dados
df_banco["km"] = pd.to_numeric(df_banco["km"], errors="coerce")
df_banco["sentido"] = df_banco["sentido"].astype(str).str.strip().str.upper()
df_banco["rodovia"] = (
    df_banco["rodovia"]
    .astype(str)
    .str.strip()
    .str.upper()
    .str.replace("-", "", regex=False)
)

# Desmembra sentidos compostos (ex: "N/S" vira duas linhas: "N" e "S")
df_banco["sentido"] = df_banco["sentido"].str.split("/")
df_banco = df_banco.explode("sentido")
df_banco["sentido"] = df_banco["sentido"].str.strip()

# Mantém endereco_mac no subset caso exista mais de um equipamento no mesmo ponto
df_banco = df_banco.drop_duplicates(subset=["rodovia", "km", "sentido", "endereco_mac"])
df_banco = df_banco.rename(columns={"km": "km_pedagio"})

# ==============================================================================
# 2. CONFIGURAÇÕES DE PROCESSAMENTO E CLIENTE AWS S3
# ==============================================================================
s3 = boto3.client("s3")

bucket_entrada = "s3-teste-03261007"
chave_entrada = "01-bronze/trafego/contagem_sat_hora_2026.csv"

bucket_saida = "s3-teste-03261007"
chave_saida = "02-silver/trafego/oficial/2026.csv"

TAMANHO_BLOCO = 500_000
colunas_uteis = ["RODOVIA", "KM", "SENTIDO", "DATA", "HORA", "QTD_MOTO", "QTD_PASSEIO", "QTD_COMERCIAL"]
colunas_agrupamento = ["rodovia", "km_pedagio", "sentido", "data", "hora"]
colunas_soma = ["qtd_moto", "qtd_passeio", "qtd_comercial"]

# ==============================================================================
# 3. ETAPA 0: MAPEAR O SAT MAIS PRÓXIMO DE CADA PEDÁGIO (LENDO VIA STREAM DO S3)
# ==============================================================================
print(f"Iniciando mapeamento lendo de s3://{bucket_entrada}/{chave_entrada}...")
sats_unicos = set()

# Abre o stream de leitura diretamente do S3
objeto_s3_etapa0 = s3.get_object(Bucket=bucket_entrada, Key=chave_entrada)

for chunk in pd.read_csv(objeto_s3_etapa0["Body"], usecols=["RODOVIA", "KM", "SENTIDO"], chunksize=TAMANHO_BLOCO):
    chunk.columns = chunk.columns.str.lower()
    chunk["rodovia"] = chunk["rodovia"].astype(str).str.strip().str.upper().str.replace("-", "", regex=False)
    chunk["km"] = pd.to_numeric(chunk["km"], errors="coerce")
    chunk["sentido"] = chunk["sentido"].astype(str).str.strip().str.upper()
    
    pares = chunk.dropna(subset=["rodovia", "km", "sentido"]).drop_duplicates()
    sats_unicos.update(zip(pares["rodovia"], pares["km"], pares["sentido"]))

df_sats = pd.DataFrame(list(sats_unicos), columns=["rodovia", "km_sat", "sentido"])

# Cruza pedágios com todos os SATs da mesma rodovia/sentido e filtra pela menor distância
df_mapa = df_banco.merge(df_sats, on=["rodovia", "sentido"], how="inner")
df_mapa["distancia_km"] = (df_mapa["km_pedagio"] - df_mapa["km_sat"]).abs()
df_mapa = (
    df_mapa.sort_values("distancia_km")
    .drop_duplicates(subset=["rodovia", "km_pedagio", "sentido", "endereco_mac"], keep="first")
)

print("Mapeamento concluído:")
print(df_mapa[["rodovia", "sentido", "km_pedagio", "km_sat", "endereco_mac", "distancia_km"]])

# ==============================================================================
# 4. ETAPA UNIFICADA: FILTRAR E AGREGAR EM MEMÓRIA (LENDO NOVO STREAM DO S3)
# ==============================================================================
print("\nIniciando filtragem e agregação por blocos...")
resumos_parciais = []
linhas_processadas = 0

# Remove duplicatas de MAC temporariamente para não multiplicar a contagem na soma
mapa_sat_pedagio = df_mapa[["rodovia", "km_pedagio", "km_sat", "sentido"]].drop_duplicates()

# Reabre o stream do S3 do início para a segunda passagem
objeto_s3_etapa1 = s3.get_object(Bucket=bucket_entrada, Key=chave_entrada)

for chunk in pd.read_csv(objeto_s3_etapa1["Body"], usecols=colunas_uteis, chunksize=TAMANHO_BLOCO):
    chunk.columns = chunk.columns.str.lower()
    chunk["rodovia"] = chunk["rodovia"].astype(str).str.strip().str.upper().str.replace("-", "", regex=False)
    chunk["km"] = pd.to_numeric(chunk["km"], errors="coerce")
    chunk["sentido"] = chunk["sentido"].astype(str).str.strip().str.upper()

    chunk = chunk.rename(columns={"km": "km_sat"})
    chunk_filtrado = chunk.merge(mapa_sat_pedagio, on=["rodovia", "km_sat", "sentido"])

    if not chunk_filtrado.empty:
        linhas_processadas += len(chunk_filtrado)
        # Agrega o bloco imediatamente em memória para economizar RAM e disco
        agrupado = chunk_filtrado.groupby(colunas_agrupamento, as_index=False)[colunas_soma].sum()
        resumos_parciais.append(agrupado)

    print(f"Bloco processado | Registros válidos acumulados: {linhas_processadas}")

# Consolida todos os resumos parciais em um único DataFrame final
df_final = (
    pd.concat(resumos_parciais, ignore_index=True)
    .groupby(colunas_agrupamento, as_index=False)[colunas_soma]
    .sum()
)

# Renomeia colunas de contagem e calcula o total geral de veículos
df_final = df_final.rename(columns={
    "qtd_moto": "total_moto",
    "qtd_passeio": "total_passeio",
    "qtd_comercial": "total_comercial"
})
df_final["total_veiculos"] = (
    df_final["total_moto"] + df_final["total_passeio"] + df_final["total_comercial"]
)

# Reassocia o endereco_mac correspondente a cada ponto/sentido
df_final = df_final.merge(
    df_mapa[["rodovia", "km_pedagio", "sentido", "endereco_mac"]],
    on=["rodovia", "km_pedagio", "sentido"],
    how="left"
)

# Ordena colunas e registros para exportação final
colunas_finais = [
    "rodovia", "km_pedagio", "sentido", "endereco_mac",
    "data", "hora", "total_moto", "total_passeio", "total_comercial", "total_veiculos"
]
df_final = df_final[colunas_finais].sort_values(
    by=["rodovia", "km_pedagio", "sentido", "endereco_mac", "data", "hora"]
)

# ==============================================================================
# 5. EXPORTAÇÃO DIRETA PARA O BUCKET AWS S3
# ==============================================================================
print("\nGerando CSV em memória e enviando para o S3...")

csv_buffer = io.StringIO()
df_final.to_csv(csv_buffer, index=False)

s3.put_object(
    Bucket=bucket_saida,
    Key=chave_saida,
    Body=csv_buffer.getvalue(),
    ContentType="text/csv"
)

print(f"Concluído! Arquivo salvo diretamente em: s3://{bucket_saida}/{chave_saida}")