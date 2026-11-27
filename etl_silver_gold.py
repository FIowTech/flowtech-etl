import io
import json
import logging
import os
from urllib.parse import unquote_plus

import boto3
import mysql.connector
import pandas as pd
from datetime import datetime
from botocore.exceptions import ClientError

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

# === VARIÁVEIS GLOBAIS === #
#
# Carrega variáveis do .env no ambiente local. Na Lambda, as variáveis já vêm
# do próprio ambiente de execução.
if load_dotenv:
    load_dotenv()

# Define INFO como nível mínimo de log. A Lambda envia os logs para o CloudWatch.
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Cliente S3 global para reaproveitar entre invocações (cold start).
s3 = boto3.client("s3")

# Conexão com banco criada sob demanda e reaproveitada enquanto possível.
conexao = None

# Prefixos usados para validar origem (SILVER) e montar destino (GOLD).
PREFIXO_SILVER = "02-silver/metricas/"
PREFIXO_GOLD = "03-gold/metricas/"
PREFIXO_FALHAS = "03-gold/falhas/"
PREFIXO_ESTADO = "03-gold/_estado/"
LIMITE_CAPTURAS_FALHA = 2
DELIMITADOR = ";"

# Mapeia nome de componente no banco para chave interna usada na aplicação.
MAPA_COMPONENTE_DB_METRICA = {
    "cpu_uso": "cpu.uso",
    "cpu_freq": "cpu.freq",
    "ram_uso": "ram.uso",
    "swap_uso": "swap.uso",
    "disco_uso": "disco.uso",
    "rede_download": "rede.download",
    "rede_upload": "rede.upload",
    "rede_latencia": "rede.latencia",
}

# Query única: busca contexto do embarcado e limites ativos.
# Usa placeholder %s (prepared statement) e normaliza MAC no WHERE.
QUERY_CONTEXTO_LIMITES = """
SELECT
    e.empresa_id   AS id_empresa,
    e.id_embarcado AS id_embarcado,
    e.endereco_mac AS endereco_mac,
    c.nome         AS componente,
    p.limite_min   AS limite_min,
    p.limite_max   AS limite_max
FROM embarcado e
LEFT JOIN parametro p
       ON p.embarcado_id = e.id_embarcado
      AND p.empresa_id   = e.empresa_id
      AND p.status       = 1
LEFT JOIN componente c
       ON c.id_componente = p.componente_id
WHERE REPLACE(REPLACE(UPPER(e.endereco_mac), ':', ''), '-', '') = %s
;
"""


# Cria/recria conexão com banco quando necessário.
def get_conexao():
    global conexao
    if conexao is None or not conexao.is_connected():
        conexao = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
        )
    return conexao


# Conversão segura para float (com valor padrão quando inválido/nulo).
def to_float(valor, padrao=0.0):
    if pd.isna(valor):
        return padrao
    try:
        return float(valor)
    except (TypeError, ValueError):
        return padrao


# Conversão segura para inteiro (com valor padrão quando inválido/nulo).
def to_int(valor, padrao=0):
    if pd.isna(valor):
        return padrao
    try:
        return int(valor)
    except (TypeError, ValueError):
        return padrao


# Converte para texto removendo espaços nas pontas.
def to_texto(valor, padrao=""):
    if pd.isna(valor):
        return padrao
    return str(valor).strip()


# Formata MAC para padrão AA:BB:CC:DD:EE:FF quando possível.
def normalizar_mac(mac):
    bruto = "".join(ch for ch in str(mac or "") if ch.isalnum()).upper()
    if len(bruto) == 12:
        return ":".join(bruto[i:i + 2] for i in range(0, 12, 2))
    return str(mac or "").strip()


# Remove separadores e padroniza MAC para cache/consulta.
def normalizar_mac_limpo(mac):
    return "".join(ch for ch in str(mac or "") if ch.isalnum()).upper()


# Classifica a métrica conforme limites definidos.
# Sem limite configurado: situação "Normal".
def classificar(valor, limite_min, limite_max):
    # Regra pedida: sem limite => Normal
    if valor is None:
        return "Normal"

    if limite_min is not None and valor <= limite_min:
        return "Crítico"
    if limite_max is not None:
        if valor >= limite_max:
            return "Crítico"
        if valor >= 0.7 * limite_max:
            return "Alerta"
    return "Normal"


# Define a situação final do componente pela pior situação entre suas métricas.
# Prioridade: Crítico > Alerta > Normal.
def situacao_componente(situacoes_metricas):
    if any(s == "Crítico" for s in situacoes_metricas):
        return "Crítico"
    if any(s == "Alerta" for s in situacoes_metricas):
        return "Alerta"
    return "Normal"


# Busca no banco o contexto do embarcado (empresa/id) e limites por métrica.
# Retorna (contexto, limites) ou (None, None) quando MAC não encontrado.
def buscar_contexto_e_limites(endereco_mac):
    mac_limpo = normalizar_mac_limpo(endereco_mac)
    if not mac_limpo:
        return None, None

    cursor = get_conexao().cursor(dictionary=True)
    cursor.execute(QUERY_CONTEXTO_LIMITES, (mac_limpo,))
    linhas = cursor.fetchall()
    cursor.close()

    if not linhas:
        return None, None

    primeira = linhas[0]
    contexto = {
        "id_empresa": to_int(primeira.get("id_empresa")),
        "id_embarcado": to_int(primeira.get("id_embarcado")),
        "endereco_mac": to_texto(primeira.get("endereco_mac"), normalizar_mac(mac_limpo)),
    }

    limites = {}
    for linha in linhas:
        nome_componente = linha.get("componente")
        chave = MAPA_COMPONENTE_DB_METRICA.get(nome_componente)
        if not chave:
            continue
        limites[chave] = {
            "limite_min": None if linha.get("limite_min") is None else float(linha["limite_min"]),
            "limite_max": None if linha.get("limite_max") is None else float(linha["limite_max"]),
        }

    return contexto, limites


# Monta a estrutura base de uma métrica no JSON final.
def montar_metrica(nome, unidade, valor, total, limite_min, limite_max):
    valor_classificacao = valor if valor is not None else 0.0
    situacao = classificar(valor_classificacao, limite_min, limite_max)
    return {
        "nome": nome,
        "situacao": situacao,
        "unidade_medida": unidade,
        "valor": float(valor),
        "valor_pct": None,
        "total": None if total is None else float(total),
        "limite_max": 0.0 if limite_max is None else float(limite_max),
        "limite_min": 0.0 if limite_min is None else float(limite_min),
    }


# Calcula percentual a partir de valor/total, protegendo total nulo/zero.
def calcular_pct(valor, total):
    if total is None or total <= 0:
        return 0.0
    return (float(valor) / float(total)) * 100.0


# Monta o payload no formato exigido pelo AGENTS.md a partir de uma linha SILVER.
def montar_payload(linha, contexto, limites):
    def lim(chave):
        item = limites.get(chave, {})
        return item.get("limite_min"), item.get("limite_max")

    # Métricas base lidas da linha SILVER
    cpu_uso = to_float(linha.get("cpu_uso_pct"))
    cpu_total = to_float(linha.get("cpu_freq_total_mhz"))
    cpu_freq = to_float(linha.get("cpu_freq_mhz"))
    cpu_freq_pct = calcular_pct(cpu_freq, cpu_total)

    ram_uso_pct = to_float(linha.get("ram_uso_pct"))
    ram_uso = to_float(linha.get("ram_uso_gb"))
    ram_total = to_float(linha.get("ram_total_gb"))

    swap_uso_pct = to_float(linha.get("swap_uso_pct"))
    swap_uso = to_float(linha.get("swap_uso_gb"))
    swap_total = to_float(linha.get("swap_total_gb"))

    disco_uso_pct = to_float(linha.get("disco_uso_pct"))
    disco_uso = to_float(linha.get("disco_uso_gb"))
    disco_total = to_float(linha.get("disco_total_gb"))

    download = to_float(linha.get("download_mbps"))
    upload = to_float(linha.get("upload_mbps"))
    latencia = to_float(linha.get("latencia_ms"))

    lim_cpu_uso_min, lim_cpu_uso_max = lim("cpu.uso")
    lim_cpu_freq_min, lim_cpu_freq_max = lim("cpu.freq")
    lim_ram_uso_min, lim_ram_uso_max = lim("ram.uso")
    lim_swap_uso_min, lim_swap_uso_max = lim("swap.uso")
    lim_disco_uso_min, lim_disco_uso_max = lim("disco.uso")
    lim_rede_download_min, lim_rede_download_max = lim("rede.download")
    lim_rede_upload_min, lim_rede_upload_max = lim("rede.upload")
    lim_rede_latencia_min, lim_rede_latencia_max = lim("rede.latencia")

    # Lista de uso por núcleo (cpu_1 ... cpu_16), quando disponíveis.
    cpu_por_nucleo = []
    for i in range(1, 17):
        col = f"cpu_{i}"
        if col in linha and not pd.isna(linha[col]):
            cpu_por_nucleo.append(to_float(linha[col]))

    # CPU: uso já é percentual; valor e valor_pct ficam iguais.
    metrica_cpu_uso = montar_metrica("Uso", "%", cpu_uso, None, lim_cpu_uso_min, lim_cpu_uso_max)
    metrica_cpu_uso["valor_pct"] = float(cpu_uso)
    metrica_cpu_uso["por_nucleo"] = cpu_por_nucleo

    # CPU frequência: valor em MHz e valor_pct calculado por valor/total.
    metrica_cpu_freq = montar_metrica("Frequência", "MHz", cpu_freq, cpu_total, lim_cpu_freq_min, lim_cpu_freq_max)
    metrica_cpu_freq["valor_pct"] = float(cpu_freq_pct)

    # RAM/SWAP/Disco: valor em GB e valor_pct com o uso em percentual.
    metrica_ram_uso = montar_metrica("Uso", "GB", ram_uso, ram_total, lim_ram_uso_min, lim_ram_uso_max)
    metrica_ram_uso["valor_pct"] = float(ram_uso_pct)
    metrica_ram_uso["situacao"] = classificar(ram_uso_pct, lim_ram_uso_min, lim_ram_uso_max)

    metrica_swap_uso = montar_metrica("Uso", "GB", swap_uso, swap_total, lim_swap_uso_min, lim_swap_uso_max)
    metrica_swap_uso["valor_pct"] = float(swap_uso_pct)
    metrica_swap_uso["situacao"] = classificar(swap_uso_pct, lim_swap_uso_min, lim_swap_uso_max)

    metrica_disco_uso = montar_metrica("Uso", "GB", disco_uso, disco_total, lim_disco_uso_min, lim_disco_uso_max)
    metrica_disco_uso["valor_pct"] = float(disco_uso_pct)
    metrica_disco_uso["situacao"] = classificar(disco_uso_pct, lim_disco_uso_min, lim_disco_uso_max)

    # Rede: não usa valor_pct.
    metrica_rede_download = montar_metrica("Download", "Mbps", download, None, lim_rede_download_min, lim_rede_download_max)
    metrica_rede_upload = montar_metrica("Upload", "Mbps", upload, None, lim_rede_upload_min, lim_rede_upload_max)
    metrica_rede_latencia = montar_metrica("Latência", "Ms", latencia, None, lim_rede_latencia_min, lim_rede_latencia_max)

    # Situação de cada componente calculada pelas situações das métricas internas.
    situacao_cpu = situacao_componente([metrica_cpu_uso["situacao"], metrica_cpu_freq["situacao"]])
    situacao_ram = situacao_componente([metrica_ram_uso["situacao"]])
    situacao_swap = situacao_componente([metrica_swap_uso["situacao"]])
    situacao_disco = situacao_componente([metrica_disco_uso["situacao"]])
    situacao_rede = situacao_componente(
        [metrica_rede_download["situacao"], metrica_rede_upload["situacao"], metrica_rede_latencia["situacao"]]
    )

    # Estrutura final do documento JSON por snapshot.
    return {
        "ip": to_texto(linha.get("ip")),
        "id_empresa": contexto["id_empresa"],
        "id_embarcado": contexto["id_embarcado"],
        "endereco_mac": contexto["endereco_mac"],
        "timestamp": to_texto(linha.get("timestamp")),
        "componentes": {
            "cpu": {
                "nome": "CPU",
                "situacao": situacao_cpu,
                "metricas": {
                    "uso": metrica_cpu_uso,
                    "freq": metrica_cpu_freq,
                },
                "maior_processo": {
                    "nome": to_texto(linha.get("maior_processo_cpu")),
                    "unidade_medida": "%",
                    "valor": to_float(linha.get("maior_processo_cpu_pct")),
                },
                "qtd_nucleos_fisicos": to_int(linha.get("qtd_cpu_fisica")),
                "qtd_nucleos_logicos": to_int(linha.get("qtd_cpu_logica")),
                "load_avg_1m": to_float(linha.get("load_avg_1m")),
                "load_avg_5m": to_float(linha.get("load_avg_5m")),
                "load_avg_15m": to_float(linha.get("load_avg_15m")),
            },
            "ram": {
                "nome": "Memória RAM",
                "situacao": situacao_ram,
                "metricas": {
                    "uso": metrica_ram_uso,
                },
                "maior_processo": {
                    "nome": to_texto(linha.get("maior_processo_ram")),
                    "unidade_medida": "%",
                    "valor": to_float(linha.get("maior_processo_ram_pct")),
                },
            },
            "swap": {
                "nome": "Memória SWAP",
                "situacao": situacao_swap,
                "metricas": {
                    "uso": metrica_swap_uso,
                },
            },
            "disco": {
                "nome": "Disco",
                "situacao": situacao_disco,
                "metricas": {
                    "uso": metrica_disco_uso,
                },
            },
            "rede": {
                "nome": "Rede",
                "situacao": situacao_rede,
                "metricas": {
                    "download": metrica_rede_download,
                    "upload": metrica_rede_upload,
                    "latencia": metrica_rede_latencia,
                },
                "pacotes": {
                    "enviados": {
                        "bytes": to_int(linha.get("bytes_enviados")),
                        "total": to_int(linha.get("pacotes_enviados")),
                    },
                    "recebidos": {
                        "bytes": to_int(linha.get("bytes_recebidos")),
                        "total": to_int(linha.get("pacotes_recebidos")),
                    },
                    "perdidos": {
                        "chegando": to_int(linha.get("pacotes_perdidos_chegando")),
                        "saindo": to_int(linha.get("pacotes_perdidos_saindo")),
                        "total": to_int(linha.get("pacotes_perdidos_total")),
                    },
                },
            },
        },
    }


# Monta a chave GOLD preservando o caminho relativo e trocando .csv por .jsonl.
def chave_gold(chave_silver):
    relativa = chave_silver[len(PREFIXO_SILVER):]
    if relativa.lower().endswith(".csv"):
        relativa = relativa[:-4] + ".jsonl"
    return PREFIXO_GOLD + relativa


# Processa um arquivo SILVER: lê CSV, monta JSONL e grava no prefixo GOLD.
def processar_objeto(bucket, chave):
    obj = s3.get_object(Bucket=bucket, Key=chave)
    corpo = obj["Body"].read()
    df = pd.read_csv(io.BytesIO(corpo), sep=DELIMITADOR)

    linhas_jsonl = []
    payloads = []
    total_linhas = len(df)
    linhas_validas = 0
    linhas_ignoradas = 0
    cache_contexto = {}

    # Processa linha a linha para montar os snapshots.
    for _, linha in df.iterrows():
        mac_linha = linha.get("endereco_mac")
        mac_consulta = normalizar_mac_limpo(mac_linha)

        # Evita consultar o mesmo MAC várias vezes no mesmo arquivo.
        if mac_consulta not in cache_contexto:
            cache_contexto[mac_consulta] = buscar_contexto_e_limites(mac_consulta)

        contexto, limites = cache_contexto[mac_consulta]
        if contexto is None:
            linhas_ignoradas += 1
            logger.warning("MAC não encontrado/ativo no banco. Ignorando linha. mac=%s origem=%s", mac_linha, chave)
            continue

        payload = montar_payload(linha, contexto, limites or {})
        payloads.append(payload)
        linhas_jsonl.append(json.dumps(payload, ensure_ascii=False, allow_nan=False))
        linhas_validas += 1

    # Metadados de execução por arquivo processado.
    stats = {
        "origem": chave,
        "linhas_lidas": total_linhas,
        "linhas_validas": linhas_validas,
        "linhas_ignoradas": linhas_ignoradas,
    }

    if not linhas_jsonl:
        logger.warning("Nenhuma linha válida para GOLD: %s", json.dumps(stats, ensure_ascii=False))
        return {**stats, "status": "sem_registros_validos"}

    # Grava um único arquivo JSONL por arquivo CSV de origem.
    destino = chave_gold(chave)
    s3.put_object(
        Bucket=bucket,
        Key=destino,
        Body=("\n".join(linhas_jsonl) + "\n").encode("utf-8"),
        ContentType="application/x-ndjson",
    )

    # Avalia e grava as falhas usando os payloads já prontos na memória.
    falhas_atualizadas = []
    try:
        falhas_atualizadas = avaliar_e_gravar_falhas_s3(bucket, payloads)
    except Exception:
        logger.exception("Erro ao avaliar falhas para %s (métricas GOLD já salvas)", chave)

    resumo = {**stats, "destino": destino, "falhas": falhas_atualizadas}
    logger.info("ETL SILVER->GOLD concluída: %s", json.dumps(resumo, ensure_ascii=False))
    return {**resumo, "status": "ok"}


# Ponto de entrada da Lambda. Processa cada objeto vindo no evento S3.
def lambda_handler(event, context):
    resultados = []
    for registro in event.get("Records", []):
        bucket = registro["s3"]["bucket"]["name"]
        chave = unquote_plus(registro["s3"]["object"]["key"])

        if not (chave.startswith(PREFIXO_SILVER) and chave.lower().endswith(".csv")):
            logger.info("Ignorando objeto fora de %s*.csv: %s", PREFIXO_SILVER, chave)
            continue

        try:
            resultados.append(processar_objeto(bucket, chave))
        except Exception as erro:  # pragma: no cover
            logger.exception("Erro ao processar %s", chave)
            resultados.append({"origem": chave, "status": "erro", "mensagem": str(erro)})

    return {"processados": resultados}

def parse_timestamp(ts_str):
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00")).replace(tzinfo=None)


def ler_estado_s3(bucket, mac_limpo):
    chave = f"{PREFIXO_ESTADO}{mac_limpo}.json"
    try:
        resp = s3.get_object(Bucket=bucket, Key=chave)
        estado = json.loads(resp["Body"].read().decode("utf-8"))
        return estado, resp["ETag"]
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return {"ultimo_timestamp": None, "componentes": {}}, None
        raise


def salvar_estado_s3(bucket, mac_limpo, estado, etag):
    chave = f"{PREFIXO_ESTADO}{mac_limpo}.json"
    kwargs = {
        "Bucket": bucket,
        "Key": chave,
        "Body": json.dumps(estado, ensure_ascii=False).encode("utf-8"),
        "ContentType": "application/json",
    }
    if etag:
        kwargs["IfMatch"] = etag
    else:
        kwargs["IfNoneMatch"] = "*"

    s3.put_object(**kwargs)


def chave_falha_s3(mac_limpo, componente, dt_inicio, em_andamento=True):
    """
    Constrói e retorna o caminho completo (Key) onde o arquivo JSON da falha
    será salvo ou atualizado dentro do bucket S3, separando por status.

    Parâmetros:
        mac_limpo (str): Endereço MAC sem dois-pontos ou hífens (ex: 'AABBCCDDEEFF').
        componente (str): Nome da chave do componente que falhou (ex: 'ram', 'cpu').
        dt_inicio (datetime): Data e hora exatas da 1ª captura crítica que iniciou a falha.
        em_andamento (bool): True para 'em_andamento/', False para 'encerradas/'.
    """
    pasta_hora = dt_inicio.strftime("ano=%Y/mes=%m/dia=%d/hora=%H")
    ts_slug = dt_inicio.strftime("%H%M")
    subpasta_status = "em_andamento" if em_andamento else "encerradas"
    return f"{PREFIXO_FALHAS}{subpasta_status}/{pasta_hora}/{mac_limpo}_{componente}_{ts_slug}_falha.json"


def montar_doc_falha(payload, nome_comp, inicio_iso, fim_iso, contagem, em_andamento):
    """
    Monta o dicionário Python que representa a estrutura final do JSON da falha
    que será gravado na camada GOLD do S3.

    Parâmetros:
        payload (dict): Objeto completo da captura atual (usado para extrair IDs de contexto).
        nome_comp (str): Nome do componente em falha (ex: 'ram', 'disco').
        inicio_iso (str): Timestamp ISO-8601 da 1ª captura crítica da sequência.
        fim_iso (str): Timestamp ISO-8601 da última captura observada ou da normalização.
        contagem (int): Total de capturas consecutivas registradas em estado 'Crítico'.
        em_andamento (bool): True se o componente permanece crítico; False se já normalizou.
    """
    return {
        "id_empresa": payload["id_empresa"],
        "id_embarcado": payload["id_embarcado"],
        "endereco_mac": payload["endereco_mac"],
        "componente": nome_comp,
        "em_andamento": em_andamento,
        "data_inicio": inicio_iso,
        "data_fim": fim_iso,
        "capturas_consecutivas": contagem,
    }


def avaliar_e_gravar_falhas_s3(bucket, payloads):
    """
    Avalia uma lista de capturas (snapshots) de um embarcado, atualiza a máquina de estados
    no S3 (contagem de capturas críticas seguidas) e cria/atualiza os arquivos JSON de falha
    quando um componente atinge ou encerra uma sequência de >= LIMITE_CAPTURAS_FALHA.

    Parâmetros:
        bucket (str): Nome do bucket S3 onde estão os dados GOLD e o estado.
        payloads (list[dict]): Lista de dicionários gerados por montar_payload().

    Retorno:
        list[str]: Lista com as chaves (Keys) dos arquivos de falha criados/atualizados no S3.
    """
    if not payloads:
        return []

    payloads_ordenados = sorted(payloads, key=lambda p: p["timestamp"])
    mac_limpo = normalizar_mac_limpo(payloads_ordenados[0]["endereco_mac"])
    falhas_atualizadas = []

    for tentativa in range(5):
        estado, etag = ler_estado_s3(bucket, mac_limpo)
        gravacoes_pendentes = {}
        exclusoes_pendentes = set()

        ultimo_ts_salvo = (
            parse_timestamp(estado["ultimo_timestamp"])
            if estado.get("ultimo_timestamp")
            else None
        )

        for payload in payloads_ordenados:
            ts_atual = parse_timestamp(payload["timestamp"])

            if ultimo_ts_salvo and ts_atual <= ultimo_ts_salvo:
                continue

            for nome_comp, dados_comp in payload["componentes"].items():
                situacao = dados_comp["situacao"]

                comp_estado = estado["componentes"].setdefault(
                    nome_comp, {"contagem": 0, "inicio": None}
                )

                if situacao in ("Alerta", "Crítico"):
                    if comp_estado["contagem"] == 0:
                        comp_estado["inicio"] = ts_atual.isoformat()

                    comp_estado["contagem"] += 1

                    if comp_estado["contagem"] >= LIMITE_CAPTURAS_FALHA:
                        dt_inicio = parse_timestamp(comp_estado["inicio"])
                        destino = chave_falha_s3(mac_limpo, nome_comp, dt_inicio, em_andamento=True)
                        doc = montar_doc_falha(
                            payload,
                            nome_comp,
                            comp_estado["inicio"],
                            ts_atual.isoformat(),
                            comp_estado["contagem"],
                            em_andamento=True,
                        )
                        gravacoes_pendentes[destino] = doc
                else:
                    if comp_estado["contagem"] >= LIMITE_CAPTURAS_FALHA and comp_estado["inicio"]:
                        dt_inicio = parse_timestamp(comp_estado["inicio"])
                        chave_ativa = chave_falha_s3(mac_limpo, nome_comp, dt_inicio, em_andamento=True)
                        destino_encerrada = chave_falha_s3(mac_limpo, nome_comp, dt_inicio, em_andamento=False)

                        doc = montar_doc_falha(
                            payload,
                            nome_comp,
                            comp_estado["inicio"],
                            ts_atual.isoformat(),
                            comp_estado["contagem"],
                            em_andamento=False,
                        )
                        # Garante que se a falha abriu e fechou no mesmo lote CSV, não grava em em_andamento
                        gravacoes_pendentes.pop(chave_ativa, None)
                        gravacoes_pendentes[destino_encerrada] = doc
                        exclusoes_pendentes.add(chave_ativa)

                    comp_estado["contagem"] = 0
                    comp_estado["inicio"] = None

            ultimo_ts_salvo = ts_atual
            estado["ultimo_timestamp"] = ts_atual.isoformat()

        try:
            salvar_estado_s3(bucket, mac_limpo, estado, etag)

            for destino, doc_falha in gravacoes_pendentes.items():
                s3.put_object(
                    Bucket=bucket,
                    Key=destino,
                    Body=json.dumps(doc_falha, ensure_ascii=False, indent=2).encode("utf-8"),
                    ContentType="application/json",
                )
                falhas_atualizadas.append(destino)

            for chave_antiga in exclusoes_pendentes:
                s3.delete_object(Bucket=bucket, Key=chave_antiga)

            break

        except ClientError as e:
            codigo_erro = e.response["Error"]["Code"]
            if codigo_erro in ("PreconditionFailed", "ConditionalRequestConflict") and tentativa < 4:
                continue

            raise

    return falhas_atualizadas


# Execução local para teste: simula evento da Lambda com bucket e chave.
if __name__ == "__main__":
    import sys

    evento = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": sys.argv[1]},
                    "object": {"key": sys.argv[2]},
                }
            }
        ]
    }
    print(json.dumps(lambda_handler(evento, None), ensure_ascii=False, indent=2))
