import io
import json
import logging
import random
import uuid
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3

# === VARIÁVEIS GLOBAIS === #
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

PREFIXO_METRICAS = "03-gold/metricas/"
PREFIXO_TRACES = "03-gold/traces/"
ETAPAS = [
    "1_IDENTIFICAR_VEICULOS",
    "2_PERFILAMENTO_VEICULOS",
    "3_CLASSIFICACAO_VEICULOS",
    "4_ENCAMINHAMENTO_COBRANCA",
    "5_CAPTURA_METRICAS",
]


def parse_timestamp(texto):
    texto = str(texto or "").strip()
    try:
        return datetime.strptime(texto, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            return datetime.fromisoformat(texto.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            return datetime.utcnow()


def datetime_to_epoch_ms(dt):
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def epoch_ms_to_datetime(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def melhor_situacao_componentes(metricas):
    situacoes = []
    comps = metricas.get("componentes", {})
    for nome in ("cpu", "ram", "disco", "rede"):
        situacoes.append(str(comps.get(nome, {}).get("situacao", "Normal")))

    if "Crítico" in situacoes:
        return "Crítico"
    if "Alerta" in situacoes:
        return "Alerta"
    return "Normal"


def chance_falha_por_situacao(situacao, etapa):
    if situacao == "Crítico":
        base = 0.55
    elif situacao == "Alerta":
        base = 0.25
    else:
        base = 0.08

    # Etapa de classificação tende a ter um pouco mais de falha.
    if etapa == "3_CLASSIFICACAO_VEICULOS":
        base += 0.10
    return min(base, 0.90)


def extrair_extra(registro):
    componentes = registro.get("componentes", {})
    cpu = componentes.get("cpu", {})
    ram = componentes.get("ram", {})
    disco = componentes.get("disco", {})
    rede = componentes.get("rede", {})

    cpu_uso = cpu.get("metricas", {}).get("uso", {})
    ram_uso = ram.get("metricas", {}).get("uso", {})
    disco_uso = disco.get("metricas", {}).get("uso", {})
    rede_download = rede.get("metricas", {}).get("download", {})
    rede_upload = rede.get("metricas", {}).get("upload", {})

    return {
        "cpu": {
            "valor": float(cpu_uso.get("valor_pct", cpu_uso.get("valor", 0.0)) or 0.0),
            "unidadeMedida": "%",
            "status": cpu.get("situacao", "Normal"),
        },
        "ram": {
            "valor": float(ram_uso.get("valor_pct", ram_uso.get("valor", 0.0)) or 0.0),
            "unidadeMedida": "%",
            "status": ram.get("situacao", "Normal"),
        },
        "disco": {
            "valor": float(disco_uso.get("valor_pct", disco_uso.get("valor", 0.0)) or 0.0),
            "unidadeMedida": "%",
            "status": disco.get("situacao", "Normal"),
        },
        "rede": {
            "download": {
                "valor": float(rede_download.get("valor", 0.0) or 0.0),
                "unidadeMedida": "Mbps",
                "status": rede.get("situacao", "Normal"),
            },
            "upload": {
                "valor": float(rede_upload.get("valor", 0.0) or 0.0),
                "unidadeMedida": "Mbps",
                "status": rede.get("situacao", "Normal"),
            },
        },
    }


def sortear_durations(total_duration, rng):
    total = max(int(total_duration), 500)
    pesos = [rng.uniform(0.8, 1.2) for _ in ETAPAS]
    soma_pesos = sum(pesos)
    duracoes = [max(50, int(total * (p / soma_pesos))) for p in pesos]

    delta = total - sum(duracoes)
    duracoes[-1] = max(50, duracoes[-1] + delta)
    return duracoes


def montar_detalhe(etapa, status, xx, yy):
    if etapa == "1_IDENTIFICAR_VEICULOS":
        if status == "FALHA":
            return f"Falha na identificação: {yy} de {xx} veículos não puderam ser identificados no período"
        return f"{xx} veículos identificados no período"

    if etapa == "2_PERFILAMENTO_VEICULOS":
        if status == "FALHA":
            return f"Falha no perfilamento: {yy} de {xx} veículos não foram perfilados no período"
        return f"Sistema de perfilamento digitalizou {xx} veículos no período"

    if etapa == "3_CLASSIFICACAO_VEICULOS":
        if status == "FALHA":
            return f"Sistema classificou {xx} veículos no período; {yy} falharam"
        return f"Sistema classificou {xx} veículos no período; 0 falharam"

    if etapa == "4_ENCAMINHAMENTO_COBRANCA":
        if status == "FALHA":
            return f"Falha no encaminhamento: {yy} cobranças rejeitadas de {xx}"
        return f"{xx} cobranças encaminhadas com sucesso no período"

    return f"Sistema FlowTech capturou {xx} métricas no período"


def escolher_registro_para_etapa(registros, indice_etapa):
    if len(registros) == 1:
        return registros[0]
    pos = round(indice_etapa * (len(registros) - 1) / (len(ETAPAS) - 1))
    return registros[pos]


def montar_trace(registros):
    primeiro = registros[0]
    ultimo = registros[-1]

    timestamp_inicio_txt = str(primeiro.get("timestamp", ""))
    timestamp_fim_txt = str(ultimo.get("timestamp", ""))

    dt_inicio = parse_timestamp(timestamp_inicio_txt)
    dt_fim = parse_timestamp(timestamp_fim_txt)
    inicio_ms = datetime_to_epoch_ms(dt_inicio)
    fim_ms = datetime_to_epoch_ms(dt_fim)
    total_duration = max(500, fim_ms - inicio_ms)

    seed_base = f"{primeiro.get('id_embarcado', '')}|{timestamp_inicio_txt}"
    rng = random.Random(seed_base)

    duracoes = sortear_durations(total_duration, rng)
    spans = []
    corrente_ms = inicio_ms

    for i, etapa in enumerate(ETAPAS):
        registro = escolher_registro_para_etapa(registros, i)
        extra = extrair_extra(registro)
        situacao = melhor_situacao_componentes(registro)

        if etapa == "5_CAPTURA_METRICAS":
            status_span = "OK"
        else:
            chance = chance_falha_por_situacao(situacao, etapa)
            status_span = "FALHA" if rng.random() < chance else "OK"

        if etapa == "5_CAPTURA_METRICAS":
            xx = len(registros) * rng.randint(20, 60)
            yy = 0
        else:
            xx = rng.randint(80, 300)
            yy = rng.randint(1, max(1, int(xx * 0.20))) if status_span == "FALHA" else 0

        spans.append(
            {
                "etapa": etapa,
                "timestamp": epoch_ms_to_datetime(corrente_ms).strftime("%Y-%m-%d %H:%M:%S"),
                "startTime": corrente_ms,
                "duration": duracoes[i],
                "status": status_span,
                "detalhe": montar_detalhe(etapa, status_span, xx, yy),
                "extra": extra,
            }
        )
        corrente_ms += duracoes[i]

    return {
        "id_trace": f"trc-{uuid.uuid4()}",
        "id_embarcado": str(primeiro.get("id_embarcado", "")),
        "timestamp_inicio": timestamp_inicio_txt,
        "timestamp_fim": timestamp_fim_txt,
        "startTime": inicio_ms,
        "totalDuration": sum(duracoes),
        "spans": spans,
    }


def chave_trace(chave_metricas):
    relativa = chave_metricas[len(PREFIXO_METRICAS):]
    if relativa.lower().endswith(".jsonl"):
        relativa = relativa[:-6] + "_trace.json"
    else:
        relativa = relativa + "_trace.json"
    return PREFIXO_TRACES + relativa


def processar_objeto(bucket, chave):
    corpo = s3.get_object(Bucket=bucket, Key=chave)["Body"].read()
    registros = []

    for i, linha in enumerate(io.BytesIO(corpo).read().decode("utf-8").splitlines(), start=1):
        linha = linha.strip()
        if not linha:
            continue
        try:
            registros.append(json.loads(linha))
        except json.JSONDecodeError:
            logger.warning("Linha JSON inválida ignorada (%s linha %d)", chave, i)

    stats = {"origem": chave, "linhas_lidas": len(registros)}
    if not registros:
        logger.warning("Nenhum registro válido encontrado: %s", json.dumps(stats, ensure_ascii=False))
        return {**stats, "status": "vazio"}

    trace = montar_trace(registros)
    destino = chave_trace(chave)

    s3.put_object(
        Bucket=bucket,
        Key=destino,
        Body=json.dumps(trace, ensure_ascii=False, indent=2, allow_nan=False).encode("utf-8"),
        ContentType="application/json",
    )

    logger.info("ETL GOLD_METRICAS->TRACES concluída: %s", json.dumps({**stats, "destino": destino}, ensure_ascii=False))
    return {**stats, "destino": destino, "status": "ok"}


def lambda_handler(event, context):
    resultados = []

    for registro in event.get("Records", []):
        bucket = registro["s3"]["bucket"]["name"]
        chave = unquote_plus(registro["s3"]["object"]["key"])

        if not (chave.startswith(PREFIXO_METRICAS) and chave.lower().endswith(".jsonl")):
            logger.info("Ignorando objeto fora de %s*.jsonl: %s", PREFIXO_METRICAS, chave)
            continue

        try:
            resultados.append(processar_objeto(bucket, chave))
        except Exception as erro:  # pragma: no cover
            logger.exception("Erro ao processar %s", chave)
            resultados.append({"origem": chave, "status": "erro", "mensagem": str(erro)})

    return {"processados": resultados}


if __name__ == "__main__":
    import sys

    evento = {"Records": [{"s3": {"bucket": {"name": sys.argv[1]}, "object": {"key": sys.argv[2]}}}]}
    print(json.dumps(lambda_handler(evento, None), ensure_ascii=False, indent=2))
