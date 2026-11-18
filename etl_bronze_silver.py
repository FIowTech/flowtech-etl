import io
import json
import logging
from urllib.parse import unquote_plus

import boto3
import numpy as np
import pandas as pd

# === VARIÁVEIS GLOBAIS === #

# Define INFO como o nível mínimo dos logs. Sem isso, o logger (cujo nível padrão
# é WARNING) descartaria todos os logger.info(). Quem envia os logs ao CloudWatch
# é a própria Lambda: tudo o que sai pelo módulo logging já vai para lá.
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Cliente que se comunica com o serviço S3. Fica no escopo global para ser criado
# uma única vez (na primeira execução da Lambda, o "cold start") e reaproveitado
# pelas invocações seguintes, em vez de ser recriado a cada evento.
s3 = boto3.client("s3")

PREFIXO_BRONZE = "01-bronze/metricas/"
PREFIXO_SILVER = "02-silver/metricas/"
DELIMITADOR = ";"
MAX_NUCLEOS = 16    # o mesmo valor definido no Cliente

BYTES_POR_GB = 10**9    # divisor para converter Bytes -> GB (GB decimal)
BITS_POR_MBIT = 10**6   # divisor para converter bits/s -> Mbit/s


# Dicionário que define o esquema (as colunas) do CSV final em SILVER:
# uma linha para cada coluna, e a ORDEM das linhas é a ordem das colunas no CSV.
#
# Estrutura:    "nome_silver": ("nome_bronze", "tipo", divisor, casas_decimais)
# Exemplo:      "cpu_freq_mhz": ("cpu_freq", "num", 1, 1)
#
# Lê-se assim: a coluna "cpu_freq" do BRONZE se chamará "cpu_freq_mhz" no SILVER;
# é do tipo "num"; o valor é dividido por 1 (ou seja, sem conversão de unidade)
# e arredondado para 1 casa decimal.
#
# Tipos (o que acontece com cada valor):
#   "texto": vira string e perde os espaços das pontas
#   "num":   vira número; texto inválido ("abc") ou número negativo vira nulo
#   "pct":   igual a "num", e também vira nulo se for maior que 100
#   "int":   igual a "num", mas só aceita números inteiros (51.5 vira nulo)
#
# divisor:          converte a unidade original na final (ex.: bytes / 10**9 = GB)
# casas_decimais:   None = não arredonda (é o caso de textos e inteiros)
#
# OBS: se "nome_bronze" for None, a coluna NÃO existe no BRONZE: ela é derivada de
# outras colunas e calculada mais abaixo, na função transformar(). Nesse caso,
# só as casas decimais são usadas.
# Ex.: "ram_uso_pct": (None, "pct", 1, 2)
COLUNAS = {
    "endereco_mac":           ("endereco_mac",           "texto", 1, None),
    "timestamp":              ("timestamp",              "texto", 1, None),

    "cpu_uso_pct":            ("cpu_uso_pct",            "pct",   1, 2),
    "qtd_cpu_fisica":         ("qtd_cpu_fisica",         "int",   1, None),
    "qtd_cpu_logica":         ("qtd_cpu_logica",         "int",   1, None),
    "cpu_freq_mhz":           ("cpu_freq",               "num",   1, 1),
    "cpu_freq_total_mhz":     ("cpu_freq_total",         "num",   1, 1),
    "load_avg_1m":            ("load_avg_1m",            "num",   1, 2),
    "load_avg_5m":            ("load_avg_5m",            "num",   1, 2),
    "load_avg_15m":           ("load_avg_15m",           "num",   1, 2),

    "ram_uso_pct":            (None,                     "pct",   1, 2),
    "ram_uso_gb":             ("ram_uso",                "num",   BYTES_POR_GB, 3),
    "ram_total_gb":           ("ram_total",              "num",   BYTES_POR_GB, 3),
    "swap_uso_pct":           (None,                     "pct",   1, 2),
    "swap_uso_gb":            ("swap_uso",               "num",   BYTES_POR_GB, 3),
    "swap_total_gb":          ("swap_total",             "num",   BYTES_POR_GB, 3),
    "disco_uso_pct":          (None,                     "pct",   1, 2),
    "disco_uso_gb":           ("disco_uso",              "num",   BYTES_POR_GB, 3),
    "disco_total_gb":         ("disco_total",            "num",   BYTES_POR_GB, 3),

    "qtd_processos":          ("qtd_processos",          "int",   1, None),
    "maior_processo_cpu":     ("maior_processo_cpu",     "texto", 1, None),
    "maior_processo_cpu_pct": ("maior_processo_cpu_pct", "num",   1, 2),
    "maior_processo_ram":     ("maior_processo_ram",     "texto", 1, None),
    "maior_processo_ram_pct": ("maior_processo_ram_pct", "pct",   1, 2),

    "download_mbps":          ("download_bps",          "num",   BITS_POR_MBIT, 2),
    "upload_mbps":            ("upload_bps",            "num",   BITS_POR_MBIT, 2),
    "latencia_ms":            ("latencia_ms",            "num",   1, 2),
    "packet_loss":            (None,                     "int",   1, None),
    "package_loss_sent":      ("package_loss_sent",      "int",   1, None),
    "package_loss_received":  ("package_loss_received",  "int",   1, None),

    # cria, dinamicamente, as colunas de uso por núcleo: cpu_1, cpu_2 ... cpu_16.
    # O "**" espalha as chaves geradas aqui dentro do dicionário COLUNAS
    **{f"cpu_{i}": (f"cpu_{i}", "pct", 1, 2) for i in range(1, MAX_NUCLEOS + 1)},
}

# Colunas de percentual derivadas de outras duas: (nome, coluna de uso, coluna de total)
# O cálculo é feito em transformar():  percentual = uso / total * 100
PERCENTUAIS_DERIVADOS = [
    ("ram_uso_pct", "ram_uso_gb", "ram_total_gb"),
    ("swap_uso_pct", "swap_uso_gb", "swap_total_gb"),
    ("disco_uso_pct", "disco_uso_gb", "disco_total_gb"),
]

# Responsável por transformar o conteúdo de um CSV do BRONZE em um dataframe no
# formato do SILVER, aplicando o esquema definido em COLUNAS.
# Retorna dois valores:
#   out   -> o dataframe transformado: colunas renomeadas, valores limpos,
#            unidades convertidas e casas decimais aplicadas
#   stats -> dicionário com metadados da transformação: linhas_lidas,
#            linhas_gravadas e valores_anulados
# Não acessa o S3: recebe e devolve dados em memória.
def transformar(corpo: bytes):
    # Termos do pandas usados nesta função:
    #   DataFrame (df, out): uma tabela inteira, com linhas e colunas
    #   Series (serie, n, pct): UMA coluna da tabela, ou seja, uma lista de valores
    #       em que cada valor tem um índice.

    # Cria o dataframe "df" lendo o CSV do BRONZE:
    # a leitura é feita através de um buffer em memória (io.BytesIO) e linhas
    # malformadas, isto é, com colunas a mais, são ignoradas
    df = pd.read_csv(io.BytesIO(corpo), sep=DELIMITADOR, dtype=str, on_bad_lines="skip")

    # Padroniza o nome das colunas: remove espaços das pontas, deixa em minúsculas e
    # troca os espaços do meio por "_". Ex.: " CPU Uso Pct " -> "cpu_uso_pct"
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Guarda a quantidade de linhas lidas ANTES de remover as duplicatas. 
    stats = {"linhas_lidas": len(df)}

    # Remove linhas duplicadas: o mesmo endereco_mac com o mesmo timestamp.
    # keep="last" mantém apenas a última ocorrência
    df = df.drop_duplicates(subset=["endereco_mac", "timestamp"], keep="last")

    # Cria o dataframe de saída, ainda sem colunas, com os mesmos rótulos de linha
    # (índice) de df. Com o mesmo índice, cada coluna nova se encaixa na linha certa:
    # o pandas alinha os valores pelo rótulo da linha, e não pela posição
    out = pd.DataFrame(index=df.index)
    ausentes = []   # colunas previstas em COLUNAS que não vieram no CSV
    anulados = 0    # valores que existiam e foram descartados na limpeza

    # Percorre o esquema COLUNAS (e não as colunas do CSV): colunas do BRONZE que
    # não estão em COLUNAS são descartadas.
    # OBS: as casas decimais são aplicadas depois das derivações, mais abaixo
    for nome_silver, (nome_bronze, tipo, divisor, _) in COLUNAS.items():
        # Colunas derivadas (nome_bronze None) não existem no BRONZE: são calculadas
        # depois deste loop. Segue para a próxima
        if nome_bronze is None:
            continue

        # "serie" é a coluna do BRONZE que será tratada agora.
        # Se ela não veio no CSV, cria uma Series só de nulos (NaN), com o mesmo
        # índice de df, para manter o esquema do SILVER, e registra em "ausentes"
        if nome_bronze in df.columns:
            serie = df[nome_bronze]
        else:
            ausentes.append(nome_bronze)
            serie = pd.Series(np.nan, index=df.index)

        # Colunas Texto: converte para o tipo string do pandas, remove os espaços 
        # das pontas e guarda em out. O continue pula o tratamento numérico abaixo
        if tipo == "texto":
            out[nome_silver] = serie.astype("string").str.strip()
            continue

        # Colunas numéricas. Cada linha abaixo gera uma nova Series "n" a partir da
        # anterior:
        # 1) to_numeric converte texto em número; o que não for número ("abc") vira
        #    nulo (errors="coerce"). Em seguida, divide todos os valores pelo divisor
        n = pd.to_numeric(serie, errors="coerce") / divisor

        # 2) where(condição) mantém o valor onde a condição é verdadeira e põe nulo
        #    onde é falsa. Aqui: números negativos viram nulo
        n = n.where(n >= 0)

        # 3) porcentagem: valores acima de 100 também viram nulo
        if tipo == "pct":
            n = n.where(n <= 100)

        # 4) inteiro: só mantém números inteiros (51.5 vira nulo). "Int64" é o tipo
        #    inteiro do pandas que aceita nulos (o int comum não aceita)
        if tipo == "int":
            n = n.where(n % 1 == 0).astype("Int64")

        # Conta quantos valores existiam em "serie" e viraram nulo em "n" por serem
        # inválidos (texto, negativo, acima de 100 ou não inteiro). Campos que já
        # vinham vazios não entram na conta, pois já eram nulos.
        # notna() marca com True cada valor não nulo, e sum() soma os True
        anulados += int(serie.notna().sum() - n.notna().sum())

        # guarda a coluna tratada em out
        out[nome_silver] = n

    # Calcula os percentuais derivados: uso / total * 100
    #  - se o total for 0, o percentual é 0
    #  - se o resultado passar de 100, vira nulo
    for nome, uso, total in PERCENTUAIS_DERIVADOS:
        pct = (out[uso] / out[total] * 100).mask(out[total] == 0, 0.0)
        out[nome] = pct.where(pct <= 100)

    # Soma os dois contadores de pacotes descartados (enviados + recebidos).
    # Se um dos dois for nulo, o resultado também é nulo
    out["packet_loss"] = out["package_loss_sent"] + out["package_loss_received"]

    # Arredonda cada coluna para o número de casas definido em COLUNAS. Colunas com
    # casas_decimais None (textos e inteiros) não são arredondadas
    for nome_silver, (_, _, _, casas) in COLUNAS.items():
        if casas is not None:
            out[nome_silver] = out[nome_silver].round(casas)

    # Ajustes finais:
    #  - out[list(COLUNAS)]: seleciona as colunas e as coloca na ordem de COLUNAS
    #  - sort_values("timestamp", kind="stable"): ordena as LINHAS por timestamp
    #  - reset_index(drop=True): renumera as linhas de 0 a n-1 (o índice fica
    # bagunçado depois de remover as duplicatas)
    out = out[list(COLUNAS)].sort_values("timestamp", kind="stable").reset_index(drop=True)

    # Se alguma coluna prevista em COLUNAS não veio no CSV, informa no log
    if ausentes:
        logger.warning("Colunas ausentes no bronze: %s", ausentes)

    # Completa os metadados com quantas linhas serão gravadas e quantos valores
    # foram anulados
    stats.update(linhas_gravadas=len(out), valores_anulados=anulados)

    return out, stats


# Responsável por montar a chave SILVER a partir da chave BRONZE do arquivo original.
# Mantém o mesmo caminho e o mesmo nome, trocando só o prefixo:
# 01-bronze/metricas/ano=2026/.../arquivo.csv -> 02-silver/metricas/ano=2026/.../arquivo.csv
def chave_silver(chave_bronze: str):
    return PREFIXO_SILVER + chave_bronze[len(PREFIXO_BRONZE):]

# Responsável por buscar o arquivo no bucket, transformá-lo de BRONZE para SILVER
# e gravar o resultado no prefixo SILVER
def processar_objeto(bucket: str, chave: str):
    # pega o conteúdo (em bytes) do arquivo CSV armazenado em BRONZE
    corpo = s3.get_object(Bucket=bucket, Key=chave)["Body"].read()

    # transforma os dados de BRONZE para SILVER
    df, stats = transformar(corpo)

    # adiciona a chave de origem aos metadados da transformação
    stats["origem"] = chave

    # Se o dataframe está vazio, o arquivo não tinha nenhuma linha de dados (por
    # exemplo, só o cabeçalho): informamos no log e retornamos os metadados com
    # status="vazio", sem gravar nada.
    # Erros reais (arquivo ilegível, coluna endereco_mac ausente...) não são tratados
    # aqui: geram uma exceção, e a Lambda tenta executar de novo
    if df.empty:
        logger.warning("Nenhuma linha gravada: %s", json.dumps(stats, ensure_ascii=False))
        return {**stats, "status": "vazio"}

    # Se não, adicionamos a chave de destino aos metadados da transformação
    stats["destino"] = chave_silver(chave)

    # Grava o dataframe transformado no prefixo SILVER. to_csv() gera o texto do CSV
    # (index=False não escreve o índice das linhas) e encode() o converte em bytes,
    # que é o que o S3 recebe
    s3.put_object(
        Bucket=bucket,
        Key=stats["destino"],
        Body=df.to_csv(index=False, sep=DELIMITADOR, lineterminator="\n").encode("utf-8"),
        ContentType="text/csv",
    )

    # informamos o sucesso da ETL através de um log e retornamos os metadados da
    # transformação, com status="ok"
    logger.info("ETL concluída: %s", json.dumps(stats, ensure_ascii=False))
    return {**stats, "status": "ok"}

# Ponto de entrada da Lambda: a AWS chama esta função automaticamente a cada evento
# do S3 (arquivo novo em BRONZE). Ela lê os dados do evento e chama
# processar_objeto() para cada arquivo
def lambda_handler(event, context):
    resultados = []

    # Um evento pode trazer vários arquivos; cada um é um item da lista "Records"
    for registro in event.get("Records", []):
        # pega o nome do bucket seguindo a estrutura do JSON de Records
        bucket = registro["s3"]["bucket"]["name"]

        # pega a chave do objeto seguindo a estrutura do JSON de Records.
        # No evento, a chave vem codificada para URL ("ano=2026" chega como
        # "ano%3D2026"); unquote_plus() a decodifica para o nome real
        chave = unquote_plus(registro["s3"]["object"]["key"])

        # Se a chave NÃO estiver sob o prefixo BRONZE ou NÃO for um arquivo .csv
        # gera um log e pula para o próximo arquivo da lista
        if not (chave.startswith(PREFIXO_BRONZE) and chave.lower().endswith(".csv")):
            logger.info("Ignorando objeto fora de %s*.csv: %s", PREFIXO_BRONZE, chave)
            continue

        # Se passou na checagem, chamamos processar_objeto() e adicionamos seu
        # resultado à lista de resultados
        resultados.append(processar_objeto(bucket, chave))

    return {"processados": resultados}


# Este bloco só roda quando o arquivo é executado diretamente (python etl_bronze_silver.py);
# na Lambda, o arquivo é importado e o bloco é ignorado. Serve para testar a ETL
# localmente: basta passar o nome do bucket e a chave do arquivo que deseja tratar.
# Ele monta um evento de mentira, com o mesmo formato do evento real do S3.
# OBS: o arquivo precisa existir no datalake, estar em BRONZE e ser um CSV, e a máquina
# precisa ter credenciais AWS configuradas
if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    evento = {"Records": [{"s3": {"bucket": {"name": sys.argv[1]}, "object": {"key": sys.argv[2]}}}]}
    print(json.dumps(lambda_handler(evento, None), ensure_ascii=False, indent=2))