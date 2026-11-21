# ETL SILVER2GOLD RESULTING JSON format:

```bash
{
    "ip": "",
    "id_empresa": 1,
    "id_embarcado": 1,
    "endereco_mac": "1A:2B:3C:4D:5E:6F",
    "timestamp": "2026-11-20 20:56:32",
    "componentes": {
        "cpu": {
            "nome": "CPU",
            "situacao": "Normal",
            "metricas": {
                "uso": {
                    "nome": "Uso",
                    "situacao": "Normal",
                    "unidade_medida": "%",
                    "valor": 0.0,
                    "valor_pct": 0.0,
                    "total": 0.0,
                    "limite_max": 0.0,
                    "limite_min": 0.0,
                    "por_nucleo": []
                },
                "freq": {
                    "nome": "Frequência",
                    "situacao": "Normal",
                    "unidade_medida": "MHz",
                    "valor": 0.0,
                    "valor_pct": 0.0,
                    "total": 0.0,
                    "limite_max": 0.0,
                    "limite_min": 0.0
                }
            },
            "maior_processo": {
                "nome": "",
                "unidade_medida": "%",
                "valor": 0.0
            },
            "qtd_nucleos_fisicos": 0,
            "qtd_nucleos_logicos": 0,
            "load_avg_1m": 0.0,
            "load_avg_5m": 0.0,
            "load_avg_15m": 0.0
        },
        "ram": {
            "nome": "Memória RAM",
            "situacao": "Normal",
            "metricas": {
                "uso": {
                    "nome": "Uso",
                    "situacao": "Normal",
                    "unidade_medida": "GB",
                    "valor": 0.0,
                    "valor_pct": 0.0,
                    "total": 0.0,
                    "limite_max": 0.0,
                    "limite_min": 0.0
                }
            },
            "maior_processo": {
                "nome": "",
                "unidade_medida": "%",
                "valor": 0.0
            }
        },
        "swap": {
            "nome": "Memória SWAP",
            "situacao": "Normal",
            "metricas": {
                "uso": {
                    "nome": "Uso",
                    "situacao": "Normal",
                    "unidade_medida": "GB",
                    "valor": 0.0,
                    "valor_pct": 0.0,
                    "total": 0.0,
                    "limite_max": 0.0,
                    "limite_min": 0.0
                }
            }
        },
        "disco": {
            "nome": "Disco",
            "situacao": "Normal",
            "metricas": {
                "uso": {
                    "nome": "Uso",
                    "situacao": "Normal",
                    "unidade_medida": "GB",
                    "valor": 0.0,
                    "valor_pct": 0.0,
                    "total": 0.0,
                    "limite_max": 0.0,
                    "limite_min": 0.0
                }
            }
        },
        "rede": {
            "nome": "Rede",
            "situacao": "Normal",
            "metricas": {
                "download": {
                    "nome": "Download",
                    "situacao": "Normal",
                    "unidade_medida": "Mbps",
                    "valor": 0.0,
                    "valor_pct": null,
                    "total": null,
                    "limite_max": 0.0,
                    "limite_min": 0.0
                },
                "upload": {
                    "nome": "Upload",
                    "situacao": "Normal",
                    "unidade_medida": "Mbps",
                    "valor": 0.0,
                    "valor_pct": null,
                    "total": null,
                    "limite_max": 0.0,
                    "limite_min": 0.0
                },
                "latencia": {
                    "nome": "Latência",
                    "situacao": "Normal",
                    "unidade_medida": "Ms",
                    "valor": 0.0,
                    "valor_pct": null,
                    "total": null,
                    "limite_max": 0.0,
                    "limite_min": 0.0
                }
            },
            "pacotes": {
                "enviados": {
                    "bytes": 0,
                    "total": 0
                },
                "recebidos": {
                    "bytes": 0,
                    "total": 0
                },
                "perdidos": {
                    "chegando": 0,
                    "saindo": 0,
                    "total": 0
                }
            }
        }
    }
}
```

# ETL TRACES RESULTING JSON format:

```bash
{
  "id_trace": "trc-<uuid>",
  "id_embarcado": "1",
  "timestamp_inicio": "2026-11-04 18:45:10",
  "timestamp_fim": "2026-11-04 18:45:10",
  "startTime": 1730745910125,
  "totalDuration": 1320,
  "spans": [
    {
      "etapa": "1_IDENTIFICAR_VEICULOS",
      "timestamp": "2026-11-04 18:45:10",
      "startTime": 1730745910125,
      "duration": 150,
      "status": "OK",
      "detalhe": "XX veículos identificados no período",
      "extra": {
        "cpu": {
          "valor": 45.2,
          "unidadeMedida": "%",
          "status": "Normal"
        },
        "ram": {
          "valor": 60.1,
          "unidadeMedida": "%",
          "status": "Normal"
        },
        "disco": {
          "valor": 22.4,
          "unidadeMedida": "%",
          "status": "Normal"
        },
        "rede": {
          "download": {
            "valor": 1.1,
            "unidadeMedida": "Mbps",
            "status": "Normal"
          },
          "upload": {
            "valor": 0.8,
            "unidadeMedida": "Mbps",
            "status": "Normal"
          }
        }
      }
    },
    {
      "etapa": "2_PERFILAMENTO_VEICULOS",
      "timestamp": "2026-11-04 18:45:10",
      "startTime": 1730745910125,
      "duration": 150,
      "status": "OK",
      "detalhe": "Sistema de perfilamento digitalizou XX veículos no período",
      "extra": { // same as in span-1... }
    },
    {
      "etapa": "3_CLASSIFICACAO_VEICULOS",
      "timestamp": "2026-11-04 18:46:10",
      "startTime": 1730745910125,
      "duration": 150,
      "status": "FALHA",
      "detalhe": "Sistema classificou XX veículos no perído; YY falharam",
      "extra": { // same as in span-1... }
    },
    {
      "etapa": "4_ENCAMINHAMENTO_COBRANCA",
      "timestamp": "2026-11-04 18:47:10",
      "startTime": 1730745910125,
      "duration": 150,
      "status": "OK",
      "detalhe": "XX cobranças encaminhadas com sucesso no período",
      "extra": { // same as in span-1... }
    },
    {
      "etapa": "5_CAPTURA_METRICAS",
      "timestamp": "2026-11-04 18:48:10",
      "startTime": 1730745910125,
      "duration": 150,
      "status": "OK",
      "detalhe": "Sistema FlowTech capturou XX métricas no período",
      "extra": { // same as in span-1... }
    }
  ]
}
```


