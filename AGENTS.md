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
