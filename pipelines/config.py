import os
from dataclasses import dataclass

@dataclass(frozen=True)
class PipelineConfig:
    # 1. Estrutura Unity Catalog
    CATALOG:str = "workspace"
    SCHEMA: str = "case_spark_cvm"

    # 2. Caminho base dos Volumes (Onde os arquivos fisícos moram)
    VOLUME_BASE: str = f"/Volumes/{CATALOG}/{SCHEMA}"

    # 3. caminho das Camadas da Arquitetura Medalhão
    RAW_PATH: str = f"{VOLUME_BASE}/raw"
    BRONZE_PATH: str =  f"{VOLUME_BASE}/bronze"
    SILVER_PATH: str = f"{VOLUME_BASE}/silver"
    GOLD_PATH: str = f"{VOLUME_BASE}/gold"

    # 4. Caminho de Auditoria e Logs
    AUDIT_PATH: str = f"{VOLUME_BASE}/_auditoria/pipeline_runs"

# Instanciamos a classe para exportar o objeto pronto para uso
CFG = PipelineConfig()