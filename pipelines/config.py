import os
import io          
import zipfile     
import logging
import requests
from dataclasses import dataclass
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

@dataclass(frozen=True)
class PipelineRoute:
    """
    Mapeamento centralizado de rotas e estruturas do Databricks.
    
    A utilização do `frozen=True` garante a imutabilidade dessas rotas
    durante a execução dos pipelines, prevenindo alterações acidentais.
    """

    # 1. Estrutura Unity Catalog
    CATALOG:str = "workspace"
    SCHEMA: str = "case_spark_cvm"

    # 2. Caminho base dos Volumes (Onde os arquivos fisícos moram)
    VOLUME_BASE: str = f"/Volumes/{CATALOG}/{SCHEMA}"

    # 3. Caminho para criação da Camada Intermediaria (RAW)
    RAW_PATH: str = f"{VOLUME_BASE}/raw"

    # 4. Caminho para criação das Tabelas
    TABLE_BASE: str = f"{CATALOG}.{SCHEMA}"

    # 5. Caminho de Auditoria e Logs
    AUDIT_PATH: str = f"{VOLUME_BASE}/auditoria/pipeline_runs"

# Instanciamos a classe para exportar o objeto pronto para uso
ROUTES = PipelineRoute()


class PipelineConfig:
    """
    Coleção de métodos estáticos para operações de rede e sistema de arquivos.
    
    Atua como um cliente de API blindado, gerenciando sessões HTTP com
    políticas de retentativa (retry) e extração de dados compactados.
    """
    
    @staticmethod
    def _build_session(retries:int=3, backoff:float=1.5) -> requests.Session:
        """
        Cria uma sessão HTTP com política de Retry (Exponential Backoff).
        
        Args:
            retries (int): Número máximo de tentativas antes de falhar. Padrão é 3.
            backoff (float): Fator multiplicador de tempo entre as tentativas. Padrão é 1.5.
            
        Returns:
            requests.Session: Sessão HTTP configurada com o adaptador de resiliência.
        """
        
        session = requests.Session()
        retry = Retry(total=retries, backoff_factor=backoff, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retry))
        return session
    
    @staticmethod
    def retorno_json_url(url:str):
        """
        Acessa uma URL via GET usando a sessão blindada e converte o retorno em JSON.
        
        Args:
            url (str): O link completo da API alvo.
            
        Returns:
            dict: Objeto JSON parseado com os dados da resposta.
            
        Raises:
            requests.exceptions.HTTPError: Se o servidor retornar erro após as retentativas.
        """

        headers = {"User-Agent": "Mozilla/5.0"}

        try:
            session = PipelineConfig._build_session() 
            response = session.get(url, headers=headers, timeout=60)
            response.raise_for_status()

            return response.json() 
            
        except Exception as e:
            log.error(f"Falha na requisição para a URL {url}: {e}")
            raise


    @staticmethod
    def retorno_text_url(url:str):
        """
        Acessa uma URL via GET usando a sessão blindada e retorna o texto/HTML puro.
        
        Ideal para tarefas de Web Scraping (ex: BeautifulSoup) onde o HTML
        da página precisa ser inspecionado.
        
        Args:
            url (str): O link completo da página alvo.
            
        Returns:
            str: O corpo da resposta em formato de texto.
            
        Raises:
            requests.exceptions.HTTPError: Se o servidor retornar erro após as retentativas.
        """

        headers = {"User-Agent": "Mozilla/5.0"}

        try:
            session = PipelineConfig._build_session() 
            response = session.get(url, headers=headers, timeout=60)
            response.raise_for_status()

            return response.text
            
        except Exception as e:
            log.error(f"Falha na requisição para a URL {url}: {e}")
            raise


    @staticmethod
    def baixar_e_extrair_zip(url: str, raw_path: str) -> list:
        """
        Realiza o download de um arquivo ZIP via URL, extrai em memória e 
        salva apenas os arquivos CSV no caminho RAW especificado.
        
        Esta função é idempotente quanto à criação de diretórios e atua de
        forma silenciosa (void), ou seja, apenas grava no disco sem retornar 
        listas para o escopo do código que a chamou.
        
        Args:
            url (str): Link direto para o arquivo .zip a ser baixado.
            raw_path (str): Caminho físico absoluto da pasta RAW de destino (ex: Databricks Volume).
            
        Returns:
            None
            
        Raises:
            requests.exceptions.HTTPError: Em caso de falha de download.
            zipfile.BadZipFile: Se o arquivo baixado estiver corrompido ou não for um ZIP.
        """
        headers = {"User-Agent": "Mozilla/5.0"}
        arquivos_salvos = [] 
        
        try:
            # Garante que o diretório de destino exista fisicamente
            os.makedirs(raw_path, exist_ok=True)
            
            session = PipelineConfig._build_session()
            response = session.get(url, headers=headers, timeout=120) 
            response.raise_for_status()
            
            # Extração em memória
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                for file_name in z.namelist():
                    if file_name.endswith(".csv"):
                        output_path = os.path.join(raw_path, file_name)

                        with z.open(file_name) as source, open(output_path, "wb") as target:
                            target.write(source.read())

                        log.info(f"  → Extraído: {output_path}")
                        arquivos_salvos.append(output_path) 

            if not arquivos_salvos:
                log.warning(f"ZIP baixado de {url} não continha arquivos CSV.")

                        
        except Exception as e:
            log.error(f"Falha ao processar ZIP da URL {url}: {e}")
            raise

    @staticmethod
    def registrar_auditoria(
        spark,
        audit_path: str,
        pipeline_nome: str,
        tabela_destino: str,
        n_linhas: int,
        status: str,           # "SUCESSO" | "FALHA"
        data_proc: int,
        mensagem: str = "",
    ) -> None:
        from pyspark.sql import Row
        from datetime import datetime

        registro = Row(
            pipeline_nome   = pipeline_nome,
            tabela_destino  = tabela_destino,
            data_proc       = data_proc,
            n_linhas        = n_linhas,
            status          = status,
            mensagem        = mensagem[:2000],
            executado_em    = datetime.now().isoformat(),
        )
        (spark.createDataFrame([registro])
            .write
            .mode("append")
            .format("delta")
            .save(audit_path))




