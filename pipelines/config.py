import os
import io          
import zipfile     
import logging
import requests
from dataclasses import dataclass
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql.window import Window
from delta.tables import DeltaTable

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
    

    @staticmethod
    def ler_ultima_particao(spark, table_name:str, partition_col:str = "data_processamento") -> DataFrame:

        try:
            #ultima partição em formato DataFrame
            show_partitions_df = spark.sql(f"SHOW PARTITIONS {table_name}")
            # maior partição da data_processamento
            max_partition = show_partitions_df.agg(f.max(partition_col)).collect()[0][0]
            log.info(f"Partição Maxima ({partition_col}): {max_partition}")

            if not max_partition:
                log.error(f"Tabela '{table_name}' não possui dados na coluna '{partition_col}' ")
                raise

            return (spark
                    .table(f"{table_name}")
                    .filter(f.col(partition_col) == max_partition)
                    )
        except Exception as e:
            log.error(f"Falha  ao ler o caminho {table_name}: {e}")
            raise


    @staticmethod
    def normalizar_cnpj(col_name: str):
        """
        Remove máscaras, aceita numéricos sem zeros à esquerda 
        e padroniza para exatamente 14 dígitos.
        """
        
        #Forçar a coluna a virar texto antes de usar funções de string!
        coluna_como_texto = f.col(col_name).cast("string")
        
        # Limpa tudo que não for número (0 a 9)
        cnpj_limpo = f.regexp_replace(coluna_como_texto, r"[^0-9]", "")
        
        return (
            f.when(
                # É válido se tiver pelo menos 1 número e no máximo 14
                (f.length(cnpj_limpo) > 0) & (f.length(cnpj_limpo) <= 14),
                
                # Preenche com "0" à esquerda até bater 14 posições
                f.lpad(cnpj_limpo, 14, "0")
                
            ).otherwise(f.lit(None))  # Inválidos viram null
        )


    @staticmethod
    def aplicar_qualidade_e_separar(
        df: DataFrame,
        regras: dict          # {"coluna": "tipo_esperado"}
    ) -> tuple[DataFrame, DataFrame]:
        """
        Valida regras de qualidade e separa registros válidos dos inválidos.
        Remove automaticamente as colunas temporárias de cast do DataFrame válido.
        
        Returns:
            (df_valido, df_quarentena)
        """
        condicoes_invalidas = list()
        colunas_cast_temporarias = list() # Rastreia as colunas criadas para limpeza

        for coluna, tipo in regras.items():
            if tipo == "decimal":
                nome_coluna_cast = f"cast_{coluna}"
                colunas_cast_temporarias.append(nome_coluna_cast)
                
                df = df.withColumn(
                    nome_coluna_cast,
                    f.col(coluna).cast(t.DecimalType(22, 2))
                )
                cond_invalida = ( 
                    f.col(coluna).isNotNull() &
                    f.col(nome_coluna_cast).isNull()
                )
                condicoes_invalidas.append(cond_invalida)

            elif tipo == "date":
                nome_coluna_cast = f"cast_{coluna}"
                colunas_cast_temporarias.append(nome_coluna_cast)
                
                df = df.withColumn(
                    nome_coluna_cast,
                    f.to_date(f.col(coluna))
                )
                cond_invalida = (
                    f.col(coluna).isNotNull() &
                    f.col(nome_coluna_cast).isNull()
                )
                condicoes_invalidas.append(cond_invalida)

            elif tipo == "int":
                nome_coluna_cast = f"cast_{coluna}"
                colunas_cast_temporarias.append(nome_coluna_cast)
                
                df = df.withColumn(
                    nome_coluna_cast,
                    f.col(coluna).cast(t.IntegerType())
                )
                cond_invalida = ( 
                    f.col(coluna).isNotNull() &
                    f.col(nome_coluna_cast).isNull()
                )
                condicoes_invalidas.append(cond_invalida)
            
            elif tipo == "not_null":
                cond_invalida = f.col(coluna).isNull()
                condicoes_invalidas.append(cond_invalida)
            
        # Agrupa todas as condições de erro com o operador OR (|)
        condicao_quarentena = condicoes_invalidas[0]
        for c in condicoes_invalidas[1:]:
            condicao_quarentena = condicao_quarentena | c

        df = df.withColumn("_quarentena", condicao_quarentena)
        
        df = df.withColumn(
            "_motivo_quarentena",
            f.when(
                f.col("_quarentena"), 
                f.lit(f"Falha de qualidade nas colunas monitoradas: {list(regras.keys())}")
            ).otherwise(f.lit(None))
        )
        
        df_valido     = df.filter(~f.col("_quarentena")).drop("_quarentena", "_motivo_quarentena", *colunas_cast_temporarias)
        
        # Mantemos as colunas cast no df_quarentena porque elas ajudam a diagnosticar qual coluna falhou
        df_quarentena = df.filter(f.col("_quarentena"))

        return df_valido, df_quarentena
    

    @staticmethod
    def remover_duplicatas(df: DataFrame, chave_negocio: list, coluna_ordenacao: str) -> tuple[DataFrame, DataFrame]:
        window_check = Window.partitionBy(chave_negocio).orderBy(f.col(coluna_ordenacao).desc())
        df = df.withColumn("_num_linha", f.row_number().over(window_check))


        df_duplicadas_quarentena = (df
                                .filter(f.col("_num_linha") > 1)
                                .withColumn("_motivo_quarentena", f.lit(f"Descarte de cópia - Chave duplicada: {chave_negocio}"))
                                .drop("_num_linha")
                               )
        df_valido = (df
                           .filter(f.col("_num_linha") == 1)
                           .drop("_num_linha")
                           )

        return df_valido, df_duplicadas_quarentena


    @staticmethod
    def salvar_quarentena(spark, df_quarentena: DataFrame, tabela_origem: str, data_proc: int):
        
        # Se não houver dados ruins, não faz nada
        if df_quarentena.count() == 0:
            return

        # 1. Separamos o motivo e transformamos o RESTO das colunas em um JSON string único
        # Removemos o motivo da string JSON para não ficar redundante
        colunas_dados = [c for c in df_quarentena.columns if c not in ["_motivo_quarentena"]]
        
        df_unificado = (df_quarentena
            .withColumn("_dados_raw", f.to_json(f.struct(*colunas_dados)))
            .select(
                f.lit(tabela_origem).alias("_tabela_origem"),
                f.lit(data_proc).cast("int").alias("_data_proc"),
                f.current_timestamp().alias("_capturado_em"),
                f.col("_motivo_quarentena").alias("_motivo_quarentena"),
                f.col("_source_url").alias("_source_url"), 
                f.col("_dados_raw")
            )
        )

        tabela_destino = "workspace.case_spark_cvm.silver_quarentena"

        # 2. GARANTIA DE IDEMPOTÊNCIA: Limpeza cirúrgica antes de inserir
        if spark.catalog.tableExists(tabela_destino):
            # Se a tabela global já existe, deletamos apenas o lote antigo DESTA pipeline DESTE dia
            delta_table = DeltaTable.forName(spark, tabela_destino)
            delta_table.delete(f"_tabela_origem = '{tabela_origem}' AND _data_proc = {data_proc}")
            
        # 3. Escrita segura via Append (o delete acima garante que não haverá duplicados do mesmo dia)
        (df_unificado.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true") # Garante flexibilidade se a estrutura de metadados mudar
            .saveAsTable(tabela_destino))
            
        log.warning(f"[QUALIDADE] {df_quarentena.count()} registros isolados na quarentena unificada de '{tabela_origem}'")


    @staticmethod
    def upsert_silver(spark, df_novo, tabela_destino: str, chave_negocio: list):
        """
        Executa MERGE INTO (upsert) na tabela Silver usando a chave de negócio.
        """
        
        # Se a tabela já existir no Unity Catalog, faz o MERGE
        if spark.catalog.tableExists(tabela_destino):
            delta_table = DeltaTable.forName(spark, tabela_destino)

            # Monta a regra de "match" dinâmica baseada na lista de chaves
            condicao_merge = " AND ".join([f"destino.{col} = origem.{col}" for col in chave_negocio])

            (delta_table.alias("destino")
                 .merge(df_novo.alias("origem"), condicao_merge)
                 .whenMatchedUpdateAll()    # Se já existir, atualiza tudo (Pega as correções da CVM)
                 .whenNotMatchedInsertAll() # Se for novo, insere
                 .execute())
            log.info(f"MERGE executado com sucesso na tabela {tabela_destino}")
            
        else:
            # Se for a primeira vez rodando, cria a tabela do zero
            log.info(f"Tabela {tabela_destino} não existe. Criando com carga inicial...")
            (df_novo.write
                  .format("delta")
                  .mode("overwrite")
                  # Particionamento por data de negócio precisa vir do df_novo
                  # Obs: Se for particionar, faça no notebook antes de chamar essa função!
                  .saveAsTable(tabela_destino))

