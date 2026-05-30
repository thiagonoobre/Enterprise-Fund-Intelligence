# 📊 Data Lakehouse CVM & BCB Analytics

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Apache Spark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00AAD2?style=for-the-badge&logo=databricks&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

## 🎯 Sobre o Projeto
Este projeto é uma arquitetura de dados fim a fim (End-to-End) construída no ecossistema **Databricks**, focada na ingestão, processamento e análise de dados de Fundos de Investimento da Comissão de Valores Mobiliários (CVM) e indicadores econômicos do Banco Central do Brasil (BCB).

![Demonstração do Dashboard Analítico](Docs/Gifs/V1-0001_Vídeo.gif)

O objetivo principal é fornecer uma base de dados analítica otimizada (Medallion Architecture) para um painel gerencial no Power BI, permitindo a análise de risco, retorno, captação e ranking de mais de 25 mil fundos de investimento com histórico de 10 anos.

## 🏗️ Arquitetura de Dados (Medallion)
O pipeline adota o padrão de arquitetura Medalhão, estruturado em três camadas lógicas no **Unity Catalog**:

* 🥉 **Bronze (Ingestion):** Captura de dados brutos (JSON/CSV) das APIs do BCB (Selic, CDI, IPCA) e portal de dados abertos da CVM. Ingestão idempotente mantendo rastreabilidade e dados no formato original.
* 🥈 **Silver (Refinement):** Limpeza, conversão de tipos, desduplicação e aplicação de regras de negócio. Implementação de uma **Tabela de Quarentena** para isolar registros anômalos vindos da origem.
* 🥇 **Gold (Business & Serving):** Modelagem dimensional (`Star Schema`) otimizada para o Power BI.
    * **Dimensão:** SCD Tipo 1 para cadastro unificado de Fundos, Classes e Subclasses (`gold_dim_fundo`), resolvendo anomalias de cardinalidade.
    * **Fato:** Tabela de rentabilidade com particionamento mensal (`ano_mes`) processada usando *Dynamic Partition Overwrite*.
    * **Cubos:** Tabelas agregadas com métricas de negócio pré-calculadas (Ranks de Sharpe, Volatilidade e Captação).

## 🚀 Destaques Técnicos e Engenharia

* **Processamento de Big Data (PySpark):** Uso de *Window Functions* complexas e *Partition Pruning* para lidar com massas de dados de 10 anos sem gerar falhas de *Out of Memory (OOM)*.
* **Otimização de Armazenamento (Delta Lake):** Implementação nativa de `OPTIMIZE` e `Z-ORDER BY (cnpj_fundo_classe, dt_comptc)` para garantir *Data Skipping* milissegundos nas queries do BI, prevenindo o *Small Files Problem*.
* **Controle de Qualidade Ativo (IA/Automação):** Desenvolvimento do `CVMQuarantineAgent`, um agente automatizado que varre anomalias de dados rejeitados na camada Silver e envia e-mails estruturados em HTML detalhando a falha na base de origem.
* **Power BI (UX & Analytics):** Dashboard em *Dark Mode* contendo análise de dispersão (Risco x Retorno), navegação avançada (Tooltips/Drill-through) e DAX blindado contra anomalias de cardinalidade (*Auto-Exist*).

## 📂 Estrutura do Repositório
Consulte a documentação técnica dentro das pastas `pipeline/` e `agents/` para entender o funcionamento interno de cada script. O arquivo `config/config.py` contém as classes orientadas a objeto que padronizam a escrita e otimização das tabelas Delta no catálogo.

## 🛠️ Como Executar

1. Importe os notebooks da pasta `pipeline/` para o seu workspace do Databricks.
2. Certifique-se de configurar um *Cluster* com suporte ao Unity Catalog (Databricks Runtime 13.3 LTS ou superior).
3. No Databricks Workflows, configure as *Tasks* e os *Job Parameters* (ex: `serie_codigo`, `serie_nome` para a API do BCB).
4. O `CVMQuarantineAgent` requer a configuração de um *Databricks Secret Scope* contendo as credenciais do servidor SMTP.
