# 🚕 iFood Case – Pipeline de Dados com PySpark e Databricks

Este projeto foi desenvolvido como parte de um **case técnico para a posição de Data Engineer no iFood**, utilizando o **Databricks Free Edition** e versionado com **GitHub**.

A solução foca na ingestão, transformação e análise de dados de táxis de Nova York com uma arquitetura em camadas (Bronze, Silver, Gold), utilizando **PySpark**, **Delta Lake** e armazenamento em **S3** via **Unity Catalog**.

---

## 🎯 Objetivo

- Ingerir dados das corridas de Yellow Taxis de NY (jan a mai/2023).
- Armazenar e estruturar os dados no formato Delta Lake em camadas (Bronze → Silver → Gold).
- Criar tabelas no Unity Catalog.
- Responder duas perguntas analíticas usando PySpark e SQL.

---

## 📚 Perguntas Respondidas

1. Qual a **média do valor total (`total_amount`)** recebido por mês?
2. Qual a **média de passageiros (`passenger_count`) por hora** no mês de maio?

---

## 🧰 Tecnologias Utilizadas

- [x] **Databricks Free Edition**
- [x] **PySpark**
- [x] **Delta Lake**
- [x] **Amazon S3** (via External Location)
- [x] **Unity Catalog**
- [x] **GitHub** (controle de versão)

---

## 📁 Estrutura do Projeto

```
ifood-case/
├── src/
│   ├── data_requests.py          # Download dos arquivos .parquet
│   ├── data_transformers.py      # Transformações (ingestão, partição, colunas)
│   ├── save_data.py              # Escrita no S3 / Escrita no Unity Catalog
│   └── data_analysis.py          # Análises com PySpark
│
├── etl_bronze.ipynb              # Baixa dados e salva na camada Bronze
├── etl_silver.ipynb              # Filtra e transforma para Silver
├── etl_gold.ipynb                # Agrega e salva resultados finais (Gold)
│
├── analysis/
│   └── perguntas.ipynb           # Consulta dos resultados analíticos
│
├── requirements.txt
└── README.md
```

---

## ▶️ Como Executar no Databricks (Free Edition)

### 1. Clone e use direto no Databricks

No Databricks, vá em **Repos > Add Repo > Git URL** e insira:

```bash
https://github.com/<seuGitHub>/ifood-case
```
### 2. Configure uma External Location no Unity Catalog

- Bucket: `s3://<seunome>-ifood-case/`
- Role IAM com permissões de leitura/escrita no bucket

##### Obs.: Não é necessário instalar dependências, pois você conseguirá executar diretamente no Databricks.

---

## 🚦 Ordem de Execução

1. `etl_bronze.ipynb` – Baixa arquivos `.parquet` e salva dados brutos no S3  
2. `etl_silver.ipynb` – Seleciona colunas requeridas e adiciona partição  
3. `etl_gold.ipynb` – Agrega métricas e salva tabelas finais  
4. `analysis/perguntas.ipynb` – Executa as consultas finais

---

## 📊 Acesso aos Dados

### Tabelas criadas:
- `ifood_case.bronze.yellow_trip`
- `ifood_case.silver.yellow_trip`
- `ifood_case.gold.media_valor_mensal`
- `ifood_case.gold.media_passageiros_hora`

### Exemplo de consulta:

```sql
-- Média mensal de total_amount
SELECT * FROM ifood_case.gold.media_valor_mensal;

-- Média de passageiros por hora em maio
SELECT * FROM ifood_case.gold.media_passageiros_hora;
```

---

## 📦 Requisitos

- Databricks Free Edition (https://www.databricks.com/learn/free-edition)
- Conta AWS com bucket S3 configurado e external location registrada
- Permissões de leitura/gravação no Unity Catalog
- Python 3.8+ com PySpark

---

## ✅ Status do Projeto

- [x] Ingestão automatizada com controle por módulo
- [x] Camadas Bronze, Silver e Gold implementadas
- [x] Tabelas em Delta criadas no Unity Catalog
- [x] Respostas analíticas obtidas via PySpark e SQL
- [x] Versionamento completo no GitHub

---

## 📬 Contato

Desenvolvido por [@WendySilva](https://github.com/WendySilva)  
Em caso de dúvidas, sugestões ou contribuições, abra uma issue ou entre em contato.

[@LinkedIn](https://www.linkedin.com/in/wendysmendonca/)
