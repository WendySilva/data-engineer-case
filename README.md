# 🚕 Case – Pipeline de Dados com PySpark e Databricks

Este projeto foi desenvolvido como parte de um **case técnico para a posição de Data Engineer**, utilizando o **Databricks Free Edition** e versionado com **GitHub**.

A solução foca na ingestão, transformação e análise de dados de táxis de Nova York com uma arquitetura em camadas (Bronze, Silver, Gold), utilizando **PySpark**, **Delta Lake** e armazenamento em **S3** via **Unity Catalog**.

---

## 🎯 Objetivo

-Ingerir dados das corridas de Yellow Taxis de NY (jan a mai/2023).
-Criar uma estrutura de Data Lake com camadas Bronze → Silver → Gold.
-Criar tabelas no Unity Catalog.
-Disponibilizar os dados via SQL.

---

## 🧠 Dados Utilizados

Fonte oficial: [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

Foram utilizados os dados de **Janeiro a Julho de 2023**, considerando a observação na documentação de que os dados podem levar até **dois meses** para serem disponibilizados. Os meses 06 e 07 foram incluídos na ingestão, mas **apenas os meses de jan-mai foram utilizados nas análises finais**, conforme exigido no desafio. Um filtro de meses foi aplicado diretamente no código

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

## 📂 Arquitetura
Camadas utilizadas:

- **Bronze**: Armazena os arquivos .parquet sem modificações.
- **Silver**: Aplica seleção de colunas, padroniza nomes e cria a particao `year_month`.
- **Gold**: Agrega métricas e disponibiliza os dados finais para análise.

![image](https://github.com/user-attachments/assets/aaab2299-d372-42b2-9d33-652e35f95d26)


> ✉️ Os dados não foram modificados em conteúdo, apenas **padronizados os nomes das colunas** (para camelCase) e feito o cast de tipos adequados.


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

- Clone esse repositório
- No Databricks, vá em **Repos > Add Repo > Git URL** e insira:

```bash
https://github.com/<seuGitHub>/data-engineer-case
```
<img width="805" alt="image" src="https://github.com/user-attachments/assets/22113e98-7fb3-4044-b732-c89a7c2b4abc" />

### 2. Configure uma External Location no Unity Catalog

- Ex. de nome para o Bucket: `s3://<seunome>-ifood-case/`
- Role IAM com permissões de leitura/escrita no bucket

![image](https://github.com/user-attachments/assets/a670d666-92cc-4ee6-a7c6-77132c6bf880)

##### Obs.: para instalar as dependencias é só usar `pip install -r requirements.txt`

---

## 🚦 Ordem de Execução

1. `etl_bronze.ipynb` – Baixa arquivos `.parquet` e salva dados brutos no S3  
2. `etl_silver.ipynb` – Seleciona colunas requeridas e adiciona partição  
3. `etl_gold.ipynb` – Agrega métricas e salva tabelas finais  
4. `analysis/perguntas.ipynb` – Executa as consultas finais

Você consegue acompanhar os logs e erros da execução:
![image](https://github.com/user-attachments/assets/9a4ca78f-bb46-41df-9569-f0f37daeddfc)


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

## 📊 Resultados Analíticos

### Média mensal de total_amount (2023-01 a 2023-05)
| year_month | media_total_amount |
|------------|--------------------|
| 2023-01    | 27.02              |
| 2023-02    | 26.9               |
| 2023-03    | 27.9               |
| 2023-04    | 28.27              |
| 2023-05    | 28.96              |

### Média de passageiros por hora (maio/2023)
744 linhas correspondentes às combinações de `data x hora` com médias arredondadas entre 1.2 a 1.5 passageiros.

---

## 📦 Requisitos

- Databricks Free Edition (https://www.databricks.com/learn/free-edition)
- Conta AWS com bucket S3 configurado e external location registrada
- Permissões de leitura/gravação no Unity Catalog
- Python 3.8+ com PySpark

---

## 🖼️ Imagens

- Camada Bronze
  - Notebook
    ![image](https://github.com/user-attachments/assets/16099c71-0e92-40f4-a564-40b285ecafed)
    
  - Bucket
    ![image](https://github.com/user-attachments/assets/dc03d9e9-2818-429e-8e09-efe264b48f70)
    
  - Tabela
    ![image](https://github.com/user-attachments/assets/274af521-22d2-4cd1-9226-afc49f67b24a)
 
- Camada Silver
  - Notebook
    ![image](https://github.com/user-attachments/assets/7d8390e2-dac7-4f63-a559-5aad5a8e855d)
    
  - Bucket
    ![image](https://github.com/user-attachments/assets/d897a148-df38-4ca9-aae1-4c089045e723)

  - Tabela
    ![image](https://github.com/user-attachments/assets/fb3fa78e-b382-4f2f-bd41-e04844f69191)

- Camada Gold
  - Notebook
    ![image](https://github.com/user-attachments/assets/b22f1297-e71e-4f29-b787-c3af4cfe47b7)

  - Bucket
    ![image](https://github.com/user-attachments/assets/39ff1bfa-caf7-4634-9030-7666ca48c1a3)

  - Tabela
    ![image](https://github.com/user-attachments/assets/64a2e3ac-5e93-4f50-a716-e0ca3b479e35)
    
  - Perguntas
    ![image](https://github.com/user-attachments/assets/d3f07d4b-17ad-43c5-809b-c86923a0c882)



---

## ✅ Status do Projeto

- [x] Ingestão automatizada com controle por módulo
- [x] Camadas Bronze, Silver e Gold implementadas
- [x] Tabelas em Delta criadas no Unity Catalog
- [x] Respostas analíticas obtidas via PySpark e SQL
- [x] Versionamento completo no GitHub

---

## ✅ Atendimento aos Requisitos do Desafio

Este projeto cumpre integralmente os requisitos propostos no **Case Técnico de Data Engineer do iFood**, incluindo:

- ✔️ Ingestão dos dados de corridas de táxi de NY (janeiro e maio de 2023) diretamente da fonte oficial em formato Parquet;
- ✔️ Organização em camadas no Data Lake (**Bronze**, **Silver** e **Gold**), utilizando **Delta Lake** com **particionamento** e **Unity Catalog**;
- ✔️ Disponibilização dos dados para consumo via **SQL** diretamente no Databricks;
- ✔️ Implementação em **PySpark** com separação clara entre extração, transformação, análise e salvamento;
- ✔️ Garantia das colunas obrigatórias: `VendorID`, `passenger_count`, `total_amount`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`;
- ✔️ Respostas às perguntas analíticas solicitadas, disponíveis no notebook [`/analysis/perguntas.ipynb`](https://github.com/WendySilva/ifood-case/blob/main/analysis/perguntas.ipynb):
  - Média mensal de `total_amount` por frota.
  - Média de `passenger_count` por hora, no mês de **maio/2023**.

---

## 📬 Contato

Desenvolvido por [@WendySilva](https://github.com/WendySilva)  
Em caso de dúvidas, sugestões ou contribuições, abra uma issue ou entre em contato.

[@LinkedIn](https://www.linkedin.com/in/wendysmendonca/)
