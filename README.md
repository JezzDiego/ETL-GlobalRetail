# ETL Database Project - Bancos

Este projeto implementa um processo ETL (Extract, Transform, Load) completo para transferência e transformação de dados entre bancos PostgreSQL, incluindo a criação de um Data Warehouse.

## 📋 Visão Geral

O sistema processa dados de um banco transacional (CRM) e os transfere para um Data Warehouse (DW) com transformações e otimizações adequadas para análise.

## 🏗️ Estrutura do Projeto

```text
├── etl_completo.py           # Script principal do ETL
├── requirements.txt          # Dependências Python
├── .docker/                 # Configurações Docker
│   └── docker-compose.postgresql.yml
└── sql/                     # Scripts SQL
    ├── create_tables.sql
    ├── cria_dw.sql
    ├── cria_indices_dw.sql
    ├── dados_completos_padronizado.sql
    └── setup_databases.sql
```

## 🚀 Como Executar

### Pré-requisitos

- Python 3.8+
- Docker e Docker Compose
- Git

### 1. Configurar o Ambiente Virtual

```bash
python3 -m venv venv
source venv/bin/activate  # No Windows: venv\Scripts\activate
```

### 2. Instalar Dependências

```bash
pip install -r requirements.txt
```

### 3. Subir os Bancos de Dados

```bash
docker compose -f .docker/docker-compose.postgresql.yml up -d
```

### 4. Executar o ETL

```bash
python3 ./etl_completo.py
```

### Bancos de Dados

- **CRM (Origem)**: `global_retail_transacional`
- **DW (Destino)**: Data Warehouse otimizado para análises

### Conexões Padrão

- Host: localhost
- Porta: 5432
- Usuário: postgres
- Senha: postgres
