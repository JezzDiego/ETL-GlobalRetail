-- Script de criação do Data Warehouse (DW)
-- Estrutura OLAP com tabelas de dimensão e fato

-- =============================================
-- TABELAS DE DIMENSÃO
-- =============================================

-- Dimensão Tempo
CREATE TABLE dim_tempo (
    sk_tempo SERIAL PRIMARY KEY,
    data_completa DATE NOT NULL,
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    dia INTEGER NOT NULL,
    trimestre INTEGER NOT NULL,
    semestre INTEGER NOT NULL,
    dia_semana INTEGER NOT NULL,
    nome_dia_semana VARCHAR(20) NOT NULL,
    nome_mes VARCHAR(20) NOT NULL,
    eh_fim_semana BOOLEAN NOT NULL
);

-- Dimensão Localidade
CREATE TABLE dim_localidade (
    sk_localidade SERIAL PRIMARY KEY,
    id_localidade INTEGER,
    cidade VARCHAR(100),
    estado VARCHAR(50),
    regiao VARCHAR(50),
    regiao_padronizada VARCHAR(50),
    eh_capital BOOLEAN DEFAULT FALSE
);

-- Dimensão Categoria Cliente
CREATE TABLE dim_categoria_cliente (
    sk_categoria_cliente SERIAL PRIMARY KEY,
    id_categoria_cliente INTEGER,
    nome_categoria_cliente VARCHAR(100),
    categoria_padronizada VARCHAR(100)
);

-- Dimensão Cliente
CREATE TABLE dim_cliente (
    sk_cliente SERIAL PRIMARY KEY,
    id_cliente INTEGER,
    nome_cliente VARCHAR(200),
    nome_padronizado VARCHAR(200),
    sk_categoria_cliente INTEGER,
    sk_localidade INTEGER,
    data_cadastro DATE,
    status_cliente VARCHAR(20) DEFAULT 'ATIVO'
);

-- Dimensão Categoria Produto
CREATE TABLE dim_categoria_produto (
    sk_categoria_produto SERIAL PRIMARY KEY,
    id_categoria_produto INTEGER,
    nome_categoria_produto VARCHAR(100),
    categoria_padronizada VARCHAR(100)
);

-- Dimensão Fornecedor
CREATE TABLE dim_fornecedor (
    sk_fornecedor SERIAL PRIMARY KEY,
    id_fornecedor INTEGER,
    nome_fornecedor VARCHAR(200),
    nome_padronizado VARCHAR(200),
    sk_localidade INTEGER,
    status_fornecedor VARCHAR(20) DEFAULT 'ATIVO'
);

-- Dimensão Produto
CREATE TABLE dim_produto (
    sk_produto SERIAL PRIMARY KEY,
    id_produto INTEGER,
    nome_produto VARCHAR(200),
    nome_padronizado VARCHAR(200),
    sk_categoria_produto INTEGER,
    preco_unitario DECIMAL(10,2),
    custo_unitario DECIMAL(10,2),
    margem_lucro DECIMAL(5,2),
    status_produto VARCHAR(20) DEFAULT 'ATIVO'
);

-- Dimensão Vendedor
CREATE TABLE dim_vendedor (
    sk_vendedor SERIAL PRIMARY KEY,
    id_vendedor INTEGER,
    nome_vendedor VARCHAR(200),
    nome_padronizado VARCHAR(200),
    sk_localidade INTEGER,
    status_vendedor VARCHAR(20) DEFAULT 'ATIVO'
);

-- Dimensão Loja
CREATE TABLE dim_loja (
    sk_loja SERIAL PRIMARY KEY,
    id_loja INTEGER,
    nome_loja VARCHAR(200),
    nome_padronizado VARCHAR(200),
    sk_localidade INTEGER,
    tipo_loja VARCHAR(50),
    status_loja VARCHAR(20) DEFAULT 'ATIVA'
);

-- Dimensão Promoção
CREATE TABLE dim_promocao (
    sk_promocao SERIAL PRIMARY KEY,
    id_promocao INTEGER,
    nome_promocao VARCHAR(200),
    tipo_promocao VARCHAR(50),
    percentual_desconto DECIMAL(5,2),
    data_inicio DATE,
    data_fim DATE,
    status_promocao VARCHAR(20) DEFAULT 'ATIVA'
);

-- =============================================
-- TABELA DE FATO
-- =============================================

-- Fato Vendas
CREATE TABLE fato_vendas (
    sk_venda SERIAL PRIMARY KEY,
    id_venda INTEGER,
    sk_tempo INTEGER,
    sk_cliente INTEGER,
    sk_vendedor INTEGER,
    sk_loja INTEGER,
    sk_produto INTEGER,
    sk_promocao INTEGER,
    -- Métricas de vendas
    quantidade_vendida INTEGER NOT NULL,
    preco_unitario_venda DECIMAL(10,2) NOT NULL,
    valor_total_item DECIMAL(12,2) NOT NULL,
    custo_unitario DECIMAL(10,2),
    custo_total_item DECIMAL(12,2),
    lucro_bruto DECIMAL(12,2),
    percentual_desconto DECIMAL(5,2) DEFAULT 0,
    valor_desconto DECIMAL(10,2) DEFAULT 0,
    valor_final DECIMAL(12,2) NOT NULL,
    -- Campos de controle
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    origem_dados VARCHAR(50) DEFAULT 'SISTEMA_CRM'
);
