-- Script para criação de índices no Data Warehouse
-- Executar após a carga dos dados

-- =============================================
-- ÍNDICES PARA PERFORMANCE
-- =============================================

-- Índices nas tabelas de dimensão
CREATE INDEX IF NOT EXISTS idx_dim_cliente_categoria ON dim_cliente(sk_categoria_cliente);
CREATE INDEX IF NOT EXISTS idx_dim_cliente_localidade ON dim_cliente(sk_localidade);
CREATE INDEX IF NOT EXISTS idx_dim_produto_categoria ON dim_produto(sk_categoria_produto);
CREATE INDEX IF NOT EXISTS idx_dim_vendedor_localidade ON dim_vendedor(sk_localidade);
CREATE INDEX IF NOT EXISTS idx_dim_loja_localidade ON dim_loja(sk_localidade);
CREATE INDEX IF NOT EXISTS idx_dim_fornecedor_localidade ON dim_fornecedor(sk_localidade);

-- Índices na tabela de fato
CREATE INDEX IF NOT EXISTS idx_fato_vendas_tempo ON fato_vendas(sk_tempo);
CREATE INDEX IF NOT EXISTS idx_fato_vendas_cliente ON fato_vendas(sk_cliente);
CREATE INDEX IF NOT EXISTS idx_fato_vendas_produto ON fato_vendas(sk_produto);
CREATE INDEX IF NOT EXISTS idx_fato_vendas_vendedor ON fato_vendas(sk_vendedor);
CREATE INDEX IF NOT EXISTS idx_fato_vendas_loja ON fato_vendas(sk_loja);
CREATE INDEX IF NOT EXISTS idx_fato_vendas_promocao ON fato_vendas(sk_promocao);

-- Índices compostos para consultas temporais
CREATE INDEX IF NOT EXISTS idx_fato_vendas_tempo_cliente ON fato_vendas(sk_tempo, sk_cliente);
CREATE INDEX IF NOT EXISTS idx_fato_vendas_tempo_produto ON fato_vendas(sk_tempo, sk_produto);

-- Índices nas chaves naturais das dimensões
CREATE INDEX IF NOT EXISTS idx_dim_localidade_id ON dim_localidade(id_localidade);
CREATE INDEX IF NOT EXISTS idx_dim_cliente_id ON dim_cliente(id_cliente);
CREATE INDEX IF NOT EXISTS idx_dim_produto_id ON dim_produto(id_produto);
CREATE INDEX IF NOT EXISTS idx_dim_vendedor_id ON dim_vendedor(id_vendedor);
CREATE INDEX IF NOT EXISTS idx_dim_loja_id ON dim_loja(id_loja);
CREATE INDEX IF NOT EXISTS idx_dim_promocao_id ON dim_promocao(id_promocao);
CREATE INDEX IF NOT EXISTS idx_dim_categoria_cliente_id ON dim_categoria_cliente(id_categoria_cliente);
CREATE INDEX IF NOT EXISTS idx_dim_categoria_produto_id ON dim_categoria_produto(id_categoria_produto);
CREATE INDEX IF NOT EXISTS idx_dim_fornecedor_id ON dim_fornecedor(id_fornecedor);
