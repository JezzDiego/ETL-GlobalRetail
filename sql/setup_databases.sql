-- Script para criar as bases de dados separadas
-- Base CRM (OLTP) - Sistema de origem
DROP DATABASE IF EXISTS global_retail_transacional;
CREATE DATABASE global_retail_transacional;

-- Base DW (OLAP) - Data Warehouse
DROP DATABASE IF EXISTS global_retail_dw;
CREATE DATABASE global_retail_dw;
