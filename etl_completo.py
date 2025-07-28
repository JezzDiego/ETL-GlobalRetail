import psycopg2
from pathlib import Path
import re
from datetime import datetime, timedelta
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ETLProcessor:
    def __init__(self):
        self.conn_crm = None
        self.conn_dw = None
        
    def connect_to_crm(self):
        """Conecta ao banco CRM (origem)"""
        try:
            self.conn_crm = psycopg2.connect(
                host="localhost",
                port=5432,
                database="global_retail_transacional",
                user="postgres",
                password="postgres"
            )
            logger.info("Conexão com CRM estabelecida com sucesso")
            return True
        except Exception as e:
            logger.error(f"Erro ao conectar com CRM: {e}")
            return False
    
    def connect_to_dw(self):
        """Conecta ao Data Warehouse"""
        try:
            self.conn_dw = psycopg2.connect(
                host='localhost',
                database='global_retail_dw',
                user='postgres',
                password='postgres'
            )
            self.conn_dw.autocommit = False
            logger.info("Conexão com DW estabelecida com sucesso")
            return True
        except Exception as e:
            logger.error(f"Erro ao conectar com DW: {e}")
            return False
    
    def reset_dw_connection(self):
        """Reseta a conexão do DW em caso de erro"""
        try:
            if self.conn_dw:
                self.conn_dw.close()
            return self.connect_to_dw()
        except Exception as e:
            logger.error(f"Erro ao resetar conexão DW: {e}")
            return False
    
    def setup_databases(self):
        """Configura as bases de dados"""
        try:
            # Conecta ao PostgreSQL para criar as bases
            conn_admin = psycopg2.connect(
                host="localhost",
                port=5432,
                database="postgres",
                user="postgres",
                password="postgres"
            )
            conn_admin.autocommit = True
            cursor = conn_admin.cursor()
            
            # Criar base CRM
            cursor.execute("DROP DATABASE IF EXISTS global_retail_transacional")
            cursor.execute("CREATE DATABASE global_retail_transacional")
            logger.info("Base global_retail_transacional criada")
            
            # Criar base DW
            cursor.execute("DROP DATABASE IF EXISTS global_retail_dw")
            cursor.execute("CREATE DATABASE global_retail_dw")
            logger.info("Base global_retail_dw criada")
            
            cursor.close()
            conn_admin.close()
            return True
            
        except Exception as e:
            logger.error(f"Erro ao configurar bases de dados: {e}")
            return False
    
    def execute_sql_file(self, connection, file_path, description=""):
        """Executa um arquivo SQL"""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                sql_script = file.read()
            
            # Limpar SQL para PostgreSQL
            sql_script = self.clean_sql_for_postgresql(sql_script)
            
            cursor = connection.cursor()
            
            # Executar o script completo
            try:
                cursor.execute(sql_script)
                connection.commit()
                logger.info(f"{description} executado com sucesso")
                cursor.close()
                return True
            except Exception as e:
                logger.error(f"Erro ao executar script completo: {e}")
                connection.rollback()
                
                # Tentar executar comando por comando como fallback
                commands = sql_script.split(';')
                executed_count = 0
                
                for command in commands:
                    command = command.strip()
                    if command and not command.startswith('--') and len(command) > 5:
                        try:
                            cursor.execute(command)
                            connection.commit()
                            executed_count += 1
                            if executed_count % 10 == 0:
                                logger.info(f"{description}: {executed_count} comandos executados...")
                        except Exception as cmd_error:
                            logger.error(f"Erro ao executar comando: {cmd_error}")
                            logger.error(f"Comando: {command[:100]}...")
                            connection.rollback()
                
                logger.info(f"{description} executado. Total: {executed_count} comandos")
                cursor.close()
                return executed_count > 0
            
        except Exception as e:
            logger.error(f"Erro ao executar {file_path}: {e}")
            return False
    
    def clean_sql_for_postgresql(self, sql_content):
        """Remove/substitui comandos específicos do MySQL para PostgreSQL"""
        sql_content = sql_content.replace('SET FOREIGN_KEY_CHECKS=0;', '')
        sql_content = sql_content.replace('SET FOREIGN_KEY_CHECKS=1;', '')
        sql_content = sql_content.replace(' VALUE ', ' VALUES ')
        return sql_content
    
    def extract_and_transform_localidade(self):
        """ETL para dimensão Localidade"""
        logger.info("Iniciando ETL da dimensão Localidade...")
        
        try:
            cursor_crm = self.conn_crm.cursor()
            cursor_dw = self.conn_dw.cursor()
            
            # Extração
            cursor_crm.execute("""
                SELECT DISTINCT id_localidade, cidade, estado, regiao 
                FROM localidade 
                ORDER BY id_localidade
            """)
            
            localidades = cursor_crm.fetchall()
            
            for row in localidades:
                id_loc, cidade, estado, regiao = row
                
                # Transformações
                cidade_clean = self.clean_text(cidade) if cidade else 'N/A'
                estado_clean = self.clean_text(estado) if estado else 'N/A'
                regiao_clean = self.standardize_region(regiao) if regiao else 'N/A'
                eh_capital = self.is_capital(cidade_clean, estado_clean)
                
                # Carga
                cursor_dw.execute("""
                    INSERT INTO dim_localidade 
                    (id_localidade, cidade, estado, regiao, regiao_padronizada, eh_capital)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (id_loc, cidade_clean, estado_clean, regiao, regiao_clean, eh_capital))
            
            self.conn_dw.commit()
            logger.info(f"Dimensão Localidade carregada: {len(localidades)} registros")
            
        except Exception as e:
            logger.error(f"Erro no ETL de Localidade: {e}")
            self.conn_dw.rollback()
    
    def extract_and_transform_categoria_cliente(self):
        """ETL para dimensão Categoria Cliente"""
        logger.info("Iniciando ETL da dimensão Categoria Cliente...")
        
        try:
            cursor_crm = self.conn_crm.cursor()
            cursor_dw = self.conn_dw.cursor()
            
            cursor_crm.execute("""
                SELECT id_categoria_cliente, nome_categoria_cliente 
                FROM categoria_cliente 
                ORDER BY id_categoria_cliente
            """)
            
            categorias = cursor_crm.fetchall()
            
            for row in categorias:
                id_cat, nome_cat = row
                
                # Transformações
                nome_clean = self.clean_text(nome_cat) if nome_cat else 'Não Definido'
                nome_padronizado = self.standardize_customer_category(nome_clean)
                
                cursor_dw.execute("""
                    INSERT INTO dim_categoria_cliente 
                    (id_categoria_cliente, nome_categoria_cliente, categoria_padronizada)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (id_cat, nome_clean, nome_padronizado))
            
            self.conn_dw.commit()
            logger.info(f"Dimensão Categoria Cliente carregada: {len(categorias)} registros")
            
        except Exception as e:
            logger.error(f"Erro no ETL de Categoria Cliente: {e}")
            self.conn_dw.rollback()
    
    def extract_and_transform_categoria_produto(self):
        """ETL para dimensão Categoria Produto"""
        logger.info("Iniciando ETL da dimensão Categoria Produto...")
        
        try:
            cursor_crm = self.conn_crm.cursor()
            cursor_dw = self.conn_dw.cursor()
            
            cursor_crm.execute("""
                SELECT id_categoria_produto, nome_categoria_produto 
                FROM categoria_produto 
                ORDER BY id_categoria_produto
            """)
            
            categorias = cursor_crm.fetchall()
            
            for row in categorias:
                id_cat, nome_cat = row
                
                # Transformações
                nome_clean = self.clean_text(nome_cat) if nome_cat else 'Não Definido'
                nome_padronizado = self.standardize_product_category(nome_clean)
                
                cursor_dw.execute("""
                    INSERT INTO dim_categoria_produto 
                    (id_categoria_produto, nome_categoria_produto, categoria_padronizada)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (id_cat, nome_clean, nome_padronizado))
            
            self.conn_dw.commit()
            logger.info(f"Dimensão Categoria Produto carregada: {len(categorias)} registros")
            
        except Exception as e:
            logger.error(f"Erro no ETL de Categoria Produto: {e}")
            self.conn_dw.rollback()
    
    def extract_and_transform_fornecedor(self):
        """ETL para dimensão Fornecedor"""
        logger.info("Iniciando ETL da dimensão Fornecedor...")
        
        try:
            cursor_crm = self.conn_crm.cursor()
            cursor_dw = self.conn_dw.cursor()
            
            cursor_crm.execute("""
                SELECT f.id_fornecedor, f.nome_fornecedor, f.pais_origem
                FROM fornecedores f
                ORDER BY f.id_fornecedor
            """)
            
            fornecedores = cursor_crm.fetchall()
            
            for row in fornecedores:
                id_forn, nome_forn, pais_origem = row
                
                # Transformações
                nome_clean = self.clean_text(nome_forn) if nome_forn else 'Fornecedor N/A'
                nome_padronizado = self.standardize_name(nome_clean)
                
                # Para fornecedores, vamos usar sk_localidade = NULL já que não temos localidade
                cursor_dw.execute("""
                    INSERT INTO dim_fornecedor 
                    (id_fornecedor, nome_fornecedor, nome_padronizado, sk_localidade, status_fornecedor)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (id_forn, nome_clean, nome_padronizado, None, 'ATIVO'))
            
            self.conn_dw.commit()
            logger.info(f"Dimensão Fornecedor carregada: {len(fornecedores)} registros")
            
        except Exception as e:
            logger.error(f"Erro no ETL de Fornecedor: {e}")
            self.conn_dw.rollback()
    
    def extract_and_transform_cliente(self):
        """ETL para dimensão Cliente"""
        logger.info("Iniciando ETL da dimensão Cliente...")
        
        try:
            cursor_crm = self.conn_crm.cursor()
            cursor_dw = self.conn_dw.cursor()
            
            cursor_crm.execute("""
                SELECT c.id_cliente, c.nome_cliente, c.id_categoria_cliente, c.id_localidade
                FROM cliente c
                ORDER BY c.id_cliente
            """)
            
            clientes = cursor_crm.fetchall()
            
            for row in clientes:
                id_cli, nome_cli, id_cat_cli, id_loc = row
                
                # Buscar chaves surrogadas no DW
                sk_cat_cli = None
                sk_loc = None
                
                if id_cat_cli:
                    cursor_dw.execute("SELECT sk_categoria_cliente FROM dim_categoria_cliente WHERE id_categoria_cliente = %s", (id_cat_cli,))
                    cat_result = cursor_dw.fetchone()
                    if cat_result:
                        sk_cat_cli = cat_result[0]
                
                if id_loc:
                    cursor_dw.execute("SELECT sk_localidade FROM dim_localidade WHERE id_localidade = %s", (id_loc,))
                    loc_result = cursor_dw.fetchone()
                    if loc_result:
                        sk_loc = loc_result[0]
                
                # Transformações
                nome_clean = self.clean_text(nome_cli) if nome_cli else 'Cliente N/A'
                nome_padronizado = self.standardize_name(nome_clean)
                
                cursor_dw.execute("""
                    INSERT INTO dim_cliente 
                    (id_cliente, nome_cliente, nome_padronizado, sk_categoria_cliente, 
                     sk_localidade, data_cadastro, status_cliente)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (id_cli, nome_clean, nome_padronizado, sk_cat_cli, sk_loc, 
                     datetime.now().date(), 'ATIVO'))
            
            self.conn_dw.commit()
            logger.info(f"Dimensão Cliente carregada: {len(clientes)} registros")
            
        except Exception as e:
            logger.error(f"Erro no ETL de Cliente: {e}")
            self.conn_dw.rollback()
    
    def extract_and_transform_produto(self):
        """ETL para dimensão Produto"""
        logger.info("Iniciando ETL da dimensão Produto...")
        
        try:
            cursor_crm = self.conn_crm.cursor()
            cursor_dw = self.conn_dw.cursor()
            
            cursor_crm.execute("""
                SELECT p.id_produto, p.nome_produto, p.id_categoria_produto
                FROM produto p
                ORDER BY p.id_produto
            """)
            
            produtos = cursor_crm.fetchall()
            
            for row in produtos:
                id_prod, nome_prod, id_cat_prod = row
                
                # Buscar sk_categoria_produto no DW
                sk_cat_prod = None
                if id_cat_prod:
                    cursor_dw.execute("SELECT sk_categoria_produto FROM dim_categoria_produto WHERE id_categoria_produto = %s", (id_cat_prod,))
                    cat_result = cursor_dw.fetchone()
                    if cat_result:
                        sk_cat_prod = cat_result[0]
                
                # Buscar preço médio do produto nas vendas
                cursor_crm.execute("""
                    SELECT AVG(preco_venda) FROM item_vendas WHERE id_produto = %s
                """, (id_prod,))
                preco_result = cursor_crm.fetchone()
                preco_medio = float(preco_result[0]) if preco_result[0] else 0.0
                
                # Transformações
                nome_clean = self.clean_text(nome_prod) if nome_prod else 'Produto N/A'
                nome_padronizado = self.standardize_name(nome_clean)
                custo_estimado = preco_medio * 0.7 if preco_medio > 0 else 0.0  # Estimar custo como 70% do preço
                margem = ((preco_medio - custo_estimado) / preco_medio * 100) if preco_medio > 0 else 0.0
                
                cursor_dw.execute("""
                    INSERT INTO dim_produto 
                    (id_produto, nome_produto, nome_padronizado, sk_categoria_produto,
                     preco_unitario, custo_unitario, margem_lucro, status_produto)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (id_prod, nome_clean, nome_padronizado, sk_cat_prod, 
                     preco_medio, custo_estimado, margem, 'ATIVO'))
            
            self.conn_dw.commit()
            logger.info(f"Dimensão Produto carregada: {len(produtos)} registros")
            
        except Exception as e:
            logger.error(f"Erro no ETL de Produto: {e}")
            self.conn_dw.rollback()
    
    def extract_and_transform_vendedor(self):
        """ETL para dimensão Vendedor"""
        logger.info("Iniciando ETL da dimensão Vendedor...")
        
        try:
            cursor_crm = self.conn_crm.cursor()
            cursor_dw = self.conn_dw.cursor()
            
            cursor_crm.execute("""
                SELECT v.id_vendedor, v.nome_vendedor, v.telefone, v.email
                FROM vendedor v
                ORDER BY v.id_vendedor
            """)
            
            vendedores = cursor_crm.fetchall()
            
            for row in vendedores:
                id_vend, nome_vend, telefone, email = row
                
                # Transformações
                nome_clean = self.clean_text(nome_vend) if nome_vend else 'Vendedor N/A'
                nome_padronizado = self.standardize_name(nome_clean)
                telefone_clean = self.clean_text(telefone) if telefone else None
                email_clean = self.clean_text(email) if email else None
                
                cursor_dw.execute("""
                    INSERT INTO dim_vendedor 
                    (id_vendedor, nome_vendedor, nome_padronizado, telefone, 
                     email, data_cadastro, status_vendedor)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (id_vend, nome_clean, nome_padronizado, telefone_clean, 
                     email_clean, datetime.now().date(), 'ATIVO'))
            
            self.conn_dw.commit()
            logger.info(f"Dimensão Vendedor carregada: {len(vendedores)} registros")
            
        except Exception as e:
            logger.error(f"Erro no ETL de Vendedor: {e}")
            self.conn_dw.rollback()
    
    def extract_and_transform_loja(self):
        """ETL para dimensão Loja"""
        logger.info("Iniciando ETL da dimensão Loja...")
        
        try:
            cursor_crm = self.conn_crm.cursor()
            cursor_dw = self.conn_dw.cursor()
            
            cursor_crm.execute("""
                SELECT l.id_loja, l.nome_loja, l.cidade, l.estado, l.telefone
                FROM loja l
                ORDER BY l.id_loja
            """)
            
            lojas = cursor_crm.fetchall()
            
            for row in lojas:
                id_loja, nome_loja, cidade, estado, telefone = row
                
                # Buscar localidade correspondente por cidade/estado
                sk_localidade = None
                if cidade and estado:
                    cursor_dw.execute("""
                        SELECT sk_localidade 
                        FROM dim_localidade 
                        WHERE LOWER(cidade) = LOWER(%s) AND LOWER(estado) = LOWER(%s)
                    """, (cidade.strip(), estado.strip()))
                    loc_result = cursor_dw.fetchone()
                    if loc_result:
                        sk_localidade = loc_result[0]
                
                # Transformações
                nome_clean = self.clean_text(nome_loja) if nome_loja else 'Loja N/A'
                nome_padronizado = self.standardize_name(nome_clean)
                telefone_clean = self.clean_text(telefone) if telefone else None
                
                cursor_dw.execute("""
                    INSERT INTO dim_loja 
                    (id_loja, nome_loja, nome_padronizado, sk_localidade,
                     telefone, data_abertura, status_loja)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (id_loja, nome_clean, nome_padronizado, sk_localidade, 
                     telefone_clean, datetime.now().date(), 'ATIVA'))
            
            self.conn_dw.commit()
            logger.info(f"Dimensão Loja carregada: {len(lojas)} registros")
            
        except Exception as e:
            logger.error(f"Erro no ETL de Loja: {e}")
            self.conn_dw.rollback()
    
    def extract_and_transform_vendedor(self):
        """ETL para dimensão Vendedor"""
        logger.info("Iniciando ETL da dimensão Vendedor...")
        
        try:
            cursor_crm = self.conn_crm.cursor()
            cursor_dw = self.conn_dw.cursor()
            
            cursor_crm.execute("""
                SELECT v.id_vendedor, v.nome_vendedor
                FROM vendedor v
                ORDER BY v.id_vendedor
            """)
            
            vendedores = cursor_crm.fetchall()
            
            for row in vendedores:
                id_vend, nome_vend = row
                
                # Transformações
                nome_clean = self.clean_text(nome_vend) if nome_vend else 'Vendedor N/A'
                nome_padronizado = self.standardize_name(nome_clean)
                
                # Para vendedores, vamos usar sk_localidade = NULL já que não temos localidade
                cursor_dw.execute("""
                    INSERT INTO dim_vendedor 
                    (id_vendedor, nome_vendedor, nome_padronizado, sk_localidade, status_vendedor)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (id_vend, nome_clean, nome_padronizado, None, 'ATIVO'))
            
            self.conn_dw.commit()
            logger.info(f"Dimensão Vendedor carregada: {len(vendedores)} registros")
            
        except Exception as e:
            logger.error(f"Erro no ETL de Vendedor: {e}")
            self.conn_dw.rollback()
    
    def extract_and_transform_loja(self):
        """ETL para dimensão Loja"""
        logger.info("Iniciando ETL da dimensão Loja...")
        
        try:
            cursor_crm = self.conn_crm.cursor()
            cursor_dw = self.conn_dw.cursor()
            
            cursor_crm.execute("""
                SELECT l.id_loja, l.nome_loja, l.gerente_loja, l.cidade, l.estado
                FROM lojas l
                ORDER BY l.id_loja
            """)
            
            lojas = cursor_crm.fetchall()
            
            for row in lojas:
                id_loja, nome_loja, gerente_loja, cidade, estado = row
                
                # Buscar localidade baseada na cidade e estado da loja
                sk_loc = None
                if cidade and estado:
                    cursor_dw.execute("""
                        SELECT sk_localidade FROM dim_localidade 
                        WHERE LOWER(cidade) = LOWER(%s) AND LOWER(estado) = LOWER(%s)
                        LIMIT 1
                    """, (cidade.strip(), estado.strip()))
                    loc_result = cursor_dw.fetchone()
                    if loc_result:
                        sk_loc = loc_result[0]
                
                # Transformações
                nome_clean = self.clean_text(nome_loja) if nome_loja else 'Loja N/A'
                nome_padronizado = self.standardize_name(nome_clean)
                tipo_loja = self.classify_store_type(nome_clean)
                
                cursor_dw.execute("""
                    INSERT INTO dim_loja 
                    (id_loja, nome_loja, nome_padronizado, sk_localidade, tipo_loja, status_loja)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (id_loja, nome_clean, nome_padronizado, sk_loc, tipo_loja, 'ATIVA'))
            
            self.conn_dw.commit()
            logger.info(f"Dimensão Loja carregada: {len(lojas)} registros")
            
        except Exception as e:
            logger.error(f"Erro no ETL de Loja: {e}")
            self.conn_dw.rollback()
    
    def extract_and_transform_vendas(self):
        """ETL para fato Vendas"""
        logger.info("Iniciando ETL da tabela fato Vendas...")
        
        try:
            cursor_crm = self.conn_crm.cursor()
            cursor_dw = self.conn_dw.cursor()
            
            cursor_crm.execute("""
                SELECT v.id_venda, v.data_venda, v.id_cliente, iv.id_produto, 
                       v.id_vendedor, v.id_loja, iv.qtd_vendida, iv.preco_venda, 0 as desconto
                FROM vendas v
                INNER JOIN item_vendas iv ON v.id_venda = iv.id_venda
                ORDER BY v.data_venda, v.id_venda, iv.id_produto
            """)
            
            vendas = cursor_crm.fetchall()
            
            for row in vendas:
                id_venda, data_venda, id_cli, id_prod, id_vend, id_loja, qtd, preco, desconto = row
                
                # Buscar chaves surrogadas
                sk_tempo = sk_cliente = sk_produto = sk_vendedor = sk_loja = None
                
                # SK Tempo - converter string para date
                if data_venda and str(data_venda) not in ['Data Inválida', 'N/A', 'NULL', '']:
                    try:
                        # Tentar diferentes formatos de data
                        if len(str(data_venda)) == 10 and str(data_venda).count('-') == 2:
                            # Formato YYYY-MM-DD
                            from datetime import datetime
                            data_obj = datetime.strptime(str(data_venda), '%Y-%m-%d').date()
                            cursor_dw.execute("SELECT sk_tempo FROM dim_tempo WHERE data_completa = %s", (data_obj,))
                        elif len(str(data_venda)) == 10 and str(data_venda).count('/') == 2:
                            # Formato DD/MM/YYYY
                            from datetime import datetime
                            data_obj = datetime.strptime(str(data_venda), '%d/%m/%Y').date()
                            cursor_dw.execute("SELECT sk_tempo FROM dim_tempo WHERE data_completa = %s", (data_obj,))
                        else:
                            continue  # Pular datas inválidas
                        
                        tempo_result = cursor_dw.fetchone()
                        if tempo_result:
                            sk_tempo = tempo_result[0]
                    except:
                        continue  # Pular datas que não conseguimos converter
                
                # SK Cliente
                if id_cli:
                    cursor_dw.execute("SELECT sk_cliente FROM dim_cliente WHERE id_cliente = %s", (id_cli,))
                    cli_result = cursor_dw.fetchone()
                    if cli_result:
                        sk_cliente = cli_result[0]
                
                # SK Produto
                if id_prod:
                    cursor_dw.execute("SELECT sk_produto FROM dim_produto WHERE id_produto = %s", (id_prod,))
                    prod_result = cursor_dw.fetchone()
                    if prod_result:
                        sk_produto = prod_result[0]
                
                # SK Vendedor
                if id_vend:
                    cursor_dw.execute("SELECT sk_vendedor FROM dim_vendedor WHERE id_vendedor = %s", (id_vend,))
                    vend_result = cursor_dw.fetchone()
                    if vend_result:
                        sk_vendedor = vend_result[0]
                
                # SK Loja
                if id_loja:
                    cursor_dw.execute("SELECT sk_loja FROM dim_loja WHERE id_loja = %s", (id_loja,))
                    loja_result = cursor_dw.fetchone()
                    if loja_result:
                        sk_loja = loja_result[0]
                
                # Calcular métricas
                qtd_clean = float(qtd) if qtd and qtd > 0 else 0.0
                preco_clean = float(preco) if preco and preco > 0 else 0.0
                desconto_clean = float(desconto) if desconto and desconto >= 0 else 0.0
                
                valor_bruto = qtd_clean * preco_clean
                valor_desconto = valor_bruto * (desconto_clean / 100) if desconto_clean > 0 else 0.0
                valor_liquido = valor_bruto - valor_desconto
                
                # Buscar custo unitário do produto
                custo_unitario = 0.0
                if sk_produto:
                    cursor_dw.execute("SELECT custo_unitario FROM dim_produto WHERE sk_produto = %s", (sk_produto,))
                    custo_result = cursor_dw.fetchone()
                    if custo_result and custo_result[0]:
                        custo_unitario = float(custo_result[0])
                
                custo_total = qtd_clean * custo_unitario
                lucro_bruto = valor_liquido - custo_total
                
                # Gerar ID único para o fato (combinação de id_venda + id_produto)
                id_fato = f"{id_venda}_{id_prod}"
                
                cursor_dw.execute("""
                    INSERT INTO fato_vendas 
                    (id_venda, sk_tempo, sk_cliente, sk_produto, sk_vendedor, sk_loja,
                     quantidade_vendida, preco_unitario_venda, valor_total_item, 
                     percentual_desconto, valor_desconto, valor_final, 
                     custo_unitario, custo_total_item, lucro_bruto)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (id_fato, sk_tempo, sk_cliente, sk_produto, sk_vendedor, sk_loja,
                     qtd_clean, preco_clean, valor_bruto, desconto_clean, 
                     valor_desconto, valor_liquido, custo_unitario, custo_total, lucro_bruto))
            
            self.conn_dw.commit()
            logger.info(f"Fato Vendas carregado: {len(vendas)} registros")
            
        except Exception as e:
            logger.error(f"Erro no ETL de Vendas: {e}")
            self.conn_dw.rollback()
    
    def extract_and_transform_promocao(self):
        """ETL para dimensão Promoção"""
        logger.info("Iniciando ETL da dimensão Promoção...")
        
        try:
            cursor_crm = self.conn_crm.cursor()
            cursor_dw = self.conn_dw.cursor()
            
            cursor_crm.execute("""
                SELECT id_promocao, nome_promocao, tipo_desconto, data_inicio, data_fim
                FROM promocoes
                ORDER BY id_promocao
            """)
            
            promocoes = cursor_crm.fetchall()
            
            for row in promocoes:
                id_promo, nome_promo, tipo_desc, data_ini, data_fim = row
                
                # Transformações
                nome_clean = self.clean_text(nome_promo) if nome_promo else 'Promoção N/A'
                tipo_promo = self.classify_promotion_type(nome_clean)
                
                # Extrair percentual do tipo_desconto (assumindo formato "10%" ou similar)
                perc_clean = 0.0
                if tipo_desc:
                    try:
                        # Tentar extrair número do campo tipo_desconto
                        import re
                        match = re.search(r'(\d+(?:\.\d+)?)', str(tipo_desc))
                        if match:
                            perc_clean = float(match.group(1))
                    except:
                        perc_clean = 0.0
                
                # Validar datas
                data_ini_clean = None
                data_fim_clean = None
                
                if data_ini and str(data_ini) not in ['Data Inválida', 'N/A', 'NULL', '']:
                    data_ini_clean = data_ini
                    
                if data_fim and str(data_fim) not in ['Data Inválida', 'N/A', 'NULL', '']:
                    data_fim_clean = data_fim
                
                cursor_dw.execute("""
                    INSERT INTO dim_promocao 
                    (id_promocao, nome_promocao, tipo_promocao, percentual_desconto,
                     data_inicio, data_fim, status_promocao)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (id_promo, nome_clean, tipo_promo, perc_clean, 
                     data_ini_clean, data_fim_clean, 'ATIVA'))
            
            self.conn_dw.commit()
            logger.info(f"Dimensão Promoção carregada: {len(promocoes)} registros")
            
        except Exception as e:
            logger.error(f"Erro no ETL de Promoção: {e}")
            self.conn_dw.rollback()
    
    def generate_dim_tempo(self):
        """Gera a dimensão tempo"""
        logger.info("Gerando dimensão Tempo...")
        
        try:
            cursor_dw = self.conn_dw.cursor()
            
            # Gerar datas de 2020 a 2025
            start_date = datetime(2020, 1, 1)
            end_date = datetime(2025, 12, 31)
            
            current_date = start_date
            while current_date <= end_date:
                ano = current_date.year
                mes = current_date.month
                dia = current_date.day
                trimestre = (mes - 1) // 3 + 1
                semestre = 1 if mes <= 6 else 2
                dia_semana = current_date.weekday() + 1  # 1=Segunda, 7=Domingo
                nome_dia_semana = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo'][current_date.weekday()]
                nome_mes = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
                           'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'][mes-1]
                eh_fim_semana = dia_semana in [6, 7]  # Sábado e Domingo
                
                cursor_dw.execute("""
                    INSERT INTO dim_tempo 
                    (data_completa, ano, mes, dia, trimestre, semestre, dia_semana,
                     nome_dia_semana, nome_mes, eh_fim_semana)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (current_date.date(), ano, mes, dia, trimestre, semestre, dia_semana,
                     nome_dia_semana, nome_mes, eh_fim_semana))
                
                current_date += timedelta(days=1)
            
            self.conn_dw.commit()
            logger.info("Dimensão Tempo gerada com sucesso")
            
        except Exception as e:
            logger.error(f"Erro ao gerar dimensão Tempo: {e}")
            self.conn_dw.rollback()
    
    def extract_and_transform_fato_vendas(self):
        """ETL para tabela de fato Vendas"""
        logger.info("Iniciando ETL da tabela Fato Vendas...")
        
        try:
            cursor_crm = self.conn_crm.cursor()
            cursor_dw = self.conn_dw.cursor()
            
            # Query para extrair dados do CRM
            cursor_crm.execute("""
                SELECT 
                    v.id_venda, v.data_venda, v.id_cliente, v.id_vendedor, v.id_loja,
                    iv.id_produto, iv.qtd_vendida, iv.preco_venda, iv.id_promocao_aplicada
                FROM vendas v
                JOIN item_vendas iv ON v.id_venda = iv.id_venda
                ORDER BY v.id_venda, iv.id_produto
            """)
            
            vendas = cursor_crm.fetchall()
            count = 0
            
            for row in vendas:
                (id_venda, data_venda, id_cliente, id_vendedor, id_loja,
                 id_produto, qtd_vendida, preco_venda, id_promocao) = row
                
                # Buscar chaves surrogadas no DW
                sk_tempo = None
                sk_cliente = None
                sk_vendedor = None
                sk_loja = None
                sk_produto = None
                sk_promocao = None
                
                # SK Tempo
                if data_venda:
                    cursor_dw.execute("SELECT sk_tempo FROM dim_tempo WHERE data_completa = %s", (data_venda,))
                    tempo_result = cursor_dw.fetchone()
                    if tempo_result:
                        sk_tempo = tempo_result[0]
                
                # SK Cliente
                if id_cliente:
                    cursor_dw.execute("SELECT sk_cliente FROM dim_cliente WHERE id_cliente = %s", (id_cliente,))
                    cliente_result = cursor_dw.fetchone()
                    if cliente_result:
                        sk_cliente = cliente_result[0]
                
                # SK Vendedor
                if id_vendedor:
                    cursor_dw.execute("SELECT sk_vendedor FROM dim_vendedor WHERE id_vendedor = %s", (id_vendedor,))
                    vendedor_result = cursor_dw.fetchone()
                    if vendedor_result:
                        sk_vendedor = vendedor_result[0]
                
                # SK Loja
                if id_loja:
                    cursor_dw.execute("SELECT sk_loja FROM dim_loja WHERE id_loja = %s", (id_loja,))
                    loja_result = cursor_dw.fetchone()
                    if loja_result:
                        sk_loja = loja_result[0]
                
                # SK Produto
                if id_produto:
                    cursor_dw.execute("SELECT sk_produto FROM dim_produto WHERE id_produto = %s", (id_produto,))
                    produto_result = cursor_dw.fetchone()
                    if produto_result:
                        sk_produto = produto_result[0]
                
                # SK Promoção
                if id_promocao:
                    cursor_dw.execute("SELECT sk_promocao FROM dim_promocao WHERE id_promocao = %s", (id_promocao,))
                    promocao_result = cursor_dw.fetchone()
                    if promocao_result:
                        sk_promocao = promocao_result[0]
                
                # Transformações e cálculos
                qtd_clean = int(qtd_vendida) if qtd_vendida and qtd_vendida > 0 else 1
                preco_clean = float(preco_venda) if preco_venda and preco_venda > 0 else 0.0
                valor_total_item = qtd_clean * preco_clean
                
                # Buscar custo do produto
                custo_unitario = 0.0
                if sk_produto:
                    cursor_dw.execute("SELECT custo_unitario FROM dim_produto WHERE sk_produto = %s", (sk_produto,))
                    custo_result = cursor_dw.fetchone()
                    if custo_result and custo_result[0]:
                        custo_unitario = float(custo_result[0])
                
                custo_total_item = qtd_clean * custo_unitario
                lucro_bruto = valor_total_item - custo_total_item
                
                # Calcular desconto
                percentual_desconto = 0.0
                valor_desconto = 0.0
                if sk_promocao:
                    cursor_dw.execute("SELECT percentual_desconto FROM dim_promocao WHERE sk_promocao = %s", (sk_promocao,))
                    desc_result = cursor_dw.fetchone()
                    if desc_result and desc_result[0]:
                        percentual_desconto = float(desc_result[0])
                        valor_desconto = valor_total_item * (percentual_desconto / 100)
                
                valor_final = valor_total_item - valor_desconto
                
                # Inserir na tabela de fato
                cursor_dw.execute("""
                    INSERT INTO fato_vendas 
                    (id_venda, sk_tempo, sk_cliente, sk_vendedor, sk_loja, sk_produto, sk_promocao,
                     quantidade_vendida, preco_unitario_venda, valor_total_item, custo_unitario,
                     custo_total_item, lucro_bruto, percentual_desconto, valor_desconto, valor_final)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (id_venda, sk_tempo, sk_cliente, sk_vendedor, sk_loja, sk_produto, sk_promocao,
                     qtd_clean, preco_clean, valor_total_item, custo_unitario, custo_total_item,
                     lucro_bruto, percentual_desconto, valor_desconto, valor_final))
                
                count += 1
                if count % 1000 == 0:
                    logger.info(f"Fato Vendas: {count} registros processados...")
                    self.conn_dw.commit()
            
            self.conn_dw.commit()
            logger.info(f"Tabela Fato Vendas carregada: {count} registros")
            
        except Exception as e:
            logger.error(f"Erro no ETL de Fato Vendas: {e}")
            self.conn_dw.rollback()
    
    # =============================================
    # FUNÇÕES DE TRANSFORMAÇÃO E LIMPEZA
    # =============================================
    
    def clean_text(self, text):
        """Limpa e padroniza texto"""
        if not text:
            return 'N/A'
        
        # Remove espaços extras e caracteres especiais
        text = re.sub(r'\s+', ' ', str(text).strip())
        # Capitaliza primeira letra de cada palavra
        text = text.title()
        return text
    
    def standardize_name(self, name):
        """Padroniza nomes de pessoas/empresas"""
        if not name or name == 'N/A':
            return name
        
        # Lista de conectores que ficam em minúscula
        conectores = ['da', 'de', 'do', 'das', 'dos', 'e', 'em', 'na', 'no', 'com']
        
        words = name.split()
        result = []
        
        for i, word in enumerate(words):
            if i == 0 or word.lower() not in conectores:
                result.append(word.title())
            else:
                result.append(word.lower())
        
        return ' '.join(result)
    
    def standardize_region(self, region):
        """Padroniza nomes de regiões"""
        if not region:
            return 'Não Definido'
        
        region_map = {
            'rio de janeiro': 'Rio de Janeiro',
            'são paulo': 'São Paulo',
            'minas gerais': 'Minas Gerais',
            'mato grosso': 'Mato Grosso',
            'mato grosso do sul': 'Mato Grosso do Sul',
            'rio grande do sul': 'Rio Grande do Sul',
            'rio grande do norte': 'Rio Grande do Norte',
            'espírito santo': 'Espírito Santo',
            'distrito federal': 'Distrito Federal'
        }
        
        region_lower = region.lower().strip()
        return region_map.get(region_lower, region.title())
    
    def is_capital(self, city, state):
        """Verifica se a cidade é capital"""
        capitals = {
            'Rio Branco': ['AC'], 'Maceió': ['AL'], 'Macapá': ['AP'], 'Manaus': ['AM'],
            'Salvador': ['BA'], 'Fortaleza': ['CE'], 'Brasília': ['DF'], 'Vitória': ['ES'],
            'Goiânia': ['GO'], 'São Luís': ['MA'], 'Cuiabá': ['MT'], 'Campo Grande': ['MS'],
            'Belo Horizonte': ['MG'], 'Belém': ['PA'], 'João Pessoa': ['PB'], 'Curitiba': ['PR'],
            'Recife': ['PE'], 'Teresina': ['PI'], 'Rio de Janeiro': ['RJ'], 'Natal': ['RN'],
            'Porto Alegre': ['RS'], 'Porto Velho': ['RO'], 'Boa Vista': ['RR'], 'Florianópolis': ['SC'],
            'São Paulo': ['SP'], 'Aracaju': ['SE'], 'Palmas': ['TO']
        }
        
        return city in capitals and state in capitals.get(city, [])
    
    def standardize_customer_category(self, category):
        """Padroniza categoria de cliente"""
        if not category:
            return 'Não Definido'
        
        category_lower = category.lower()
        if 'vip' in category_lower or 'premium' in category_lower:
            return 'Premium'
        elif 'gold' in category_lower or 'ouro' in category_lower:
            return 'Gold'
        elif 'silver' in category_lower or 'prata' in category_lower:
            return 'Silver'
        else:
            return 'Padrão'
    
    def standardize_product_category(self, category):
        """Padroniza categoria de produto"""
        if not category:
            return 'Não Definido'
        
        return category.title()
    
    def classify_store_type(self, store_name):
        """Classifica tipo da loja baseado no nome"""
        if not store_name:
            return 'Loja Padrão'
        
        name_lower = store_name.lower()
        if 'shopping' in name_lower or 'mall' in name_lower:
            return 'Shopping'
        elif 'centro' in name_lower:
            return 'Centro'
        elif 'outlet' in name_lower:
            return 'Outlet'
        else:
            return 'Loja Padrão'
    
    def classify_promotion_type(self, promo_name):
        """Classifica tipo de promoção"""
        if not promo_name:
            return 'Desconto Geral'
        
        name_lower = promo_name.lower()
        if 'black' in name_lower:
            return 'Black Friday'
        elif 'natal' in name_lower:
            return 'Natal'
        elif 'liquidação' in name_lower:
            return 'Liquidação'
        else:
            return 'Desconto Geral'
    
    def check_dw_summary(self):
        """Exibe resumo completo do Data Warehouse"""
        logger.info("=== GERANDO RESUMO DO DATA WAREHOUSE ===")
        
        try:
            cursor = self.conn_dw.cursor()
            
            tables = [
                'dim_localidade', 'dim_categoria_cliente', 'dim_categoria_produto',
                'dim_fornecedor', 'dim_cliente', 'dim_produto', 'dim_vendedor',
                'dim_loja', 'dim_promocao', 'dim_tempo', 'fato_vendas'
            ]
            
            print('\n' + '='*50)
            print('📊 RESUMO DO DATA WAREHOUSE')
            print('='*50)
            print()
            
            total_records = 0
            for table in tables:
                cursor.execute(f'SELECT COUNT(*) FROM {table}')
                count = cursor.fetchone()[0]
                total_records += count
                
                # Adicionar emoji baseado no tipo de tabela
                if table.startswith('dim_'):
                    emoji = '📋'
                elif table.startswith('fato_'):
                    emoji = '📈'
                else:
                    emoji = '📊'
                
                print(f'{emoji} {table:22} : {count:>8,} registros')
            
            print()
            print('='*50)
            print(f'🎯 TOTAL DE REGISTROS    : {total_records:>8,}')
            print('='*50)
            print()
            
            # Informações adicionais sobre o Data Warehouse
            print('📋 DETALHES ADICIONAIS:')
            print(f'   • Período dos dados: 2020-2025')
            print(f'   • Arquitetura: Esquema Estrela (mesmo da atividade anterior)')
            print(f'   • Ambiente: PostgreSQL')
            print()
            
            cursor.close()
            logger.info("Resumo do Data Warehouse gerado com sucesso")
            
        except Exception as e:
            logger.error(f"Erro ao gerar resumo do DW: {e}")
            print(f"❌ Erro ao gerar resumo: {e}")
    
    def run_full_etl(self):
        """Executa o processo ETL completo"""
        logger.info("=== INICIANDO PROCESSO ETL COMPLETO ===")
        
        try:
            # 1. Configurar bases de dados
            if not self.setup_databases():
                return False
            
            # 2. Conectar às bases
            if not self.connect_to_crm() or not self.connect_to_dw():
                return False
            
            # 3. Criar estrutura CRM e popular
            logger.info("=== ETAPA 1: PREPARANDO AMBIENTE CRM ===")
            scripts_dir = Path("sql")
            
            if not self.execute_sql_file(self.conn_crm, scripts_dir / "create_tables.sql", "Criando tabelas CRM"):
                return False
            
            if not self.execute_sql_file(self.conn_crm, scripts_dir / "dados_completos_padronizado.sql", "Populando CRM"):
                return False
            
            # 4. Criar estrutura DW
            logger.info("=== ETAPA 2: CRIANDO ESTRUTURA DW ===")
            if not self.execute_sql_file(self.conn_dw, scripts_dir / "cria_dw.sql", "Criando estrutura DW"):
                return False
            
            # 5. ETL das Dimensões (ordem importante!)
            logger.info("=== ETAPA 3: CARREGANDO DIMENSÕES ===")
            
            # Dimensões básicas primeiro
            self.extract_and_transform_localidade()
            self.extract_and_transform_categoria_cliente()
            self.extract_and_transform_categoria_produto()
            
            # Resetar conexão após erro no produto anterior
            self.reset_dw_connection()
            
            # Dimensões que dependem das básicas
            self.extract_and_transform_fornecedor()
            self.extract_and_transform_cliente()
            self.extract_and_transform_produto()
            self.extract_and_transform_vendedor()
            self.extract_and_transform_loja()
            self.extract_and_transform_promocao()
            self.generate_dim_tempo()
            
            # 6. ETL da Tabela de Fato
            logger.info("=== ETAPA 4: CARREGANDO TABELA DE FATO ===")
            
            # Resetar conexão antes da tabela de fato
            self.reset_dw_connection()
            
            self.extract_and_transform_vendas()
            
            # 7. Criar índices para performance
            logger.info("=== ETAPA 5: CRIANDO ÍNDICES ===")
            if not self.execute_sql_file(self.conn_dw, scripts_dir / "cria_indices_dw.sql", "Criando índices"):
                logger.warning("Erro ao criar índices, mas o ETL continuou")
            
            # 8. Exibir resumo final do Data Warehouse
            logger.info("=== ETAPA 6: RESUMO FINAL ===")
            self.check_dw_summary()
            
            logger.info("=== PROCESSO ETL CONCLUÍDO COM SUCESSO! ===")
            return True
            
        except Exception as e:
            logger.error(f"Erro no processo ETL: {e}")
            return False
        
        finally:
            if self.conn_crm:
                self.conn_crm.close()
            if self.conn_dw:
                self.conn_dw.close()

# Execução principal
if __name__ == "__main__":
    etl = ETLProcessor()
    success = etl.run_full_etl()
    
    if success:
        print("\n🎉 ETL executado com sucesso!")
    else:
        print("\n❌ Erro na execução do ETL. Verifique os logs.")
