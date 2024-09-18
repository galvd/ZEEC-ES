"""
ExtratorCNPJ.py
Este módulo contém funções para extrair e processar dados de empresas a partir da base do CNPJ e de outras fontes relacionadas. Ele inclui consultas SQL para obter dados de estabelecimentos e informações dos municípios, bem como funções para baixar e processar arquivos de CNPJ.

Dependências
sys e json para manipulação de caminhos e configuração.
extrair_dados_sql e baixar_e_processar_cnpjs do módulo Extrator para executar consultas SQL e processar arquivos de CNPJ.
MainParameters do módulo ToolsColeta para obter parâmetros de configuração.
Funções
extrair_empresas_bd
Extrai dados sobre empresas a partir da base de dados para anos e cidades especificados.

Parâmetros:

anos (list): Lista de anos para filtrar os dados.
cidades (list): Lista de cidades para filtrar os dados.
main_dir (str, opcional): Diretório principal onde os arquivos serão salvos.
ufs (list, opcional): Lista de unidades federativas para filtrar os dados.
mes (int, opcional): Mês para filtrar os dados.
limit (str, opcional): Limitação adicional para a consulta SQL.
Retorno:

DataFrame com dados de empresas, contendo colunas como ano, mes, cnpj, identificador_matriz_filial, situacao_cadastral, data_situacao_cadastral, motivo_situacao_cadastral, nome_cidade_exterior, id_pais, data_inicio_atividade, cnae_fiscal_principal, cnae_fiscal_secundaria, sigla_uf, sigla_uf_nome, id_municipio, id_municipio_nome, id_municipio_rf, tipo_logradouro, logradouro, numero, complemento, bairro e cep.
extrair_munic_dict
Extrai um dicionário com informações sobre municípios, incluindo códigos e nomes, a partir da base de dados.

Parâmetros:

main_dir (str, opcional): Diretório principal onde os arquivos serão salvos.
Retorno:

DataFrame com dicionário de municípios, contendo colunas como id_municipio_rf, id_municipio_nome, id_municipio, e sigla_uf.
extrair_empresas_url
Baixa e processa arquivos de CNPJ a partir de uma URL usando o método de processamento especificado.

Parâmetros:

cidades (list): Lista de cidades para filtrar os dados.
processadores (str): Tipo de processador a ser utilizado ('um', 'dois' ou 'multi').
main_dir (str, opcional): Diretório principal onde os arquivos serão salvos.
ufs (list, opcional): Lista de unidades federativas para filtrar os dados.
anos (list, opcional): Lista de anos para filtrar os dados.
Retorno:

DataFrame com dados processados a partir dos arquivos de CNPJ.
Configuração
O módulo carrega configurações de um arquivo JSON localizado no diretório '.\\Arquivos\\config.json'. Essas configurações incluem o caminho para a rede e URLs usadas para baixar dados.

Observações
As consultas SQL são adaptadas para diferentes tabelas e dados, e são construídas dinamicamente com base nos parâmetros fornecidos.
O tipo de processador deve ser um dos seguintes: 'um', 'dois' ou 'multi'.
"""



from __future__ import annotations
import json, sys
with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])


from Arquivos.ColetaDados.Extrator import extrair_dados_sql, baixar_e_processar_cnpjs
from Arquivos.ColetaDados.ToolsColeta import MainParameters

param = MainParameters()
cnae_lista = param.cnae_analise()
cnae_sql = "|".join(f"{cnae}" for cnae in cnae_lista)


def extrair_empresas_bd(anos: list, cidades: list, main_dir: str = None, ufs: list = [], mes: int = None, limit: str = ""):

    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/e43f0d5b-43cf-4bfb-8d90-c38a4e0d7c4f?table=b8432ff5-06c8-45ca-b8b6-33fceb24089d
    query_empresas = """
            WITH 
        dicionario_identificador_matriz_filial AS (
            SELECT
                chave AS chave_identificador_matriz_filial,
                valor AS descricao_identificador_matriz_filial
            FROM `basedosdados.br_me_cnpj.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'identificador_matriz_filial'
                AND id_tabela = 'estabelecimentos'
        ),
        dicionario_situacao_cadastral AS (
            SELECT
                chave AS chave_situacao_cadastral,
                valor AS descricao_situacao_cadastral
            FROM `basedosdados.br_me_cnpj.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'situacao_cadastral'
                AND id_tabela = 'estabelecimentos'
        ),
        dicionario_id_pais AS (
            SELECT
                chave AS chave_id_pais,
                valor AS descricao_id_pais
            FROM `basedosdados.br_me_cnpj.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'id_pais'
                AND id_tabela = 'estabelecimentos'
        )
        SELECT
            EXTRACT(YEAR FROM data) AS ano,
            EXTRACT(MONTH FROM data) AS mes,
            cnpj,
            descricao_identificador_matriz_filial AS identificador_matriz_filial,
            descricao_situacao_cadastral AS situacao_cadastral,
            data_situacao_cadastral,
            motivo_situacao_cadastral,
            nome_cidade_exterior,
            descricao_id_pais AS id_pais,
            data_inicio_atividade,
            cnae_fiscal_principal,
            cnae_fiscal_secundaria,
            dados.sigla_uf AS sigla_uf,
            diretorio_sigla_uf.nome AS sigla_uf_nome,
            dados.id_municipio AS id_municipio,
            diretorio_id_municipio.nome AS id_municipio_nome,
            id_municipio_rf,
            tipo_logradouro,
            logradouro,
            numero,
            complemento,
            bairro,
            cep
        FROM `basedosdados.br_me_cnpj.estabelecimentos` AS dados
        LEFT JOIN `dicionario_identificador_matriz_filial`
            ON dados.identificador_matriz_filial = chave_identificador_matriz_filial
        LEFT JOIN `dicionario_situacao_cadastral`
            ON dados.situacao_cadastral = chave_situacao_cadastral
        LEFT JOIN `dicionario_id_pais`
            ON dados.id_pais = chave_id_pais
        LEFT JOIN (SELECT DISTINCT sigla,nome  FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_sigla_uf
            ON dados.sigla_uf = diretorio_sigla_uf.sigla
        LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio

        WHERE 
            EXTRACT(YEAR FROM data) = {ano}             

            """
    query_empresas+= f"""AND (REGEXP_CONTAINS(cnae_fiscal_principal, r'^({cnae_sql})') 
                        OR REGEXP_CONTAINS(cnae_fiscal_secundaria, r'^({cnae_sql})'))\n"""

    processamento_empresas = extrair_dados_sql(
    table_name= "cnpj_empresas",
    anos=anos,
    cidades=cidades,
    query_base=query_empresas,
    main_dir=main_dir,
    ufs=ufs,
    limit=limit
    )

    return processamento_empresas

    

def extrair_munic_dict(main_dir: str = None):
    # Query que gera o dicionário com nome, código IBGE, código SERPRO (fiscal da RF) dos municípios do Brasil

    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/e43f0d5b-43cf-4bfb-8d90-c38a4e0d7c4f?table=b8432ff5-06c8-45ca-b8b6-33fceb24089d
    query_munic = """
            WITH 
            dicionario_identificador_matriz_filial AS (
                SELECT
                    chave AS chave_identificador_matriz_filial,
                    valor AS descricao_identificador_matriz_filial
                FROM `basedosdados.br_me_cnpj.dicionario`
                WHERE
                    nome_coluna = 'identificador_matriz_filial'
                    AND id_tabela = 'estabelecimentos'
            ),
            dicionario_situacao_cadastral AS (
                SELECT
                    chave AS chave_situacao_cadastral,
                    valor AS descricao_situacao_cadastral
                FROM `basedosdados.br_me_cnpj.dicionario`
                WHERE
                    nome_coluna = 'situacao_cadastral'
                    AND id_tabela = 'estabelecimentos'
            ),
            dicionario_id_pais AS (
                SELECT
                    chave AS chave_id_pais,
                    valor AS descricao_id_pais
                FROM `basedosdados.br_me_cnpj.dicionario`
                WHERE
                    nome_coluna = 'id_pais'
                    AND id_tabela = 'estabelecimentos'
            )
        SELECT
            DISTINCT
            dados.id_municipio_rf AS id_municipio_rf,
            diretorio_id_municipio.nome AS id_municipio_nome,
            dados.id_municipio AS id_municipio,
            dados.sigla_uf AS sigla_uf
        FROM `basedosdados.br_me_cnpj.estabelecimentos` AS dados
        LEFT JOIN `dicionario_identificador_matriz_filial`
            ON dados.identificador_matriz_filial = chave_identificador_matriz_filial
        LEFT JOIN `dicionario_situacao_cadastral`
            ON dados.situacao_cadastral = chave_situacao_cadastral
        LEFT JOIN `dicionario_id_pais`
            ON dados.id_pais = chave_id_pais
        LEFT JOIN (
            SELECT DISTINCT id_municipio, nome  
            FROM `basedosdados.br_bd_diretorios_brasil.municipio`
        ) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio;
            """

    processamento_munic = extrair_dados_sql(
    table_name= "cods_municipios",
    anos = [2024],
    query_base=query_munic,
    main_dir=main_dir
    )

    return processamento_munic





def extrair_empresas_url(cidades: list, processadores: str, main_dir: str = None, ufs: list = [], anos = []):
    if processadores == 'um':
        threads = 'one'
    elif processadores == 'dois':
        threads = 'dual'
    elif processadores == 'multi':
        threads = 'multi'
    else:
        raise ValueError(f"Tipo de processador '{processadores}' não é válido. Escolha entre 'um', 'dois', ou 'multi'.")

    method = eval(f'{threads}_thread') # Opções: one_thread, dual_thread, multi_thread
    table_name= "cnpj_empresas" # placeholder com nome da tabela
    
    processamento_empresas = baixar_e_processar_cnpjs(
    url_template= param.url_cnpjs(),
    main_dir=main_dir,
    method=method,
    anos= anos,
    cidades= cidades,
    ufs= ufs
    )

    return processamento_empresas


