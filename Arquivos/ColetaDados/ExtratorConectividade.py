"""
ExtratorConectividade.py
Este módulo contém funções para extrair dados relacionados à conectividade à internet a partir de fontes da Base dos Dados. As funções fornecem consultas SQL específicas para extrair informações sobre acesso à internet e densidade de internet por município.

Dependências
sys e json para manipulação de caminhos e configuração.
extrair_dados_sql do módulo Extrator para executar consultas SQL e processar os dados extraídos.
Funções
extrair_internet_acs
Extrai dados sobre o acesso à internet (ACS) a partir da base de dados.

Parâmetros:

anos (list): Lista de anos para filtrar os dados.
cidades (list): Lista de cidades para filtrar os dados.
main_dir (str, opcional): Diretório principal onde os arquivos serão salvos.
ufs (str, opcional): Unidade federativa para filtrar os dados.
mes (int, opcional): Mês para filtrar os dados.
limit (str, opcional): Limitação adicional para a consulta SQL.
Retorno:

DataFrame com dados sobre o acesso à internet, contendo colunas como mes, ano, porte_empresa, transmissao, id_municipio, id_municipio_nome, acessos, velocidade, empresa, cnpj, produto, tecnologia, sigla_uf, e sigla_uf_nome.
extrair_internet_dens
Extrai dados sobre a densidade de internet a partir da base de dados.

Parâmetros:

anos (list): Lista de anos para filtrar os dados.
cidades (list): Lista de cidades para filtrar os dados.
main_dir (str, opcional): Diretório principal onde os arquivos serão salvos.
ufs (str, opcional): Unidade federativa para filtrar os dados.
mes (int, opcional): Mês para filtrar os dados.
limit (str, opcional): Limitação adicional para a consulta SQL.
Retorno:

DataFrame com dados sobre a densidade de internet, contendo colunas como sigla_uf, sigla_uf_nome, ano, mes, id_municipio, id_municipio_nome, e densidade.
Configuração
O módulo carrega configurações de um arquivo JSON localizado no diretório '.\\Arquivos\\config.json'. Essas configurações incluem o caminho para a rede.

Observações
As consultas SQL são adaptadas para diferentes tabelas e dados, e são construídas dinamicamente com base nos parâmetros fornecidos.
O tipo de processamento é definido através das opções fornecidas, e a função extrair_dados_sql é responsável por executar as consultas e retornar os resultados.
"""



from __future__ import annotations
import sys, json

with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

from Arquivos.ColetaDados.Extrator import extrair_dados_sql



def extrair_internet_acs(anos: list, cidades: list, main_dir: str = None, ufs: str = "", mes: int = None, limit: str = ""):

    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/562b56a3-0b01-4735-a049-eeac5681f056?table=95106d6f-e36e-4fed-b8e9-99c41cd99ecf
    query_internet = """
        SELECT
            mes,
            ano,
            porte_empresa,
            transmissao,
            dados.id_municipio AS id_municipio,
            diretorio_id_municipio.nome AS id_municipio_nome,
            acessos,
            velocidade,
            empresa,
            cnpj,
            produto,
            tecnologia,
            dados.sigla_uf AS sigla_uf,
            diretorio_sigla_uf.nome AS sigla_uf_nome
        FROM `basedosdados.br_anatel_banda_larga_fixa.microdados` AS dados
        LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio
        LEFT JOIN (SELECT DISTINCT sigla,nome  FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_sigla_uf
            ON dados.sigla_uf = diretorio_sigla_uf.sigla 
        

        WHERE 
                ano = {ano}              
            """

    processamento_internet = extrair_dados_sql(
    table_name= "banda_larga",
    anos=anos,
    cidades=cidades,
    query_base=query_internet,
    main_dir=main_dir,
    ufs=ufs,
    mes = mes,
    limit=limit
    )


def extrair_internet_dens(anos: list, cidades: list, main_dir: str = None, ufs: str = "", mes: int = None, limit: str = ""):

    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/4ba41417-ba19-4022-bc24-6837db973009?table=4f0e4223-756b-4724-99f6-7555fbb19a4c
    query_internet = """
        SELECT
            dados.sigla_uf AS sigla_uf,
            diretorio_sigla_uf.nome AS sigla_uf_nome,
            ano,
            mes,
            dados.id_municipio AS id_municipio,
            diretorio_id_municipio.nome AS id_municipio_nome,
            densidade
        FROM `basedosdados.br_anatel_banda_larga_fixa.densidade_municipio` AS dados
        LEFT JOIN (SELECT DISTINCT sigla,nome  FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_sigla_uf
            ON dados.sigla_uf = diretorio_sigla_uf.sigla
        LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio
        
        WHERE 
                ano = {ano}              
            """

    processamento_internet = extrair_dados_sql(
    table_name= "densidade_internet",
    anos=anos,
    cidades=cidades,
    query_base=query_internet,
    main_dir=main_dir,
    ufs=ufs,
    mes = mes,
    limit=limit
    )
