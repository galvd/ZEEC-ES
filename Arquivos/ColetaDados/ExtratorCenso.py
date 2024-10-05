"""
ExtratorCenso.py
Este módulo contém funções para extrair dados relacionados aos censos de 2022 fornecidos pela Base dos Dados. As funções utilizam SQL para consultar a Base dos Dados e retornar os resultados filtrados e processados.

Dependências
sys e json para manipulação de caminhos e configuração.
extrair_dados_sql do módulo Extrator para executar consultas SQL e processar os dados retornados.
Funções
extrair_censo_agua
Extrai dados sobre o abastecimento de água dos municípios a partir da base do IBGE para o ano de 2022.

Parâmetros:

cidades (list): Lista de cidades para filtrar os dados.
main_dir (str, opcional): Diretório principal onde os arquivos serão salvos.
uf (list, opcional): Lista de unidades federativas para filtrar os dados.
limit (str, opcional): Limitação adicional para a consulta SQL.
Retorno:

DataFrame com dados de abastecimento de água, contendo colunas como ano, id_municipio, id_municipio_nome, tipo_ligacao_rede_geral e domicilios.
extrair_censo_esgoto
Extrai dados sobre o esgotamento sanitário dos municípios a partir da base do IBGE para o ano de 2022.

Parâmetros:

cidades (list): Lista de cidades para filtrar os dados.
main_dir (str, opcional): Diretório principal onde os arquivos serão salvos.
ufs (list, opcional): Lista de unidades federativas para filtrar os dados.
limit (str, opcional): Limitação adicional para a consulta SQL.
Retorno:

DataFrame com dados de esgotamento sanitário, contendo colunas como ano, id_municipio, id_municipio_nome, tipo_esgotamento_sanitario e domicilios.
extrair_censo_pop
Extrai dados sobre a população residente dos municípios a partir da base do IBGE para o ano de 2022.

Parâmetros:

cidades (list): Lista de cidades para filtrar os dados.
main_dir (str, opcional): Diretório principal onde os arquivos serão salvos.
ufs (list, opcional): Lista de unidades federativas para filtrar os dados.
limit (str, opcional): Limitação adicional para a consulta SQL.
Retorno:

DataFrame com dados de população residente, contendo colunas como id_municipio, id_municipio_nome, forma_declaracao_idade, sexo, idade, idade_anos, grupo_idade e populacao_residente.
extrair_censo_alfabetizados
Extrai dados sobre a taxa de alfabetização por cor/raça e grupo etário dos municípios a partir da base do IBGE para o ano de 2022.

Parâmetros:

cidades (list): Lista de cidades para filtrar os dados.
main_dir (str, opcional): Diretório principal onde os arquivos serão salvos.
ufs (list, opcional): Lista de unidades federativas para filtrar os dados.
limit (str, opcional): Limitação adicional para a consulta SQL.
Retorno:

DataFrame com dados de alfabetização, contendo colunas como id_municipio, id_municipio_nome, cor_raca, sexo, grupo_idade e taxa_alfabetizacao.

"""


from __future__ import annotations
import sys, json

with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

from Arquivos.ColetaDados.Extrator import extrair_dados_sql



def extrair_censo_agua(cidades: list, main_dir: str = None, uf = list, limit: str = ""):

    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/562b56a3-0b01-4735-a049-eeac5681f056?table=95106d6f-e36e-4fed-b8e9-99c41cd99ecf
    query_censo_agua = """
        SELECT
        ano,
        dados.id_municipio AS id_municipio,
        diretorio_id_municipio.nome AS id_municipio_nome,
        tipo_ligacao_rede_geral,
        domicilios
    FROM `basedosdados.br_ibge_censo_2022.domicilio_ligacao_abastecimento_agua_municipio` AS dados
    LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
        ON dados.id_municipio = diretorio_id_municipio.id_municipio

        WHERE 
            """
    
    processamento_censo_agua = extrair_dados_sql(
    table_name= "censo_agua",
    anos=[2022],
    cidades=cidades,
    query_base=query_censo_agua,
    main_dir=main_dir,
    limit=limit
    )


def extrair_censo_esgoto(cidades: list, main_dir: str = None, ufs = list, limit: str = ""):

    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/08a1546e-251f-4546-9fe0-b1e6ab2b203d?table=77560413-eab2-4c43-abb3-d3694c3ea713
    query_censo_esgoto = """
        SELECT
        ano,
        dados.id_municipio AS id_municipio,
        diretorio_id_municipio.nome AS id_municipio_nome,
        tipo_esgotamento_sanitario,
        domicilios
    FROM `basedosdados.br_ibge_censo_2022.domicilio_esgotamento_sanitario_municipio` AS dados
    LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
        ON dados.id_municipio = diretorio_id_municipio.id_municipio

        WHERE 
            """
    
    processamento_censo_esgoto = extrair_dados_sql(
    table_name= "censo_esgoto",
    anos=[2022],
    cidades=cidades,
    query_base=query_censo_esgoto,
    main_dir=main_dir,
    limit=limit
    )


def extrair_censo_pop(cidades: list, main_dir: str = None, ufs = list, limit: str = ""):

    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/08a1546e-251f-4546-9fe0-b1e6ab2b203d?table=41ca9691-e1f6-4b74-9089-9a9c24c9041b
    query_censo_pop = """
        SELECT
            dados.id_municipio AS id_municipio,
            diretorio_id_municipio.nome AS id_municipio_nome,
            forma_declaracao_idade,
            sexo,
            idade,
            idade_anos,
            grupo_idade,
            populacao_residente
        FROM `basedosdados.br_ibge_censo_2022.populacao_residente_municipio` AS dados
        LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio

        WHERE 

            """
    
    processamento_censo_pop = extrair_dados_sql(
    table_name= "censo_pop",
    anos=[2022],
    cidades=cidades,
    query_base=query_censo_pop,
    main_dir=main_dir,
    limit=limit
    )


def extrair_censo_alfabetizados(cidades: list, main_dir: str = None, ufs = list, limit: str = ""):

    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/08a1546e-251f-4546-9fe0-b1e6ab2b203d?table=d8cd8ccb-7f1e-4831-8cdb-8a797822f754
    query_censo_alfabetizados = """
        SELECT
            dados.id_municipio AS id_municipio,
            diretorio_id_municipio.nome AS id_municipio_nome,
            cor_raca,
            sexo,
            grupo_idade,
            taxa_alfabetizacao
        FROM `basedosdados.br_ibge_censo_2022.taxa_alfabetizacao_cor_raca_grupo_idade_municipio` AS dados
        LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio

        WHERE 

            """
    
    processamento_censo_alfabetizados = extrair_dados_sql(
    table_name= "censo_alfabetizados",
    anos=[2022],
    cidades=cidades,
    query_base=query_censo_alfabetizados,
    main_dir=main_dir,
    limit=limit
    )
