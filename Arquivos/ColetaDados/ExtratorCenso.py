from __future__ import annotations
import json
import sys
with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

from Arquivos.ColetaDados.Extrator import extrair_dados



cloud_id = config['cloud_id']

def extrair_censo_agua(anos: list, cidades: list, save_dir: str = None, limit: str = ""):

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
    
    processamento_censo_agua = extrair_dados(
    table_name= "censo_agua",
    anos=anos,
    cidades=cidades,
    query_base=query_censo_agua,
    save_dir=save_dir,
    limit=limit
    )

    return processamento_censo_agua


def extrair_censo_esgoto(anos: list, cidades: list, save_dir: str = None, limit: str = ""):

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
    
    processamento_censo_esgoto = extrair_dados(
    table_name= "censo_esgoto",
    anos=anos,
    cidades=cidades,
    query_base=query_censo_esgoto,
    save_dir=save_dir,
    limit=limit
    )

    return processamento_censo_esgoto


def extrair_censo_pop(anos: list, cidades: list, save_dir: str = None, limit: str = ""):

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
    
    processamento_censo_pop = extrair_dados(
    table_name= "censo_pop",
    anos=anos,
    cidades=cidades,
    query_base=query_censo_pop,
    save_dir=save_dir,
    limit=limit
    )

    return processamento_censo_pop


def extrair_censo_alfabetizados(anos: list, cidades: list, save_dir: str = None, limit: str = ""):

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
    
    processamento_censo_alfabetizados = extrair_dados(
    table_name= "censo_alfabetizados",
    anos=anos,
    cidades=cidades,
    query_base=query_censo_alfabetizados,
    save_dir=save_dir,
    limit=limit
    )

    return processamento_censo_alfabetizados
