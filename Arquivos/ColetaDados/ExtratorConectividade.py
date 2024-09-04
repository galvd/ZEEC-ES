from __future__ import annotations
import json
import sys
with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

from Arquivos.ColetaDados.Extrator import extrair_dados



cloud_id = config['cloud_id']


def extrair_internet_acs(anos: list, cidades: list, save_dir: str = None, ufs: str = "", mes: int = None, limit: str = ""):

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

    processamento_internet = extrair_dados(
    table_name= "banda_larga",
    anos=anos,
    cidades=cidades,
    query_base=query_internet,
    save_dir=save_dir,
    ufs=ufs,
    mes = mes,
    limit=limit
    )

    return processamento_internet


def extrair_internet_dens(anos: list, cidades: list, save_dir: str = None, ufs: str = "", mes: int = None, limit: str = ""):

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

    processamento_internet = extrair_dados(
    table_name= "densidade_internet",
    anos=anos,
    cidades=cidades,
    query_base=query_internet,
    save_dir=save_dir,
    ufs=ufs,
    mes = mes,
    limit=limit
    )

    return processamento_internet