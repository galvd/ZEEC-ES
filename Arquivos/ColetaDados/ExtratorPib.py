from __future__ import annotations
import sys, json

with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

from Arquivos.ColetaDados.Extrator import extrair_dados_sql



def extrair_pib_cidades(anos: list, cidades: list, save_dir: str = None, ufs = list, limit: str = ""):

    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/fcf025ca-8b19-4131-8e2d-5ddb12492347?table=fbbbe77e-d234-4113-8af5-98724a956943
    query_pib = """
        SELECT
            dados.id_municipio AS id_municipio,
            diretorio_id_municipio.nome AS id_municipio_nome,
            ano,
            pib,
            impostos_liquidos,
            va,
            va_agropecuaria,
            va_industria,
            va_servicos,
            va_adespss
        FROM `basedosdados.br_ibge_pib.municipio` AS dados
        LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio
        WHERE 
                ano = {ano}              
            """

    processamento_pib = extrair_dados_sql(
    table_name= "pib_municipios",
    anos=anos,
    cidades=cidades,
    ufs=ufs,
    query_base=query_pib,
    save_dir=save_dir,
    limit=limit
    )

    return processamento_pib

