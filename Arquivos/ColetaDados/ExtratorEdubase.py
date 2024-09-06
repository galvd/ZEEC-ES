from __future__ import annotations
import json
import sys
with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

from Arquivos.ColetaDados.Extrator import extrair_dados





cloud_id = config['cloud_id']

def extrair_edu_base(anos: list, cidades: list, save_dir: str = None, ufs: str = "", limit: str = ""):

    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/e083c9a2-1cee-4342-bedc-535cbad6f3cd?table=213953ad-a609-46a3-b3e0-a0a843cba843
    query_edu_base = """
            SELECT
                ano,
                rede,
                localizacao,
                sigla_uf,
                id_municipio,
                disciplina,
                serie,
                media,
                nivel_0,
                nivel_1,
                nivel_2,
                nivel_3,
                nivel_4,
                nivel_5,
                nivel_6,
                nivel_7,
                nivel_8,
                nivel_9,
                nivel_10
            FROM `basedosdados.br_inep_saeb.municipio` AS dados
    
    WHERE 
        ano = {ano}
        AND id_municipio IN ({cidades})
    """

    
    processamento_edu_base = extrair_dados(
    table_name= "educ_base",
    anos=anos,
    cidades=cidades,
    query_base=query_edu_base,
    save_dir=save_dir,
    ufs=ufs,
    limit=limit
    )

    return processamento_edu_base


