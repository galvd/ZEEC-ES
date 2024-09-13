from __future__ import annotations
import sys, json

with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

from Arquivos.ColetaDados.Extrator import extrair_transf_url



def extrair_transferencias_fex(cidades: list, save_dir: str = None, ufs: str = "", anos = []):

    processamento_trans_fex = extrair_transf_url(
    url = r"https://www.tesourotransparente.gov.br/ckan/dataset/3b5a779d-78f5-4602-a6b7-23ece6d60f27/resource/4ca6aad2-fa9d-48e1-a608-5614578d7df2/download/FEX-por-Municipio.csv",
    table_name= "transferencias_fex",
    cidades=cidades,
    save_dir=save_dir,
    ufs=ufs
    )

    return processamento_trans_fex


def extrair_transferencias_fpm(cidades: list, save_dir: str = None, ufs: str = "", anos = []):

    processamento_trans_fpm = extrair_transf_url(
    url = r"https://www.tesourotransparente.gov.br/ckan/dataset/3b5a779d-78f5-4602-a6b7-23ece6d60f27/resource/d69ff32a-6681-4114-81f0-233bb6b17f58/download/FPM-por-Municipio.csv",
    table_name= "transferencias_fpm",
    cidades=cidades,
    save_dir=save_dir,
    ufs=ufs
    )

    return processamento_trans_fpm


def extrair_transferencias_fundeb(cidades: list, save_dir: str = None, ufs: str = "", anos = []):

    processamento_trans_fundeb = extrair_transf_url(
    url = r"https://www.tesourotransparente.gov.br/ckan/dataset/3b5a779d-78f5-4602-a6b7-23ece6d60f27/resource/18d5b0ae-8037-461e-8685-3f0d7752a287/download/FUNDEB-por-Municipio.csv",
    table_name= "transferencias_fundeb",
    cidades=cidades,
    save_dir=save_dir,
    ufs=ufs
    )

    return processamento_trans_fundeb



