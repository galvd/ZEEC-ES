from __future__ import annotations
import sys, json


with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

from Arquivos.ColetaDados.Extrator import extrair_antenas_url
from Arquivos.ColetaDados.ToolsColeta import MainParameters

param = MainParameters()
ufs = param.ufs()
cidades_ibge = param.cod_ibge()


def extrair_antenas_es(cidades: list, main_dir: str = None, ufs: list = ["ES"], anos = []):
    """
    Extrai antenas a partir do arquivo zip disponibilizado pela Anatel,
    filtrando os registros conforme os códigos IBGE das cidades e as UFs informadas.

    :param cidades: Lista de códigos de IBGE das cidades de interesse.
    :param main_dir: Diretório onde será salvo o arquivo parquet (opcional).
    :param ufs: Lista de UFs de interesse (padrão: ["ES"]).
    :param anos: Parâmetro mantido para compatibilidade (não utilizado neste processamento).
    """
    processamento_antenas = extrair_antenas_url(
        url="https://www.anatel.gov.br/dadosabertos/paineis_de_dados/outorga_e_licenciamento/estacoes_smp.zip",
        table_name="antenas_es",
        cidades_ibge=cidades,
        main_dir=main_dir,
        ufs=ufs
    )




import os
extrair_antenas_es(cidades = cidades_ibge, main_dir = os.getcwd(), ufs = ufs)