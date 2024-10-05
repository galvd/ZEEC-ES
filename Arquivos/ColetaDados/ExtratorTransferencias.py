"""
ExtratorTransferencias.py
Descrição Geral:

O ExtratorTransferencias.py é um módulo Python destinado à extração de dados sobre transferências financeiras de diferentes fontes públicas. O arquivo utiliza funções para baixar e processar dados de URLs específicas para diferentes tipos de transferências.

Importações:

sys: Utilizado para manipulação de caminhos de sistema e configuração de caminhos adicionais.
json: Utilizado para ler e interpretar o arquivo de configuração JSON.
extrair_transf_url da Arquivos.ColetaDados.Extrator: Função utilizada para extrair dados a partir de URLs fornecidas.
MainParameters da Arquivos.ColetaDados.ToolsColeta: Classe utilizada para obter URLs específicas para transferências.
Configuração:

O arquivo carrega uma configuração a partir de um arquivo JSON localizado em '.\\Arquivos\\config.json'. O caminho de rede especificado no arquivo JSON é adicionado ao sys.path para permitir a importação do módulo Extrator.

Variáveis:

param: Instância da classe MainParameters, utilizada para obter URLs específicas para as transferências.
Funções:

extrair_transferencias_fex
Descrição:

Extrai dados sobre transferências do Fundo de Exportação (FEX) para uma lista de municípios.

Parâmetros:

cidades (list): Lista de IDs de municípios para os quais os dados devem ser extraídos.
main_dir (str, opcional): Diretório principal onde os dados processados serão salvos.
ufs (str, opcional): Códigos das Unidades Federativas (UFs) para filtrar os dados.
anos (list, opcional): Lista de anos para os quais os dados devem ser extraídos (não utilizado na função).
Funcionamento:

Utiliza a função extrair_transf_url para baixar e processar os dados da URL fornecida pela função url_transferencias('fex') da instância param.
Salva os dados extraídos na tabela transferencias_fex.
Retorno:

Retorna o resultado do processamento dos dados de transferências do FEX.
extrair_transferencias_fpm
Descrição:

Extrai dados sobre transferências do Fundo de Participação dos Municípios (FPM) para uma lista de municípios.

Parâmetros:

cidades (list): Lista de IDs de municípios para os quais os dados devem ser extraídos.
main_dir (str, opcional): Diretório principal onde os dados processados serão salvos.
ufs (str, opcional): Códigos das Unidades Federativas (UFs) para filtrar os dados.
anos (list, opcional): Lista de anos para os quais os dados devem ser extraídos (não utilizado na função).
Funcionamento:

Utiliza a função extrair_transf_url para baixar e processar os dados da URL fornecida pela função url_transferencias('fpm') da instância param.
Salva os dados extraídos na tabela transferencias_fpm.
Retorno:

Retorna o resultado do processamento dos dados de transferências do FPM.
extrair_transferencias_fundeb
Descrição:

Extrai dados sobre transferências do Fundo de Desenvolvimento da Educação Básica (FUNDEB) para uma lista de municípios.

Parâmetros:

cidades (list): Lista de IDs de municípios para os quais os dados devem ser extraídos.
main_dir (str, opcional): Diretório principal onde os dados processados serão salvos.
ufs (str, opcional): Códigos das Unidades Federativas (UFs) para filtrar os dados.
anos (list, opcional): Lista de anos para os quais os dados devem ser extraídos (não utilizado na função).
Funcionamento:

Utiliza a função extrair_transf_url para baixar e processar os dados da URL fornecida pela função url_transferencias('fundeb') da instância param.
Salva os dados extraídos na tabela transferencias_fundeb.
Retorno:

Retorna o resultado do processamento dos dados de transferências do FUNDEB.
Notas:

Todas as funções seguem um padrão semelhante, diferindo apenas na URL utilizada e no nome da tabela onde os dados são salvos.
O parâmetro anos é fornecido para todas as funções, mas não é utilizado na lógica de extração.
"""

from __future__ import annotations
import sys, json

with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

from Arquivos.ColetaDados.Extrator import extrair_transf_url
from Arquivos.ColetaDados.ToolsColeta import MainParameters



param = MainParameters()

def extrair_transferencias_fex(cidades: list, main_dir: str = None, ufs: str = "", anos = []):

    processamento_trans_fex = extrair_transf_url(
    url = param.url_transferencias('fex'),
    table_name= "transferencias_fex",
    cidades=cidades,
    main_dir=main_dir,
    ufs=ufs
    )


def extrair_transferencias_fpm(cidades: list, main_dir: str = None, ufs: str = "", anos = []):

    processamento_trans_fpm = extrair_transf_url(
    url = param.url_transferencias('fpm'),
    table_name= "transferencias_fpm",
    cidades=cidades,
    main_dir=main_dir,
    ufs=ufs
    )


def extrair_transferencias_fundeb(cidades: list, main_dir: str = None, ufs: str = "", anos = []):

    processamento_trans_fundeb = extrair_transf_url(
    url = param.url_transferencias('fundeb'),
    table_name= "transferencias_fundeb",
    cidades=cidades,
    main_dir=main_dir,
    ufs=ufs
    )



