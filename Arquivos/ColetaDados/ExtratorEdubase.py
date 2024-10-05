"""
ExtratorEdubase.py
Este módulo contém uma função para extrair dados da base de educação, especificamente relacionados ao Sistema de Avaliação da Educação Básica (SAEB) do Brasil.

Dependências
sys e json para manipulação de caminhos e configuração.
extrair_dados_sql do módulo Extrator para executar consultas SQL e processar os dados extraídos.
Funções
extrair_edu_base
Extrai dados educacionais a partir da base de dados do SAEB, com foco em informações de desempenho escolar por município.

Parâmetros:

anos (list): Lista de anos para filtrar os dados.
cidades (list): Lista de IDs de municípios para filtrar os dados.
main_dir (str, opcional): Diretório principal onde os arquivos serão salvos.
ufs (str, opcional): Unidade federativa para filtrar os dados (não utilizado na consulta).
limit (str, opcional): Limitação adicional para a consulta SQL (não utilizado na consulta).
Retorno:

DataFrame com dados educacionais, contendo colunas como ano, rede, localizacao, sigla_uf, id_municipio, disciplina, serie, media, e níveis de desempenho (nivel_0 a nivel_10).
Configuração
O módulo carrega configurações de um arquivo JSON localizado no diretório '.\\Arquivos\\config.json'. Essas configurações incluem o caminho para a rede.

Observações
A consulta SQL é adaptada para a tabela de dados do SAEB e filtra os resultados com base no ano e nos municípios especificados.
O tipo de processamento e outros parâmetros adicionais são configuráveis através dos parâmetros da função extrair_edu_base, mas na consulta SQL atual, apenas o ano e os municípios são utilizados.
"""


from __future__ import annotations
import sys, json

with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

from Arquivos.ColetaDados.Extrator import extrair_dados_sql



def extrair_edu_base(anos: list, cidades: list, main_dir: str = None, ufs: str = "", limit: str = ""):

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

    
    processamento_edu_base = extrair_dados_sql(
    table_name= "educ_base",
    anos=anos,
    cidades=cidades,
    query_base=query_edu_base,
    main_dir=main_dir,
    ufs=ufs,
    limit=limit
    )



