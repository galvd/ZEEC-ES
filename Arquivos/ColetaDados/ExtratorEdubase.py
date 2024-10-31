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
                dados.ano as ano,
                dados.sigla_uf AS sigla_uf,
                diretorio_sigla_uf.nome AS sigla_uf_nome,
                dados.id_municipio AS id_municipio,
                diretorio_id_municipio.nome AS id_municipio_nome,
                dados.id_escola AS id_escola,
                diretorio_id_escola.nome AS id_escola_nome,
                diretorio_id_escola.latitude AS id_escola_latitude,
                diretorio_id_escola.longitude AS id_escola_longitude,
                dados.rede as rede,
                dados.ensino as ensino,
                dados.anos_escolares as anos_escolares,
                dados.taxa_aprovacao as taxa_aprovacao,
                dados.indicador_rendimento as indicador_rendimento,
                dados.nota_saeb_matematica as nota_saeb_matematica,
                dados.nota_saeb_lingua_portuguesa as nota_saeb_lingua_portuguesa,
                dados.nota_saeb_media_padronizada as nota_saeb_media_padronizada,
                dados.ideb as ideb,
                dados.projecao as projecao
            FROM `basedosdados.br_inep_ideb.escola` AS dados
            LEFT JOIN (SELECT DISTINCT sigla,nome  FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_sigla_uf
                ON dados.sigla_uf = diretorio_sigla_uf.sigla
            LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
                ON dados.id_municipio = diretorio_id_municipio.id_municipio
            LEFT JOIN (SELECT DISTINCT id_escola,nome,latitude,longitude  FROM `basedosdados.br_bd_diretorios_brasil.escola`) AS diretorio_id_escola
                ON dados.id_escola = diretorio_id_escola.id_escola
    
    WHERE 
        ano = {ano}
        AND dados.id_municipio IN ({cidades})
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



