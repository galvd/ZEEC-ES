"""
ExtratorRais.py
Descrição Geral:

O ExtratorRais.py é um módulo Python desenvolvido para extrair dados dos microdados de estabelecimentos da RAIS (Relação Anual de Informações Sociais). Utiliza consultas SQL para obter informações detalhadas sobre a natureza, tamanho, tipo e CNAE dos estabelecimentos, e processa esses dados com base em parâmetros fornecidos.

Importações:

sys: Utilizado para manipulação de caminhos de sistema e configuração de caminhos adicionais.
json: Utilizado para ler e interpretar o arquivo de configuração JSON.
extrair_dados_sql da Arquivos.ColetaDados.Extrator: Função utilizada para executar consultas SQL e processar os dados extraídos.
MainParameters da Arquivos.ColetaDados.ToolsColeta: Classe utilizada para obter a lista de CNAEs para análise.
Configuração:

O arquivo carrega uma configuração a partir de um arquivo JSON localizado em '.\\Arquivos\\config.json'. O caminho de rede especificado no arquivo JSON é adicionado ao sys.path para permitir a importação do módulo Extrator.

Variáveis:

cnae_lista: Lista de CNAEs obtida através da classe MainParameters.
cnae_sql: String contendo os CNAEs formatados para uso em uma expressão regular na consulta SQL.
Funções:

extrair_rais
Descrição:

Extrai dados dos microdados de estabelecimentos da RAIS para uma lista de municípios e anos específicos. Os dados são obtidos através de uma consulta SQL complexa que inclui diversas tabelas de referência e dicionários.

Parâmetros:

anos (list): Lista de anos para os quais os dados devem ser extraídos.
cidades (list): Lista de IDs de municípios para os quais os dados devem ser extraídos.
main_dir (str, opcional): Diretório principal onde os dados processados serão salvos.
ufs (str, opcional): Códigos das Unidades Federativas (UFs) para filtrar os dados.
limit (str, opcional): Limite para a consulta SQL (pode ser usado para paginação ou restrição de resultados).
Funcionamento:

Define uma consulta SQL (query_rais) que utiliza a cláusula WITH para criar tabelas temporárias com dicionários de descrições para diversos atributos dos estabelecimentos.
Realiza a seleção dos dados dos estabelecimentos, incluindo atributos como quantidade de vínculos, natureza do estabelecimento, CNAE, e outros detalhes.
Adiciona uma condição WHERE para filtrar os dados com base no ano e nos CNAEs especificados.
Utiliza a função extrair_dados_sql para executar a consulta SQL e processar os dados extraídos com base nos parâmetros fornecidos.
Retorna o resultado do processamento dos dados.
Consultas SQL:

A consulta SQL utilizada (query_rais) realiza a extração dos seguintes dados:

ano: Ano dos dados.
sigla_uf: Sigla da UF.
sigla_uf_nome: Nome da UF.
id_municipio: Identificador do município.
id_municipio_nome: Nome do município.
quantidade_vinculos_ativos: Quantidade de vínculos ativos.
quantidade_vinculos_clt: Quantidade de vínculos CLT.
quantidade_vinculos_estatutarios: Quantidade de vínculos estatutários.
natureza_estabelecimento: Descrição da natureza do estabelecimento.
tamanho_estabelecimento: Descrição do tamanho do estabelecimento.
tipo_estabelecimento: Descrição do tipo de estabelecimento.
indicador_cei_vinculado: Indicador se o CEI está vinculado.
indicador_pat: Indicador PAT.
indicador_simples: Indicador de participação no Simples.
indicador_atividade_ano: Indicador de atividade no ano.
cnae_1 e cnae_2: Códigos CNAE e suas descrições detalhadas.
subsetor_ibge: Descrição do subsetor IBGE.
subatividade_ibge: Subatividade IBGE.
Notas:

A consulta SQL inclui junções com tabelas de referência e dicionários para enriquecer os dados com descrições e informações adicionais.
A variável cnae_sql é utilizada para filtrar os dados com base na lista de CNAEs fornecida, utilizando expressões regulares para correspondência parcial.

"""



from __future__ import annotations
import sys, json

with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

from Arquivos.ColetaDados.Extrator import extrair_dados_sql
from Arquivos.ColetaDados.ToolsColeta import MainParameters


cnae_lista = MainParameters().cnae_analise()
cnae_sql = "|".join(f"{cnae}" for cnae in cnae_lista)


def extrair_rais(anos: list, cidades: list, main_dir: str = None, ufs: str = "", limit: str = ""):

    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/3e7c4d58-96ba-448e-b053-d385a829ef00?table=86b69f96-0bfe-45da-833b-6edc9a0af213
    query_rais = """
    WITH 
    dicionario_natureza_estabelecimento AS (
        SELECT
            chave AS chave_natureza_estabelecimento,
            valor AS descricao_natureza_estabelecimento
        FROM `basedosdados.br_me_rais.dicionario`
        WHERE
            nome_coluna = 'natureza_estabelecimento'
            AND id_tabela = 'microdados_estabelecimentos'
    ),
    dicionario_tamanho_estabelecimento AS (
        SELECT
            chave AS chave_tamanho_estabelecimento,
            valor AS descricao_tamanho_estabelecimento
        FROM `basedosdados.br_me_rais.dicionario`
        WHERE
            nome_coluna = 'tamanho_estabelecimento'
            AND id_tabela = 'microdados_estabelecimentos'
    ),
    dicionario_tipo_estabelecimento AS (
        SELECT
            chave AS chave_tipo_estabelecimento,
            valor AS descricao_tipo_estabelecimento
        FROM `basedosdados.br_me_rais.dicionario`
        WHERE
            nome_coluna = 'tipo_estabelecimento'
            AND id_tabela = 'microdados_estabelecimentos'
    ),
    dicionario_subsetor_ibge AS (
        SELECT
            chave AS chave_subsetor_ibge,
            valor AS descricao_subsetor_ibge
        FROM `basedosdados.br_me_rais.dicionario`
        WHERE
            nome_coluna = 'subsetor_ibge'
            AND id_tabela = 'microdados_estabelecimentos'
    )
    SELECT
        ano,
        dados.sigla_uf AS sigla_uf,
        diretorio_sigla_uf.nome AS sigla_uf_nome,
        dados.id_municipio AS id_municipio,
        diretorio_id_municipio.nome AS id_municipio_nome,
        quantidade_vinculos_ativos,
        quantidade_vinculos_clt,
        quantidade_vinculos_estatutarios,
        descricao_natureza_estabelecimento AS natureza_estabelecimento,
        descricao_tamanho_estabelecimento AS tamanho_estabelecimento,
        descricao_tipo_estabelecimento AS tipo_estabelecimento,
        indicador_cei_vinculado,
        indicador_pat,
        indicador_simples,
        indicador_atividade_ano,
        dados.cnae_1 AS cnae_1,
        diretorio_cnae_1.descricao AS cnae_1_descricao,
        diretorio_cnae_1.descricao_grupo AS cnae_1_descricao_grupo,
        diretorio_cnae_1.descricao_divisao AS cnae_1_descricao_divisao,
        diretorio_cnae_1.descricao_secao AS cnae_1_descricao_secao,
        dados.cnae_2 AS cnae_2,
        diretorio_cnae_2.descricao_subclasse AS cnae_2_descricao_subclasse,
        diretorio_cnae_2.descricao_classe AS cnae_2_descricao_classe,
        diretorio_cnae_2.descricao_grupo AS cnae_2_descricao_grupo,
        diretorio_cnae_2.descricao_divisao AS cnae_2_descricao_divisao,
        diretorio_cnae_2.descricao_secao AS cnae_2_descricao_secao,
        descricao_subsetor_ibge AS subsetor_ibge,
        subatividade_ibge
    FROM `basedosdados.br_me_rais.microdados_estabelecimentos` AS dados
    LEFT JOIN (SELECT DISTINCT sigla, nome FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_sigla_uf
        ON dados.sigla_uf = diretorio_sigla_uf.sigla
    LEFT JOIN (SELECT DISTINCT id_municipio, nome FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
        ON dados.id_municipio = diretorio_id_municipio.id_municipio
    LEFT JOIN dicionario_natureza_estabelecimento
        ON dados.natureza_estabelecimento = chave_natureza_estabelecimento
    LEFT JOIN dicionario_tamanho_estabelecimento
        ON dados.tamanho_estabelecimento = chave_tamanho_estabelecimento
    LEFT JOIN dicionario_tipo_estabelecimento
        ON dados.tipo_estabelecimento = chave_tipo_estabelecimento
    LEFT JOIN (SELECT DISTINCT cnae_1, descricao, descricao_grupo, descricao_divisao, descricao_secao FROM `basedosdados.br_bd_diretorios_brasil.cnae_1`) AS diretorio_cnae_1
        ON dados.cnae_1 = diretorio_cnae_1.cnae_1
    LEFT JOIN (SELECT DISTINCT subclasse, descricao_subclasse, descricao_classe, descricao_grupo, descricao_divisao, descricao_secao FROM `basedosdados.br_bd_diretorios_brasil.cnae_2`) AS diretorio_cnae_2
        ON dados.cnae_2 = diretorio_cnae_2.subclasse
    LEFT JOIN dicionario_subsetor_ibge
        ON dados.subsetor_ibge = chave_subsetor_ibge
    WHERE 
        ano = {ano}

    """
    # query_rais+= f"""AND (REGEXP_CONTAINS(dados.cnae_1, r'^({cnae_sql})') 
    #                     OR REGEXP_CONTAINS(dados.cnae_2, r'^({cnae_sql})'))\n"""

    
    processamento_rais = extrair_dados_sql(
    table_name= "rais",
    anos=anos,
    cidades=cidades,
    query_base=query_rais,
    main_dir=main_dir,
    ufs=ufs,
    limit=limit
    )








