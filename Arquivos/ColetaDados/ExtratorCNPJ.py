"""
ExtratorCNPJ.py
Este módulo contém funções para extrair e processar dados de empresas a partir da base do CNPJ e de outras fontes relacionadas. Ele inclui consultas SQL para obter dados de estabelecimentos e informações dos municípios, bem como funções para baixar e processar arquivos de CNPJ.

Dependências
sys e json para manipulação de caminhos e configuração.
extrair_dados_sql e baixar_e_processar_cnpjs do módulo Extrator para executar consultas SQL e processar arquivos de CNPJ.
MainParameters do módulo ToolsColeta para obter parâmetros de configuração.
Funções
extrair_empresas_bd
Extrai dados sobre empresas a partir da base de dados para anos e cidades especificados.

Parâmetros:

anos (list): Lista de anos para filtrar os dados.
cidades (list): Lista de cidades para filtrar os dados.
main_dir (str, opcional): Diretório principal onde os arquivos serão salvos.
ufs (list, opcional): Lista de unidades federativas para filtrar os dados.
mes (int, opcional): Mês para filtrar os dados.
limit (str, opcional): Limitação adicional para a consulta SQL.
Retorno:

DataFrame com dados de empresas, contendo colunas como ano, mes, cnpj, identificador_matriz_filial, situacao_cadastral, data_situacao_cadastral, motivo_situacao_cadastral, nome_cidade_exterior, id_pais, data_inicio_atividade, cnae_fiscal_principal, cnae_fiscal_secundaria, sigla_uf, sigla_uf_nome, id_municipio, id_municipio_nome, id_municipio_rf, tipo_logradouro, logradouro, numero, complemento, bairro e cep.
extrair_munic_dict
Extrai um dicionário com informações sobre municípios, incluindo códigos e nomes, a partir da base de dados.

Parâmetros:

main_dir (str, opcional): Diretório principal onde os arquivos serão salvos.
Retorno:

DataFrame com dicionário de municípios, contendo colunas como id_municipio_rf, id_municipio_nome, id_municipio, e sigla_uf.
extrair_empresas_url
Baixa e processa arquivos de CNPJ a partir de uma URL usando o método de processamento especificado.

Parâmetros:

cidades (list): Lista de cidades para filtrar os dados.
processadores (str): Tipo de processador a ser utilizado ('um', 'dois' ou 'multi').
main_dir (str, opcional): Diretório principal onde os arquivos serão salvos.
ufs (list, opcional): Lista de unidades federativas para filtrar os dados.
anos (list, opcional): Lista de anos para filtrar os dados.
Retorno:

DataFrame com dados processados a partir dos arquivos de CNPJ.
Configuração
O módulo carrega configurações de um arquivo JSON localizado no diretório '.\\Arquivos\\config.json'. Essas configurações incluem o caminho para a rede e URLs usadas para baixar dados.

Observações
As consultas SQL são adaptadas para diferentes tabelas e dados, e são construídas dinamicamente com base nos parâmetros fornecidos.
O tipo de processador deve ser um dos seguintes: 'um', 'dois' ou 'multi'.
"""



from __future__ import annotations
import json, sys
with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])


from Arquivos.ColetaDados.Extrator import extrair_dados_sql, baixar_e_processar_cnpjs
from Arquivos.ColetaDados.ToolsColeta import MainParameters

# import para method() em extrair_cnpjs_url
from Arquivos.TratamentoDados.ToolsTratamento import CnpjProcess, one_thread, dual_thread, multi_thread  

param = MainParameters()
cnae_lista = param.cnae_analise()
cnae_sql = "|".join(f"{cnae}" for cnae in cnae_lista)


def extrair_estabelecimentos_bd(anos: list, main_dir: str = None, ufs: list = [], limit: str = ""):
    # anos_sql = ", ".join(f"{ano}" for ano in anos)

    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/e43f0d5b-43cf-4bfb-8d90-c38a4e0d7c4f?table=b8432ff5-06c8-45ca-b8b6-33fceb24089d
    query_estabelecimentos = """
            WITH 
        dicionario_identificador_matriz_filial AS (
            SELECT
                chave AS chave_identificador_matriz_filial,
                valor AS descricao_identificador_matriz_filial
            FROM `basedosdados.br_me_cnpj.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'identificador_matriz_filial'
                AND id_tabela = 'estabelecimentos'
        ),
        dicionario_situacao_cadastral AS (
            SELECT
                chave AS chave_situacao_cadastral,
                valor AS descricao_situacao_cadastral
            FROM `basedosdados.br_me_cnpj.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'situacao_cadastral'
                AND id_tabela = 'estabelecimentos'
        ),
        dicionario_id_pais AS (
            SELECT
                chave AS chave_id_pais,
                valor AS descricao_id_pais
            FROM `basedosdados.br_me_cnpj.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'id_pais'
                AND id_tabela = 'estabelecimentos'
        )
        SELECT
            data,
            cnpj,
            descricao_identificador_matriz_filial AS identificador_matriz_filial,
            descricao_situacao_cadastral AS situacao_cadastral,
            data_situacao_cadastral,
            motivo_situacao_cadastral,
            nome_cidade_exterior,
            descricao_id_pais AS id_pais,
            data_inicio_atividade,
            cnae_fiscal_principal,
            cnae_fiscal_secundaria,
            dados.sigla_uf AS sigla_uf,
            diretorio_id_municipio.nome AS id_municipio_nome,
            id_municipio_rf,
            bairro,
            cep
        FROM `basedosdados.br_me_cnpj.estabelecimentos` AS dados
        LEFT JOIN `dicionario_identificador_matriz_filial`
            ON dados.identificador_matriz_filial = chave_identificador_matriz_filial
        LEFT JOIN `dicionario_situacao_cadastral`
            ON dados.situacao_cadastral = chave_situacao_cadastral
        LEFT JOIN `dicionario_id_pais`
            ON dados.id_pais = chave_id_pais
        LEFT JOIN (SELECT DISTINCT sigla,nome  FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_sigla_uf
            ON dados.sigla_uf = diretorio_sigla_uf.sigla
        LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio

        WHERE 
            EXTRACT(YEAR FROM data) = {ano}          

            """


    processamento_estabelecimentos = extrair_dados_sql(
    table_name= "cnpj_estabelecimentos",
    anos=anos,
    query_base=query_estabelecimentos,
    main_dir=main_dir,
    ufs=ufs,
    limit=limit
    )
    process = CnpjProcess()
    for ano in anos:
        
        process.cnpj_treat(main_dir=main_dir, ano = ano) # não recebe o parâmetro meses, pois o parâmetro é para extração via url

# Dicionário dos Códigos: https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf
def extrair_cnpjs_url(cidades: list, processadores: str, fonte: str, main_dir: str = None, ufs: list = [], anos = []):
    if processadores == 'um':
        threads = 'one'
    elif processadores == 'dois':
        threads = 'dual'
    elif processadores == 'multi':
        threads = 'multi'
    else:
        raise ValueError(f"Tipo de processador '{processadores}' não é válido. Escolha entre 'um', 'dois', ou 'multi'.")

    method = eval(f'{threads}_thread') # Opções: one_thread, dual_thread, multi_thread
    table_name= f"cnpj_{fonte}" # placeholder com nome da tabela
    
    processamento_estabelecimentos = baixar_e_processar_cnpjs(
    url_template= param.url_cnpjs(),
    main_dir=main_dir,
    method=method,
    anos= anos,
    cidades= cidades,
    ufs= ufs,
    fonte= fonte
    )


def extrair_empresas_bd(anos: list, main_dir: str = None, ufs: list = [], limit: str = ""):
    # anos_sql = ", ".join(f"{ano}" for ano in anos)

    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/e43f0d5b-43cf-4bfb-8d90-c38a4e0d7c4f?table=3dbb38d1-65af-44a3-b43a-7b088891ebc0
    query_empresas = """
            WITH 
            dicionario_porte AS (
                SELECT
                    chave AS chave_porte,
                    valor AS descricao_porte
                FROM `basedosdados.br_me_cnpj.dicionario`
                WHERE
                    TRUE
                    AND nome_coluna = 'porte'
                    AND id_tabela = 'empresas'
            )
            SELECT
                dados.data as data,
                dados.cnpj_basico as cnpj_basico,
                diretorio_natureza_juridica.descricao AS natureza_juridica_descricao,
                dados.capital_social as capital_social,
                descricao_porte AS porte
            FROM `basedosdados.br_me_cnpj.empresas` AS dados
            LEFT JOIN (SELECT DISTINCT id_natureza_juridica,descricao  FROM `basedosdados.br_bd_diretorios_brasil.natureza_juridica`) AS diretorio_natureza_juridica
                ON dados.natureza_juridica = diretorio_natureza_juridica.id_natureza_juridica
            LEFT JOIN `dicionario_porte`
                ON dados.porte = chave_porte

            WHERE 
                EXTRACT(YEAR FROM data) = {ano}          

            """

    processamento_empresas = extrair_dados_sql(
    table_name= "cnpj_empresas",
    anos=anos,
    query_base=query_empresas,
    main_dir=main_dir,
    ufs=ufs,
    limit=limit
    )


def extrair_empresas_url(cidades: list, processadores: str, main_dir: str = None, ufs: list = [], anos = []):
    if processadores == 'um':
        threads = 'one'
    elif processadores == 'dois':
        threads = 'dual'
    elif processadores == 'multi':
        threads = 'multi'
    else:
        raise ValueError(f"Tipo de processador '{processadores}' não é válido. Escolha entre 'um', 'dois', ou 'multi'.")

    method = eval(f'{threads}_thread') # Opções: one_thread, dual_thread, multi_thread
    table_name= "cnpj_empresas" # placeholder com nome da tabela
    
    processamento_empresas = baixar_e_processar_cnpjs(
    url_template= param.url_cnpjs(),
    main_dir=main_dir,
    method=method,
    anos= anos,
    cidades= cidades,
    ufs= ufs
    )






def extrair_cnpjs_bd(anos: list, fonte = 'Joined', main_dir: str = None, ufs: list = [], limit: str = ""):
    uf_sql = ", ".join(f"'{uf}'" for uf in ufs)

    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/e43f0d5b-43cf-4bfb-8d90-c38a4e0d7c4f?table=b8432ff5-06c8-45ca-b8b6-33fceb24089d
    #                                joined com https://basedosdados.org/dataset/e43f0d5b-43cf-4bfb-8d90-c38a4e0d7c4f?table=3dbb38d1-65af-44a3-b43a-7b088891ebc0
    
    query_cnpj_joined = '''
                WITH 
                -- Dicionário para a tabela de empresas
                dicionario_qualificacao_responsavel AS (
                    SELECT
                        chave AS chave_qualificacao_responsavel,
                        valor AS descricao_qualificacao_responsavel
                    FROM `basedosdados.br_me_cnpj.dicionario`
                    WHERE
                        nome_coluna = 'qualificacao_responsavel'
                        AND id_tabela = 'empresas'
                ),
                dicionario_porte AS (
                    SELECT
                        chave AS chave_porte,
                        valor AS descricao_porte
                    FROM `basedosdados.br_me_cnpj.dicionario`
                    WHERE
                        nome_coluna = 'porte'
                        AND id_tabela = 'empresas'
                ),

                -- Seleção de dados da tabela de empresas
                empresas AS (
                    SELECT
                        dados.data as data,
                        dados.cnpj_basico as cnpj_basico,
                        dados.razao_social as razao_social,
                        dados.natureza_juridica AS natureza_juridica,
                        diretorio_natureza_juridica.descricao AS natureza_juridica_descricao,
                        descricao_qualificacao_responsavel AS qualificacao_responsavel,
                        dados.capital_social as capital_social,
                        descricao_porte AS porte,
                        dados.ente_federativo as ente_federativo
                    FROM `basedosdados.br_me_cnpj.empresas` AS dados
                    LEFT JOIN (SELECT DISTINCT id_natureza_juridica, descricao 
                            FROM `basedosdados.br_bd_diretorios_brasil.natureza_juridica`) AS diretorio_natureza_juridica
                        ON dados.natureza_juridica = diretorio_natureza_juridica.id_natureza_juridica
                    LEFT JOIN dicionario_qualificacao_responsavel
                        ON dados.qualificacao_responsavel = chave_qualificacao_responsavel
                    LEFT JOIN dicionario_porte
                        ON dados.porte = chave_porte
                    WHERE EXTRACT(YEAR FROM data) = {ano} 
                ),

                -- Dicionário para a tabela de estabelecimentos
                dicionario_identificador_matriz_filial AS (
                    SELECT
                        chave AS chave_identificador_matriz_filial,
                        valor AS descricao_identificador_matriz_filial
                    FROM `basedosdados.br_me_cnpj.dicionario`
                    WHERE
                        nome_coluna = 'identificador_matriz_filial'
                        AND id_tabela = 'estabelecimentos'
                ),
                dicionario_situacao_cadastral AS (
                    SELECT
                        chave AS chave_situacao_cadastral,
                        valor AS descricao_situacao_cadastral
                    FROM `basedosdados.br_me_cnpj.dicionario`
                    WHERE
                        nome_coluna = 'situacao_cadastral'
                        AND id_tabela = 'estabelecimentos'
                ),
                dicionario_id_pais AS (
                    SELECT
                        chave AS chave_id_pais,
                        valor AS descricao_id_pais
                    FROM `basedosdados.br_me_cnpj.dicionario`
                    WHERE
                        nome_coluna = 'id_pais'
                        AND id_tabela = 'estabelecimentos'
                ),

                -- Seleção de dados da tabela de estabelecimentos para o ES
                estabelecimentos_es AS (
                    SELECT
                        dados.data as data,
                        dados.cnpj as cnpj,
                        dados.cnpj_basico as cnpj_basico,
                        dados.cnpj_ordem as cnpj_ordem,
                        dados.cnpj_dv as cnpj_dv,
                        descricao_identificador_matriz_filial AS identificador_matriz_filial,
                        dados.nome_fantasia as nome_fantasia,
                        descricao_situacao_cadastral AS situacao_cadastral,
                        dados.data_situacao_cadastral as data_situacao_cadastral,
                        dados.motivo_situacao_cadastral as motivo_situacao_cadastral,
                        dados.nome_cidade_exterior as nome_cidade_exterior,
                        descricao_id_pais AS id_pais,
                        dados.data_inicio_atividade as data_inicio_atividade,
                        dados.cnae_fiscal_principal as cnae_fiscal_principal,
                        dados.cnae_fiscal_secundaria as cnae_fiscal_secundaria,
                        dados.sigla_uf AS sigla_uf,
                        diretorio_sigla_uf.nome AS sigla_uf_nome,
                        dados.id_municipio AS id_municipio,
                        diretorio_id_municipio.nome AS id_municipio_nome,
                        dados.id_municipio_rf as id_municipio_rf,
                        dados.tipo_logradouro as tipo_logradouro,
                        dados.logradouro as logradouro,
                        dados.numero as numero,
                        dados.complemento as complemento,
                        dados.bairro as bairro,
                        dados.cep as cep,
                        dados.ddd_1 as ddd_1,
                        dados.telefone_1 as telefone_1,
                        dados.ddd_2 as ddd_2,
                        dados.telefone_2 as telefone_2,
                        dados.ddd_fax as ddd_fax,
                        dados.fax as fax,
                        dados.email as email,
                        dados.situacao_especial as situacao_especial,
                        dados.data_situacao_especial as data_situacao_especial
                    FROM `basedosdados.br_me_cnpj.estabelecimentos` AS dados
                    LEFT JOIN dicionario_identificador_matriz_filial
                        ON dados.identificador_matriz_filial = chave_identificador_matriz_filial
                    LEFT JOIN dicionario_situacao_cadastral
                        ON dados.situacao_cadastral = chave_situacao_cadastral
                    LEFT JOIN dicionario_id_pais
                        ON dados.id_pais = chave_id_pais
                    LEFT JOIN (SELECT DISTINCT sigla, nome 
                            FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_sigla_uf
                        ON dados.sigla_uf = diretorio_sigla_uf.sigla
                    LEFT JOIN (SELECT DISTINCT id_municipio, nome 
                            FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
                        ON dados.id_municipio = diretorio_id_municipio.id_municipio
                    WHERE EXTRACT(YEAR FROM data) = {ano} 

                    ''' + f'''
                    AND dados.sigla_uf IN ({uf_sql}) 
                )

                -- Join entre as tabelas de empresas e estabelecimentos
                SELECT 
                    est.data AS data,
                    e.cnpj_basico,
                    e.razao_social,
                    e.natureza_juridica,
                    e.natureza_juridica_descricao,
                    e.qualificacao_responsavel,
                    e.capital_social,
                    e.porte,
                    e.ente_federativo,
                    est.cnpj,
                    est.cnpj_ordem,
                    est.cnpj_dv,
                    est.identificador_matriz_filial,
                    est.nome_fantasia,
                    est.situacao_cadastral,
                    est.data_situacao_cadastral,
                    est.motivo_situacao_cadastral,
                    est.nome_cidade_exterior,
                    est.id_pais,
                    est.data_inicio_atividade,
                    est.cnae_fiscal_principal,
                    est.cnae_fiscal_secundaria,
                    est.sigla_uf,
                    est.sigla_uf_nome,
                    est.id_municipio,
                    est.id_municipio_nome,
                    est.id_municipio_rf,
                    est.tipo_logradouro,
                    est.logradouro,
                    est.numero,
                    est.complemento,
                    est.bairro,
                    est.cep,
                    est.ddd_1,
                    est.telefone_1,
                    est.ddd_2,
                    est.telefone_2,
                    est.ddd_fax,
                    est.fax,
                    est.email,
                    est.situacao_especial,
                    est.data_situacao_especial
                FROM empresas AS e
                RIGHT JOIN estabelecimentos_es AS est
                    ON e.cnpj_basico = est.cnpj_basico
                    AND e.data = est.data

'''


    processamento_cnpj_joined = extrair_dados_sql(
    table_name= "cnpj_joined",
    anos=anos,
    query_base=query_cnpj_joined,
    main_dir=main_dir,
    ufs=ufs,
    limit=limit
    )
    process = CnpjProcess()
    for ano in anos:
        
        process.cnpj_treat(main_dir=main_dir, ano = ano, fonte=fonte) # não recebe o parâmetro meses, pois o parâmetro é para extração via url


