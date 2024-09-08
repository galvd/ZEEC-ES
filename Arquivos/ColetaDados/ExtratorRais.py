from __future__ import annotations
import json
import sys
with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

from Arquivos.ColetaDados.Extrator import extrair_dados_sql



def extrair_rais(anos: list, cidades: list, save_dir: str = None, ufs: str = "", limit: str = ""):

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

    
    processamento_rais = extrair_dados_sql(
    table_name= "rais",
    anos=anos,
    cidades=cidades,
    query_base=query_rais,
    save_dir=save_dir,
    ufs=ufs,
    limit=limit
    )

    return processamento_rais







