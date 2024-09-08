from __future__ import annotations
import json
import sys
with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

from Arquivos.ColetaDados.Extrator import extrair_dados_sql



def extrair_caged(anos: list, cidades: list, save_dir: str = None, ufs: str = "", mes: int = None, limit: str = ""):

    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/562b56a3-0b01-4735-a049-eeac5681f056?table=95106d6f-e36e-4fed-b8e9-99c41cd99ecf
    query_caged = """
        WITH 
        dicionario_categoria AS (
            SELECT
                chave AS chave_categoria,
                valor AS descricao_categoria
            FROM `basedosdados.br_me_caged.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'categoria'
                AND id_tabela = 'microdados_movimentacao'
        ),
        dicionario_grau_instrucao AS (
            SELECT
                chave AS chave_grau_instrucao,
                valor AS descricao_grau_instrucao
            FROM `basedosdados.br_me_caged.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'grau_instrucao'
                AND id_tabela = 'microdados_movimentacao'
        ),
        dicionario_raca_cor AS (
            SELECT
                chave AS chave_raca_cor,
                valor AS descricao_raca_cor
            FROM `basedosdados.br_me_caged.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'raca_cor'
                AND id_tabela = 'microdados_movimentacao'
        ),
        dicionario_sexo AS (
            SELECT
                chave AS chave_sexo,
                valor AS descricao_sexo
            FROM `basedosdados.br_me_caged.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'sexo'
                AND id_tabela = 'microdados_movimentacao'
        ),
        dicionario_tipo_empregador AS (
            SELECT
                chave AS chave_tipo_empregador,
                valor AS descricao_tipo_empregador
            FROM `basedosdados.br_me_caged.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'tipo_empregador'
                AND id_tabela = 'microdados_movimentacao'
        ),
        dicionario_tipo_estabelecimento AS (
            SELECT
                chave AS chave_tipo_estabelecimento,
                valor AS descricao_tipo_estabelecimento
            FROM `basedosdados.br_me_caged.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'tipo_estabelecimento'
                AND id_tabela = 'microdados_movimentacao'
        ),
        dicionario_tipo_movimentacao AS (
            SELECT
                chave AS chave_tipo_movimentacao,
                valor AS descricao_tipo_movimentacao
            FROM `basedosdados.br_me_caged.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'tipo_movimentacao'
                AND id_tabela = 'microdados_movimentacao'
        ),
        dicionario_tipo_deficiencia AS (
            SELECT
                chave AS chave_tipo_deficiencia,
                valor AS descricao_tipo_deficiencia
            FROM `basedosdados.br_me_caged.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'tipo_deficiencia'
                AND id_tabela = 'microdados_movimentacao'
        ),
        dicionario_indicador_trabalho_intermitente AS (
            SELECT
                chave AS chave_indicador_trabalho_intermitente,
                valor AS descricao_indicador_trabalho_intermitente
            FROM `basedosdados.br_me_caged.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'indicador_trabalho_intermitente'
                AND id_tabela = 'microdados_movimentacao'
        ),
        dicionario_indicador_trabalho_parcial AS (
            SELECT
                chave AS chave_indicador_trabalho_parcial,
                valor AS descricao_indicador_trabalho_parcial
            FROM `basedosdados.br_me_caged.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'indicador_trabalho_parcial'
                AND id_tabela = 'microdados_movimentacao'
        ),
        dicionario_indicador_aprendiz AS (
            SELECT
                chave AS chave_indicador_aprendiz,
                valor AS descricao_indicador_aprendiz
            FROM `basedosdados.br_me_caged.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'indicador_aprendiz'
                AND id_tabela = 'microdados_movimentacao'
        )
        SELECT
            ano,
            mes,
            dados.sigla_uf AS sigla_uf,
            diretorio_sigla_uf.nome AS sigla_uf_nome,
            dados.id_municipio AS id_municipio,
            diretorio_id_municipio.nome AS id_municipio_nome,
            cnae_2_secao,
            cnae_2_subclasse,
            saldo_movimentacao,
            dados.cbo_2002 AS cbo_2002,
            diretorio_cbo_2002.descricao AS cbo_2002_descricao,
            diretorio_cbo_2002.descricao_familia AS cbo_2002_descricao_familia,
            diretorio_cbo_2002.descricao_subgrupo AS cbo_2002_descricao_subgrupo,
            diretorio_cbo_2002.descricao_subgrupo_principal AS cbo_2002_descricao_subgrupo_principal,
            diretorio_cbo_2002.descricao_grande_grupo AS cbo_2002_descricao_grande_grupo,
            descricao_categoria AS categoria,
            descricao_grau_instrucao AS grau_instrucao,
            idade,
            horas_contratuais,
            descricao_raca_cor AS raca_cor,
            descricao_sexo AS sexo,
            descricao_tipo_empregador AS tipo_empregador,
            descricao_tipo_estabelecimento AS tipo_estabelecimento,
            descricao_tipo_movimentacao AS tipo_movimentacao,
            descricao_tipo_deficiencia AS tipo_deficiencia,
            descricao_indicador_trabalho_intermitente AS indicador_trabalho_intermitente,
            descricao_indicador_trabalho_parcial AS indicador_trabalho_parcial,
            salario_mensal,
            descricao_indicador_aprendiz AS indicador_aprendiz
        FROM `basedosdados.br_me_caged.microdados_movimentacao` AS dados
        LEFT JOIN (SELECT DISTINCT sigla,nome  FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_sigla_uf
            ON dados.sigla_uf = diretorio_sigla_uf.sigla
        LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio
        LEFT JOIN (SELECT DISTINCT cbo_2002,descricao,descricao_familia,descricao_subgrupo,descricao_subgrupo_principal,descricao_grande_grupo  FROM `basedosdados.br_bd_diretorios_brasil.cbo_2002`) AS diretorio_cbo_2002
            ON dados.cbo_2002 = diretorio_cbo_2002.cbo_2002
        LEFT JOIN `dicionario_categoria`
            ON dados.categoria = chave_categoria
        LEFT JOIN `dicionario_grau_instrucao`
            ON dados.grau_instrucao = chave_grau_instrucao
        LEFT JOIN `dicionario_raca_cor`
            ON dados.raca_cor = chave_raca_cor
        LEFT JOIN `dicionario_sexo`
            ON dados.sexo = chave_sexo
        LEFT JOIN `dicionario_tipo_empregador`
            ON dados.tipo_empregador = chave_tipo_empregador
        LEFT JOIN `dicionario_tipo_estabelecimento`
            ON dados.tipo_estabelecimento = chave_tipo_estabelecimento
        LEFT JOIN `dicionario_tipo_movimentacao`
            ON dados.tipo_movimentacao = chave_tipo_movimentacao
        LEFT JOIN `dicionario_tipo_deficiencia`
            ON dados.tipo_deficiencia = chave_tipo_deficiencia
        LEFT JOIN `dicionario_indicador_trabalho_intermitente`
            ON dados.indicador_trabalho_intermitente = chave_indicador_trabalho_intermitente
        LEFT JOIN `dicionario_indicador_trabalho_parcial`
            ON dados.indicador_trabalho_parcial = chave_indicador_trabalho_parcial
        LEFT JOIN `dicionario_indicador_aprendiz`
            ON dados.indicador_aprendiz = chave_indicador_aprendiz

        WHERE 
                ano = {ano}              
            """

    processamento_caged = extrair_dados_sql(
    table_name= "caged",
    anos=anos,
    cidades=cidades,
    query_base=query_caged,
    save_dir=save_dir,
    ufs=ufs,
    mes = mes,
    limit=limit
    )

    return processamento_caged

