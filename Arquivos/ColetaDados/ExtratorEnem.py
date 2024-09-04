from __future__ import annotations
import json
import sys
with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

from Arquivos.ColetaDados.Extrator import extrair_dados





cloud_id = config['cloud_id']

def extrair_enem(anos: list, cidades: list, save_dir: str = None, ufs: str = "", limit: str = ""):

    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/3e9c8804-c31c-4f48-9a45-d67f1c21a859?table=9a9ad3b2-c21e-4cdb-8523-fda5a44abe29
    query_rais = """
WITH 
dicionario_faixa_etaria AS (
    SELECT
        chave AS chave_faixa_etaria,
        valor AS descricao_faixa_etaria
    FROM `basedosdados.br_inep_enem.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'faixa_etaria'
        AND id_tabela = 'microdados'
),
dicionario_sexo AS (
    SELECT
        chave AS chave_sexo,
        valor AS descricao_sexo
    FROM `basedosdados.br_inep_enem.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'sexo'
        AND id_tabela = 'microdados'
),
dicionario_cor_raca AS (
    SELECT
        chave AS chave_cor_raca,
        valor AS descricao_cor_raca
    FROM `basedosdados.br_inep_enem.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'cor_raca'
        AND id_tabela = 'microdados'
),
dicionario_situacao_conclusao AS (
    SELECT
        chave AS chave_situacao_conclusao,
        valor AS descricao_situacao_conclusao
    FROM `basedosdados.br_inep_enem.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'situacao_conclusao'
        AND id_tabela = 'microdados'
),
dicionario_tipo_escola AS (
    SELECT
        chave AS chave_tipo_escola,
        valor AS descricao_tipo_escola
    FROM `basedosdados.br_inep_enem.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'tipo_escola'
        AND id_tabela = 'microdados'
),
dicionario_ensino AS (
    SELECT
        chave AS chave_ensino,
        valor AS descricao_ensino
    FROM `basedosdados.br_inep_enem.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'ensino'
        AND id_tabela = 'microdados'
),
dicionario_dependencia_administrativa_escola AS (
    SELECT
        chave AS chave_dependencia_administrativa_escola,
        valor AS descricao_dependencia_administrativa_escola
    FROM `basedosdados.br_inep_enem.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'dependencia_administrativa_escola'
        AND id_tabela = 'microdados'
),
dicionario_localizacao_escola AS (
    SELECT
        chave AS chave_localizacao_escola,
        valor AS descricao_localizacao_escola
    FROM `basedosdados.br_inep_enem.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'localizacao_escola'
        AND id_tabela = 'microdados'
),
dicionario_situacao_funcionamento_escola AS (
    SELECT
        chave AS chave_situacao_funcionamento_escola,
        valor AS descricao_situacao_funcionamento_escola
    FROM `basedosdados.br_inep_enem.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'situacao_funcionamento_escola'
        AND id_tabela = 'microdados'
),
dicionario_presenca_objetiva AS (
    SELECT
        chave AS chave_presenca_objetiva,
        valor AS descricao_presenca_objetiva
    FROM `basedosdados.br_inep_enem.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'presenca_objetiva'
        AND id_tabela = 'microdados'
),
dicionario_tipo_prova_objetiva AS (
    SELECT
        chave AS chave_tipo_prova_objetiva,
        valor AS descricao_tipo_prova_objetiva
    FROM `basedosdados.br_inep_enem.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'tipo_prova_objetiva'
        AND id_tabela = 'microdados'
),
dicionario_presenca_ciencias_natureza AS (
    SELECT
        chave AS chave_presenca_ciencias_natureza,
        valor AS descricao_presenca_ciencias_natureza
    FROM `basedosdados.br_inep_enem.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'presenca_ciencias_natureza'
        AND id_tabela = 'microdados'
),
dicionario_presenca_ciencias_humanas AS (
    SELECT
        chave AS chave_presenca_ciencias_humanas,
        valor AS descricao_presenca_ciencias_humanas
    FROM `basedosdados.br_inep_enem.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'presenca_ciencias_humanas'
        AND id_tabela = 'microdados'
),
dicionario_presenca_linguagens_codigos AS (
    SELECT
        chave AS chave_presenca_linguagens_codigos,
        valor AS descricao_presenca_linguagens_codigos
    FROM `basedosdados.br_inep_enem.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'presenca_linguagens_codigos'
        AND id_tabela = 'microdados'
),
dicionario_presenca_matematica AS (
    SELECT
        chave AS chave_presenca_matematica,
        valor AS descricao_presenca_matematica
    FROM `basedosdados.br_inep_enem.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'presenca_matematica'
        AND id_tabela = 'microdados'
),
dicionario_lingua_estrangeira AS (
    SELECT
        chave AS chave_lingua_estrangeira,
        valor AS descricao_lingua_estrangeira
    FROM `basedosdados.br_inep_enem.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'lingua_estrangeira'
        AND id_tabela = 'microdados'
),
dicionario_presenca_redacao AS (
    SELECT
        chave AS chave_presenca_redacao,
        valor AS descricao_presenca_redacao
    FROM `basedosdados.br_inep_enem.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'presenca_redacao'
        AND id_tabela = 'microdados'
)
SELECT
    ano,
    id_inscricao,
    descricao_faixa_etaria AS faixa_etaria,
    descricao_sexo AS sexo,
    dados.id_municipio_residencia AS id_municipio_residencia,
    diretorio_id_municipio_residencia.nome AS id_municipio_residencia_nome,
    dados.sigla_uf_residencia AS sigla_uf_residencia,
    diretorio_sigla_uf_residencia.nome AS sigla_uf_residencia_nome,
    descricao_cor_raca AS cor_raca,
    descricao_situacao_conclusao AS situacao_conclusao,
    ano_conclusao,
    descricao_tipo_escola AS tipo_escola,
    descricao_ensino AS ensino,
    indicador_treineiro,
    dados.id_municipio_escola AS id_municipio_escola,
    diretorio_id_municipio_escola.nome AS id_municipio_escola_nome,
    dados.sigla_uf_escola AS sigla_uf_escola,
    diretorio_sigla_uf_escola.nome AS sigla_uf_escola_nome,
    descricao_dependencia_administrativa_escola AS dependencia_administrativa_escola,
    descricao_localizacao_escola AS localizacao_escola,
    descricao_situacao_funcionamento_escola AS situacao_funcionamento_escola,
    indicador_certificado,
    nome_certificadora,
    dados.sigla_uf_certificadora AS sigla_uf_certificadora,
    diretorio_sigla_uf_certificadora.nome AS sigla_uf_certificadora_nome,
    dados.id_municipio_prova AS id_municipio_prova,
    diretorio_id_municipio_prova.nome AS id_municipio_prova_nome,
    dados.sigla_uf_prova AS sigla_uf_prova,
    diretorio_sigla_uf_prova.nome AS sigla_uf_prova_nome,
    descricao_presenca_objetiva AS presenca_objetiva,
    descricao_tipo_prova_objetiva AS tipo_prova_objetiva,
    nota_objetiva_competencia_1,
    nota_objetiva_competencia_2,
    nota_objetiva_competencia_3,
    nota_objetiva_competencia_4,
    nota_objetiva_competencia_5,
    nota_objetiva,
    descricao_presenca_ciencias_natureza AS presenca_ciencias_natureza,
    descricao_presenca_ciencias_humanas AS presenca_ciencias_humanas,
    descricao_presenca_linguagens_codigos AS presenca_linguagens_codigos,
    descricao_presenca_matematica AS presenca_matematica,
    nota_ciencias_natureza,
    nota_ciencias_humanas,
    nota_linguagens_codigos,
    nota_matematica,
    descricao_lingua_estrangeira AS lingua_estrangeira,
    descricao_presenca_redacao AS presenca_redacao,
    nota_redacao_competencia_1,
    nota_redacao_competencia_2,
    nota_redacao_competencia_3,
    nota_redacao_competencia_4,
    nota_redacao_competencia_5,
    nota_redacao,
    indicador_questionario_socioeconomico
FROM `basedosdados.br_inep_enem.microdados` AS dados
LEFT JOIN `dicionario_faixa_etaria`
    ON dados.faixa_etaria = chave_faixa_etaria
LEFT JOIN `dicionario_sexo`
    ON dados.sexo = chave_sexo
LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio_residencia
    ON dados.id_municipio_residencia = diretorio_id_municipio_residencia.id_municipio
LEFT JOIN (SELECT DISTINCT sigla,nome  FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_sigla_uf_residencia
    ON dados.sigla_uf_residencia = diretorio_sigla_uf_residencia.sigla
LEFT JOIN `dicionario_cor_raca`
    ON dados.cor_raca = chave_cor_raca
LEFT JOIN `dicionario_situacao_conclusao`
    ON dados.situacao_conclusao = chave_situacao_conclusao
LEFT JOIN `dicionario_tipo_escola`
    ON dados.tipo_escola = chave_tipo_escola
LEFT JOIN `dicionario_ensino`
    ON dados.ensino = chave_ensino
LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio_escola
    ON dados.id_municipio_escola = diretorio_id_municipio_escola.id_municipio
LEFT JOIN (SELECT DISTINCT sigla,nome  FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_sigla_uf_escola
    ON dados.sigla_uf_escola = diretorio_sigla_uf_escola.sigla
LEFT JOIN `dicionario_dependencia_administrativa_escola`
    ON dados.dependencia_administrativa_escola = chave_dependencia_administrativa_escola
LEFT JOIN `dicionario_localizacao_escola`
    ON dados.localizacao_escola = chave_localizacao_escola
LEFT JOIN `dicionario_situacao_funcionamento_escola`
    ON dados.situacao_funcionamento_escola = chave_situacao_funcionamento_escola
LEFT JOIN (SELECT DISTINCT sigla,nome  FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_sigla_uf_certificadora
    ON dados.sigla_uf_certificadora = diretorio_sigla_uf_certificadora.sigla
LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio_prova
    ON dados.id_municipio_prova = diretorio_id_municipio_prova.id_municipio
LEFT JOIN (SELECT DISTINCT sigla,nome  FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_sigla_uf_prova
    ON dados.sigla_uf_prova = diretorio_sigla_uf_prova.sigla
LEFT JOIN `dicionario_presenca_objetiva`
    ON dados.presenca_objetiva = chave_presenca_objetiva
LEFT JOIN `dicionario_tipo_prova_objetiva`
    ON dados.tipo_prova_objetiva = chave_tipo_prova_objetiva
LEFT JOIN `dicionario_presenca_ciencias_natureza`
    ON dados.presenca_ciencias_natureza = chave_presenca_ciencias_natureza
LEFT JOIN `dicionario_presenca_ciencias_humanas`
    ON dados.presenca_ciencias_humanas = chave_presenca_ciencias_humanas
LEFT JOIN `dicionario_presenca_linguagens_codigos`
    ON dados.presenca_linguagens_codigos = chave_presenca_linguagens_codigos
LEFT JOIN `dicionario_presenca_matematica`
    ON dados.presenca_matematica = chave_presenca_matematica
LEFT JOIN `dicionario_lingua_estrangeira`
    ON dados.lingua_estrangeira = chave_lingua_estrangeira
LEFT JOIN `dicionario_presenca_redacao`
    ON dados.presenca_redacao = chave_presenca_redacao
    
    WHERE 
        ano = {ano}
        AND id_municipio_escola IN ({cidades})
        AND sigla_uf_escola IN ({ufs})
    """

    
    processamento_enem = extrair_dados(
    table_name= "enem",
    anos=anos,
    cidades=cidades,
    query_base=query_rais,
    save_dir=save_dir,
    ufs=ufs,
    limit=limit
    )

    return processamento_enem


