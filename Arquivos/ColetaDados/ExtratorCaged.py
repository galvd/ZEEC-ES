"""
ExtratorCaged.py
Este script lida com a extração e processamento de dados relacionados ao CAGED (Cadastro Geral de Empregados e Desempregados), fornecendo informações sobre movimentações de emprego formal. Ele usa a Base dos Dados como fonte de dados.

Funções principais:
- extrair_dados_caged:
    Descrição: Realiza consultas SQL para obter informações sobre movimentações de emprego a partir do CAGED, filtradas por ano e município.
    Parâmetros:
    ano: O ano de interesse.
    municipios: Lista de códigos IBGE dos municípios de interesse.
    Retorno: DataFrame contendo os dados do CAGED extraídos da Base dos Dados.

- filtrar_dados_caged:
    Descrição: Aplica filtros sobre os dados do CAGED para selecionar setores econômicos ou tipos de movimentação (admissões/demissões).
    Parâmetros:
    df: DataFrame contendo os dados do CAGED.
    setores: Lista de setores a serem filtrados.
    tipos_movimentacao: Lista de tipos de movimentação a serem filtrados.
    Retorno: Um DataFrame com os dados filtrados de acordo com os setores e tipos de movimentação.

- salvar_dados_caged:
    Descrição: Salva os dados do CAGED processados em formato Parquet, organizados por município e ano.
    Parâmetros:
    df: DataFrame com os dados tratados.
    diretorio: Caminho para o diretório onde os arquivos serão armazenados.
    Retorno: Nenhum. Os arquivos são salvos diretamente no disco.

Fluxo Geral:
    Extrai os dados do CAGED para um conjunto específico de municípios e anos.
    Aplica filtros para selecionar setores econômicos ou tipos de movimentação.
    Salva os dados filtrados no formato Parquet, organizando os arquivos conforme necessário.
    Esses arquivos seguem um padrão similar, aproveitando a Base dos Dados como fonte de dados e realizando transformações específicas, filtragens e salvamentos no formato Parquet para análises posteriores.
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


def extrair_caged(anos: list, cidades: list, main_dir: str = None, ufs: str = "", mes: int = None, limit: str = ""):

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
        ),
        dicionario_origem_informacao AS (
            SELECT
                chave AS chave_origem_informacao,
                valor AS descricao_origem_informacao
            FROM `basedosdados.br_me_caged.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'origem_informacao'
                AND id_tabela = 'microdados_movimentacao'
        )
        SELECT
            dados.ano as ano,
            dados.mes as mes,
            dados.sigla_uf AS sigla_uf,
            diretorio_sigla_uf.nome AS sigla_uf_nome,
            dados.id_municipio AS id_municipio,
            diretorio_id_municipio.nome AS id_municipio_nome,
            dados.cnae_2_secao as cnae_2_secao,
            dados.cnae_2_subclasse as cnae_2_subclasse,
            dados.saldo_movimentacao as saldo_movimentacao,
            descricao_categoria AS categoria,
            descricao_grau_instrucao AS grau_instrucao,
            dados.idade as idade,
            dados.horas_contratuais as horas_contratuais,
            descricao_raca_cor AS raca_cor,
            descricao_sexo AS sexo,
            descricao_tipo_empregador AS tipo_empregador,
            descricao_tipo_estabelecimento AS tipo_estabelecimento,
            descricao_tipo_movimentacao AS tipo_movimentacao,
            descricao_tipo_deficiencia AS tipo_deficiencia,
            descricao_indicador_trabalho_intermitente AS indicador_trabalho_intermitente,
            descricao_indicador_trabalho_parcial AS indicador_trabalho_parcial,
            dados.salario_mensal as salario_mensal,
            dados.tamanho_estabelecimento_janeiro as tamanho_estabelecimento_janeiro,
            descricao_indicador_aprendiz AS indicador_aprendiz,
            descricao_origem_informacao AS origem_informacao,
            dados.indicador_fora_prazo as indicador_fora_prazo
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
        LEFT JOIN `dicionario_origem_informacao`
            ON dados.origem_informacao = chave_origem_informacao

        WHERE 
                ano = {ano} 
                             
            """
    # query_caged+= f"""AND (REGEXP_CONTAINS(dados.cnae_1, r'^({cnae_sql})') 
    #                     OR REGEXP_CONTAINS(dados.cnae_2, r'^({cnae_sql})'))\n"""

    processamento_caged = extrair_dados_sql(
    table_name= "caged",
    anos=anos,
    cidades=cidades,
    query_base=query_caged,
    main_dir=main_dir,
    ufs=ufs,
    mes = mes,
    limit=limit
    )




def extrair_caged_es(anos: list, main_dir: str = None, ufs: str = "", mes: int = None, limit: str = ""):

    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/562b56a3-0b01-4735-a049-eeac5681f056?table=95106d6f-e36e-4fed-b8e9-99c41cd99ecf
    query_caged = """
                WITH
    dicionario_tipo_movimentacao AS (
        SELECT
            chave AS chave_tipo_movimentacao,
            valor AS descricao_tipo_movimentacao
        FROM `basedosdados.br_me_caged.dicionario`
        WHERE
            nome_coluna = 'tipo_movimentacao'
            AND id_tabela = 'microdados_movimentacao'
    ),
    salario_stats AS (
        SELECT
            APPROX_QUANTILES(dados.salario_mensal, 100) AS quantis
        FROM `basedosdados.br_me_caged.microdados_movimentacao` AS dados
    )
SELECT
    dados.ano AS ano,
    dados.mes AS mes,
    
    -- Categoriza os tipos de movimentação
    CASE
        WHEN descricao_tipo_movimentacao IN (
            'Culpa Recíproca',
            'Desligamento Por Término De Contrato',
            'Desligamento Por Demissão Com Justa Causa',
            'Desligamento A Pedido',
            'Desligamento De Tipo Ignorado',
            'Desligamento Por Demissão Sem Justa Causa',
            'Término Contrato Trabalho Prazo Determinado',
            'Desligamento Por Acordo Entre Empregado E Empregador',
            'Desligamento Por Morte',
            'Desligamento Por Aposentadoria'
        ) THEN 'Desligamento'
        
        WHEN descricao_tipo_movimentacao IN (
            'Admissão Por Reemprego',
            'Admissão Por Contrato Trabalho Prazo Determinado',
            'Admissão Por Primeiro Emprego',
            'Admissão Por Reintegração'
        ) THEN 'Admissão'
        
        ELSE 'Outros'
    END AS tipo_movimentacao,

    -- Cálculo dos percentis
    APPROX_QUANTILES(dados.salario_mensal, 100)[OFFSET(10)] AS p10,
    APPROX_QUANTILES(dados.salario_mensal, 100)[OFFSET(20)] AS p20,
    APPROX_QUANTILES(dados.salario_mensal, 100)[OFFSET(50)] AS p50,
    APPROX_QUANTILES(dados.salario_mensal, 100)[OFFSET(80)] AS p80,
    APPROX_QUANTILES(dados.salario_mensal, 100)[OFFSET(90)] AS p90,
    APPROX_QUANTILES(dados.salario_mensal, 100)[OFFSET(99)] AS p99,

    -- Média das rendas para admissões e desligamentos
    AVG(CASE WHEN descricao_tipo_movimentacao IN (
        'Admissão Por Reemprego',
        'Admissão Por Contrato Trabalho Prazo Determinado',
        'Admissão Por Primeiro Emprego',
        'Admissão Por Reintegração'
    ) THEN dados.salario_mensal END) AS media_admissao,

    AVG(CASE WHEN descricao_tipo_movimentacao IN (
        'Culpa Recíproca',
        'Desligamento Por Término De Contrato',
        'Desligamento Por Demissão Com Justa Causa',
        'Desligamento A Pedido',
        'Desligamento De Tipo Ignorado',
        'Desligamento Por Demissão Sem Justa Causa',
        'Término Contrato Trabalho Prazo Determinado',
        'Desligamento Por Acordo Entre Empregado E Empregador',
        'Desligamento Por Morte',
        'Desligamento Por Aposentadoria'
    ) THEN dados.salario_mensal END) AS media_desligamento,

    -- Nova coluna que agrega as duas médias
    COALESCE(
        AVG(CASE WHEN descricao_tipo_movimentacao IN (
            'Admissão Por Reemprego',
            'Admissão Por Contrato Trabalho Prazo Determinado',
            'Admissão Por Primeiro Emprego',
            'Admissão Por Reintegração'
        ) THEN dados.salario_mensal END),
        AVG(CASE WHEN descricao_tipo_movimentacao IN (
            'Culpa Recíproca',
            'Desligamento Por Término De Contrato',
            'Desligamento Por Demissão Com Justa Causa',
            'Desligamento A Pedido',
            'Desligamento De Tipo Ignorado',
            'Desligamento Por Demissão Sem Justa Causa',
            'Término Contrato Trabalho Prazo Determinado',
            'Desligamento Por Acordo Entre Empregado E Empregador',
            'Desligamento Por Morte',
            'Desligamento Por Aposentadoria'
        ) THEN dados.salario_mensal END)
    ) AS media_agregada

FROM `basedosdados.br_me_caged.microdados_movimentacao` AS dados
LEFT JOIN dicionario_tipo_movimentacao
    ON dados.tipo_movimentacao = chave_tipo_movimentacao

-- Filtra os dados para remover outliers
WHERE dados.salario_mensal >= (SELECT quantis[OFFSET(1)] FROM salario_stats) -- 0.5%
  AND dados.salario_mensal <= (SELECT quantis[OFFSET(98)] FROM salario_stats) -- 99.5%

GROUP BY
    dados.ano, dados.mes, tipo_movimentacao
ORDER BY
    dados.ano, dados.mes, tipo_movimentacao;

    
            """

    processamento_caged_es = extrair_dados_sql(
    table_name= "caged_ES",
    query_base=query_caged,
    main_dir=main_dir,
    ufs= ufs,
    limit=limit
    )