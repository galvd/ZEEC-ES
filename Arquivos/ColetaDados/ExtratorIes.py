from __future__ import annotations
import json
import sys
with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

from Arquivos.ColetaDados.Extrator import extrair_dados_sql



def extrair_ies(anos: list, cidades: list, save_dir: str = None, ufs: str = "", limit: str = ""):

    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/a3b57cca-ff80-4bf2-8bac-c145109e06a7?table=e8ee3373-0f3d-4617-849c-c730970a819e
    query_ies = """
        SELECT
            ano,
            sigla_uf,
            dados.id_municipio AS id_municipio,
            diretorio_id_municipio.nome AS id_municipio_nome,
            tipo_organizacao_academica,
            tipo_categoria_administrativa,
            nome_mantenedora,
            id_mantenedora,
            dados.id_ies AS id_ies,
            diretorio_id_ies.nome AS id_ies_nome,
            diretorio_id_ies.tipo_instituicao AS id_ies_tipo_instituicao,
            dados.nome,
            sigla,
            endereco,
            numero,
            complemento,
            bairro,
            dados.cep AS cep,
            diretorio_cep.logradouro AS cep_logradouro,
            diretorio_cep.localidade AS cep_localidade,
            quantidade_tecnicos,
            quantidade_tecnicos_ef_incompleto_feminino,
            quantidade_tecnicos_ef_incompleto_masculino,
            quantidade_tecnicos_ef_completo_feminino,
            quantidade_tecnicos_ef_completo_masculino,
            quantidade_tecnicos_em_feminino,
            quantidade_tecnicos_em_masculino,
            quantidade_tecnicos_es_feminino,
            quantidade_tecnicos_es_masculino,
            quantidade_tecnicos_especializacao_feminino,
            quantidade_tecnicos_especializacao_masculino,
            quantidade_tecnicos_mestrado_feminino,
            quantidade_tecnicos_mestrado_masculino,
            quantidade_tecnicos_doutorado_feminino,
            quantidade_tecnicos_doutorado_masculino,
            indicador_biblioteca_acesso_portal_capes,
            indicador_biblioteca_acesso_outras_bases,
            indicador_biblioteca_assina_outras_bases,
            indicador_biblioteca_repositorio_institucional,
            indicador_biblioteca_busca_integrada,
            indicador_biblioteca_internet,
            indicador_biblioteca_rede_social,
            indicador_biblioteca_catalogo_online,
            quantidade_biblioteca_periodicos_eletronicos,
            quantidade_biblioteca_livros_eletronicos,
            quantidade_docentes,
            quantidade_docentes_exercicio,
            quantidade_docentes_exercicio_feminino,
            quantidade_docentes_exercicio_masculino,
            quantidade_docentes_exercicio_sem_graduacao,
            quantidade_docentes_exercicio_graduacao,
            quantidade_docentes_exercicio_especializacao,
            quantidade_docentes_exercicio_mestrado,
            quantidade_docentes_exercicio_doutorado,
            quantidade_docentes_exercicio_integral,
            quantidade_docentes_exercicio_integral_dedicacao_exclusiva,
            quantidade_docentes_exercicio_integral_sem_dedicacao_exclusiva,
            quantidade_docentes_exercicio_parcial,
            quantidade_docentes_exercicio_horista,
            quantidade_docentes_exercicio_0_29,
            quantidade_docentes_exercicio_30_34,
            quantidade_docentes_exercicio_35_39,
            quantidade_docentes_exercicio_40_44,
            quantidade_docentes_exercicio_45_49,
            quantidade_docentes_exercicio_50_54,
            quantidade_docentes_exercicio_55_59,
            quantidade_docentes_exercicio_60_mais,
            quantidade_docentes_exercicio_branca,
            quantidade_docentes_exercicio_preta,
            quantidade_docentes_exercicio_parda,
            quantidade_docentes_exercicio_amarela,
            quantidade_docentes_exercicio_indigena,
            quantidade_docentes_exercicio_cor_nao_declarada,
            quantidade_docentes_exercicio_brasileiro,
            quantidade_docentes_exercicio_estrangeiro,
            quantidade_docentes_exercicio_deficiencia
        FROM `basedosdados.br_inep_censo_educacao_superior.ies` AS dados
        LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio
        LEFT JOIN (SELECT DISTINCT id_ies,nome,tipo_instituicao  FROM `basedosdados.br_bd_diretorios_brasil.instituicao_ensino_superior`) AS diretorio_id_ies
            ON dados.id_ies = diretorio_id_ies.id_ies
        LEFT JOIN (SELECT DISTINCT cep,logradouro,localidade  FROM `basedosdados.br_bd_diretorios_brasil.cep`) AS diretorio_cep
            ON dados.cep = diretorio_cep.cep
                

        WHERE 
                ano = {ano}              
            """

    processamento_ies = extrair_dados_sql(
    table_name= "ies",
    anos=anos,
    cidades=cidades,
    query_base=query_ies,
    save_dir=save_dir,
    ufs=ufs,
    limit=limit
    )

    return processamento_ies


def extrair_cursos_sup(anos: list, cidades: list, save_dir: str = None, ufs: str = "", limit: str = ""):

    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/a3b57cca-ff80-4bf2-8bac-c145109e06a7?table=03f7e043-9ea1-47f9-9e77-f55dfe449381
    query_cursos = """
        WITH 
        dicionario_tipo_dimensao AS (
            SELECT
                chave AS chave_tipo_dimensao,
                valor AS descricao_tipo_dimensao
            FROM `basedosdados.br_inep_censo_educacao_superior.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'tipo_dimensao'
                AND id_tabela = 'curso'
        ),
        dicionario_tipo_organizacao_academica AS (
            SELECT
                chave AS chave_tipo_organizacao_academica,
                valor AS descricao_tipo_organizacao_academica
            FROM `basedosdados.br_inep_censo_educacao_superior.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'tipo_organizacao_academica'
                AND id_tabela = 'curso'
        ),
        dicionario_tipo_organizacao_administrativa AS (
            SELECT
                chave AS chave_tipo_organizacao_administrativa,
                valor AS descricao_tipo_organizacao_administrativa
            FROM `basedosdados.br_inep_censo_educacao_superior.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'tipo_organizacao_administrativa'
                AND id_tabela = 'curso'
        ),
        dicionario_rede AS (
            SELECT
                chave AS chave_rede,
                valor AS descricao_rede
            FROM `basedosdados.br_inep_censo_educacao_superior.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'rede'
                AND id_tabela = 'curso'
        )
        SELECT
            ano,
            sigla_uf,
            dados.id_municipio AS id_municipio,
            diretorio_id_municipio.nome AS id_municipio_nome,
            descricao_tipo_dimensao AS tipo_dimensao,
            descricao_tipo_organizacao_academica AS tipo_organizacao_academica,
            descricao_tipo_organizacao_administrativa AS tipo_organizacao_administrativa,
            descricao_rede AS rede,
            dados.id_ies AS id_ies,
            diretorio_id_ies.nome AS id_ies_nome,
            diretorio_id_ies.tipo_instituicao AS id_ies_tipo_instituicao,
            dados.id_curso AS id_curso,
            diretorio_id_curso.nome_curso AS id_curso_nome_curso,
            diretorio_id_curso.nome_area AS id_curso_nome_area,
            diretorio_id_curso.grau_academico AS id_curso_grau_academico,
            nome_curso_cine,
            id_curso_cine,
            id_area_geral,
            nome_area_geral,
            id_area_especifica,
            nome_area_especifica,
            id_area_detalhada,
            nome_area_detalhada,
            tipo_grau_academico,
            indicador_gratuito,
            tipo_modalidade_ensino,
            tipo_nivel_academico,
            quantidade_vagas,
            quantidade_vagas_diurno,
            quantidade_vagas_noturno,
            quantidade_vagas_ead,
            quantidade_vagas_novas,
            quantidade_vagas_processos_seletivos,
            quantidade_vagas_remanescentes,
            quantidade_vagas_programas_especiais,
            quantidade_inscritos,
            quantidade_inscritos_diurno,
            quantidade_inscritos_noturno,
            quantidade_inscritos_ead,
            quantidade_inscritos_vagas_novas,
            quantidade_inscritos_processos_seletivos,
            quantidade_inscritos_remanescentes,
            quantidade_inscritos_programas_especiais,
            quantidade_ingressantes,
            quantidade_ingressantes_feminino,
            quantidade_ingressantes_masculino,
            quantidade_ingressantes_diurno,
            quantidade_ingressantes_noturno,
            quantidade_ingressantes_vagas_novas,
            quantidade_ingressantes_vestibular,
            quantidade_ingressantes_enem,
            quantidade_ingressantes_avaliacao_seriada,
            quantidade_ingressantes_selecao_simplificada,
            quantidade_ingressantes_egressos,
            quantidade_ingressantes_outro_tipo_selecao,
            quantidade_ingressantes_processos_seletivos,
            quantidade_ingressantes_remanescentes,
            quantidade_ingressantes_programas_especiais,
            quantidade_ingressantes_outras_formas,
            quantidade_ingressantes_0_17,
            quantidade_ingressantes_18_24,
            quantidade_ingressantes_25_29,
            quantidade_ingressantes_30_34,
            quantidade_ingressantes_35_39,
            quantidade_ingressantes_40_49,
            quantidade_ingressantes_50_59,
            quantidade_ingressantes_60_mais,
            quantidade_ingressantes_branca,
            quantidade_ingressantes_preta,
            quantidade_ingressantes_parda,
            quantidade_ingressantes_amarela,
            quantidade_ingressantes_indigena,
            quantidade_ingressantes_cor_nao_declarada,
            quantidade_matriculas,
            quantidade_matriculas_feminino,
            quantidade_matriculas_masculino,
            quantidade_matriculas_diurno,
            quantidade_matriculas_noturno,
            quantidade_matriculas_0_17,
            quantidade_matriculas_18_24,
            quantidade_matriculas_25_29,
            quantidade_matriculas_30_34,
            quantidade_matriculas_35_39,
            quantidade_matriculas_40_49,
            quantidade_matriculas_50_59,
            quantidade_matriculas_60_mais,
            quantidade_matriculas_branca,
            quantidade_matriculas_preta,
            quantidade_matriculas_parda,
            quantidade_matriculas_amarela,
            quantidade_matriculas_indigena,
            quantidade_matriculas_cor_nao_declarada,
            quantidade_concluintes,
            quantidade_concluintes_feminino,
            quantidade_concluintes_masculino,
            quantidade_concluintes_diurno,
            quantidade_concluintes_noturno,
            quantidade_concluintes_0_17,
            quantidade_concluintes_18_24,
            quantidade_concluintes_25_29,
            quantidade_concluintes_30_34,
            quantidade_concluintes_35_39,
            quantidade_concluintes_40_49,
            quantidade_concluintes_50_59,
            quantidade_concluintes_60_mais,
            quantidade_concluintes_branca,
            quantidade_concluintes_preta,
            quantidade_concluintes_parda,
            quantidade_concluintes_amarela,
            quantidade_concluintes_indigena,
            quantidade_concluintes_cor_nao_declarada,
            quantidade_ingressantes_brasileiro,
            quantidade_ingressantes_estrangeiro,
            quantidade_matriculas_brasileiro,
            quantidade_matriculas_estrangeiro,
            quantidade_concluintes_brasileiro,
            quantidade_concluintes_estrangeiro,
            quantidade_alunos_deficiencia,
            quantidade_ingressantes_deficiencia,
            quantidade_matriculas_deficiencia,
            quantidade_concluintes_deficiencia,
            quantidade_ingressantes_financiamento,
            quantidade_ingressantes_financiamento_reembolsavel,
            quantidade_ingressantes_financiamento_reembolsavel_fies,
            quantidade_ingressantes_financiamento_reembolsavel_instituicao,
            quantidade_ingressantes_financiamento_reembolsavel_outros,
            quantidade_ingressantes_financiamento_nao_reembolsavel,
            quantidade_ingressantes_financiamento_nao_reembolsavel_prouni_integral,
            quantidade_ingressantes_financiamento_nao_reembolsavel_prouni_parcial,
            quantidade_ingressantes_financiamento_nao_reembolsavel_instituicao,
            quantidade_ingressantes_financiamento_nao_reembolsavel_outros,
            quantidade_matriculas_financiamento,
            quantidade_matriculas_financiamento_reembolsavel,
            quantidade_matriculas_financiamento_reembolsavel_fies,
            quantidade_matriculas_financiamento_reembolsavel_instituicao,
            quantidade_matriculas_financiamento_reembolsavel_outros,
            quantidade_matriculas_financiamento_nao_reembolsavel,
            quantidade_matriculas_financiamento_nao_reembolsavel_prouni_integral,
            quantidade_matriculas_financiamento_nao_reembolsavel_prouni_parcial,
            quantidade_matriculas_financiamento_nao_reembolsavel_instituicao,
            quantidade_matriculas_financiamento_nao_reembolsavel_outros,
            quantidade_concluintes_financiamento,
            quantidade_concluintes_financiamento_reembolsavel,
            quantidade_concluintes_financiamento_reembolsavel_fies,
            quantidade_concluintes_financiamento_reembolsavel_instituicao,
            quantidade_concluintes_financiamento_reembolsavel_outros,
            quantidade_concluintes_financiamento_nao_reembolsavel,
            quantidade_concluintes_financiamento_nao_reembolsavel_prouni_integral,
            quantidade_concluintes_financiamento_nao_reembolsavel_prouni_parcial,
            quantidade_concluintes_financiamento_nao_reembolsavel_instituicao,
            quantidade_concluintes_financiamento_nao_reembolsavel_outros,
            quantidade_ingressantes_reserva_vaga,
            quantidade_ingressantes_reserva_vaga_rede_publica,
            quantidade_ingressantes_reserva_vaga_etnico,
            quantidade_ingressantes_reserva_vaga_deficiencia,
            quantidade_ingressantes_reserva_vaga_social_renda_familiar,
            quantidade_ingressantes_reserva_vaga_outros,
            quantidade_matriculas_reserva_vaga,
            quantidade_matriculas_reserva_vaga_rede_publica,
            quantidade_matriculas_reserva_vaga_etnico,
            quantidade_matriculas_reserva_vaga_deficiencia,
            quantidade_matriculas_reserva_vaga_social_renda_familiar,
            quantidade_matriculas_reserva_vaga_outros,
            quantidade_concluintes_reserva_vaga,
            quantidade_concluintes_reserva_vaga_rede_publica,
            quantidade_concluintes_reserva_vaga_etnico,
            quantidade_concluintes_reserva_vaga_deficiencia,
            quantidade_concluintes_reserva_vaga_social_renda_familiar,
            quantidade_concluintes_reserva_vaga_outros,
            quantidade_alunos_situacao_trancada,
            quantidade_alunos_situacao_desvinculada,
            quantidade_alunos_situacao_transferida,
            quantidade_alunos_situacao_falecidos,
            quantidade_ingressantes_em_rede_publica,
            quantidade_ingressantes_em_rede_privada,
            quantidade_ingressantes_em_rede_nao_informada,
            quantidade_matriculas_em_rede_publica,
            quantidade_matriculas_em_rede_privada,
            quantidade_matriculas_em_rede_nao_informada,
            quantidade_concluintes_em_rede_publica,
            quantidade_concluintes_em_rede_privada,
            quantidade_concluintes_em_rede_nao_informada,
            quantidade_alunos_parfor,
            quantidade_ingressantes_parfor,
            quantidade_matriculas_parfor,
            quantidade_concluintes_parfor,
            quantidade_alunos_apoio_social,
            quantidade_ingressantes_apoio_social,
            quantidade_matriculas_apoio_social,
            quantidade_concluintes_apoio_social,
            quantidade_alunos_atividade_extracurricular,
            quantidade_ingressantes_atividade_extracurricular,
            quantidade_matriculas_atividade_extracurricular,
            quantidade_concluintes_atividade_extracurricular,
            quantidade_alunos_mobilidade_academica,
            quantidade_ingressantes_mobilidade_academica,
            quantidade_matriculas_mobilidade_academica,
            quantidade_concluintes_mobilidade_academica
        FROM `basedosdados.br_inep_censo_educacao_superior.curso` AS dados
        LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio
        LEFT JOIN `dicionario_tipo_dimensao`
            ON dados.tipo_dimensao = chave_tipo_dimensao
        LEFT JOIN `dicionario_tipo_organizacao_academica`
            ON dados.tipo_organizacao_academica = chave_tipo_organizacao_academica
        LEFT JOIN `dicionario_tipo_organizacao_administrativa`
            ON dados.tipo_organizacao_administrativa = chave_tipo_organizacao_administrativa
        LEFT JOIN `dicionario_rede`
            ON dados.rede = chave_rede
        LEFT JOIN (SELECT DISTINCT id_ies,nome,tipo_instituicao  FROM `basedosdados.br_bd_diretorios_brasil.instituicao_ensino_superior`) AS diretorio_id_ies
            ON dados.id_ies = diretorio_id_ies.id_ies
        LEFT JOIN (SELECT DISTINCT id_curso,nome_curso,nome_area,grau_academico  FROM `basedosdados.br_bd_diretorios_brasil.curso_superior`) AS diretorio_id_curso
            ON dados.id_curso = diretorio_id_curso.id_curso 
        

        WHERE 
                ano = {ano}              
            """

    processamento_cursos = extrair_dados_sql(
    table_name= "cursos_superiores",
    anos=anos,
    cidades=cidades,
    query_base=query_cursos,
    save_dir=save_dir,
    ufs=ufs,
    limit=limit
    )

    return processamento_cursos