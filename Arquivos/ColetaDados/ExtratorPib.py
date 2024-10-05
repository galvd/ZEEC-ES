"""
ExtratorPib.py
Descrição Geral:

O ExtratorPib.py é um módulo Python responsável pela extração de dados relacionados ao Produto Interno Bruto (PIB) de municípios a partir de uma base de dados fornecida pela Base dos Dados. Utiliza consultas SQL para obter informações detalhadas sobre o PIB e outros indicadores econômicos e processa esses dados com base em parâmetros fornecidos.

Importações:

sys: Utilizado para manipulação de caminhos de sistema e configuração de caminhos adicionais.
json: Utilizado para ler e interpretar o arquivo de configuração JSON.
extrair_dados_sql da Arquivos.ColetaDados.Extrator: Função utilizada para executar consultas SQL e processar os dados extraídos.
Configuração:

O arquivo carrega uma configuração a partir de um arquivo JSON localizado em '.\\Arquivos\\config.json'. O caminho de rede especificado no arquivo JSON é adicionado ao sys.path para permitir a importação do módulo Extrator.

Funções:

extrair_pib_cidades
Descrição:

Extrai dados de PIB para uma lista de municípios e anos específicos. Os dados são obtidos através de uma consulta SQL e processados com base nas condições fornecidas.

Parâmetros:

anos (list): Lista de anos para os quais os dados devem ser extraídos.
cidades (list): Lista de IDs de municípios para os quais os dados devem ser extraídos.
main_dir (str, opcional): Diretório principal onde os dados processados serão salvos.
ufs (list): Lista de códigos de Unidades Federativas (UFs) para filtrar os dados.
limit (str, opcional): Limite para a consulta SQL (pode ser usado para paginação ou restrição de resultados).
Funcionamento:

Define uma consulta SQL (query_pib) para obter dados de PIB dos municípios, incluindo identificadores e valores relacionados ao PIB e outras métricas econômicas.
Utiliza a função extrair_dados_sql para executar a consulta SQL com base nos parâmetros fornecidos e processar os dados extraídos.
Retorna o resultado do processamento dos dados.
Consultas SQL:

A consulta SQL utilizada (query_pib) obtém os seguintes dados:

id_municipio: Identificador do município.
id_municipio_nome: Nome do município.
ano: Ano dos dados.
pib: Valor do PIB.
impostos_liquidos: Impostos líquidos.
va: Valor adicionado total.
va_agropecuaria: Valor adicionado da agropecuária.
va_industria: Valor adicionado da indústria.
va_servicos: Valor adicionado dos serviços.
va_adespss: Valor adicionado das atividades de defesa, segurança pública e assistência social.
Notas:

O query_pib é uma consulta base que será ajustada com o parâmetro ano para a extração de dados por ano específico.
A função extrair_dados_sql deve ser implementada no módulo Arquivos.ColetaDados.Extrator e é responsável por lidar com a execução da consulta SQL e processamento dos dados.

"""



from __future__ import annotations
import sys, json

with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

from Arquivos.ColetaDados.Extrator import extrair_dados_sql



def extrair_pib_cidades(anos: list, cidades: list, main_dir: str = None, ufs = list, limit: str = ""):

    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/fcf025ca-8b19-4131-8e2d-5ddb12492347?table=fbbbe77e-d234-4113-8af5-98724a956943
    query_pib = """
        SELECT
            dados.id_municipio AS id_municipio,
            diretorio_id_municipio.nome AS id_municipio_nome,
            ano,
            pib,
            impostos_liquidos,
            va,
            va_agropecuaria,
            va_industria,
            va_servicos,
            va_adespss
        FROM `basedosdados.br_ibge_pib.municipio` AS dados
        LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio
        WHERE 
                ano = {ano}              
            """

    processamento_pib = extrair_dados_sql(
    table_name= "pib_municipios",
    anos=anos,
    cidades=cidades,
    query_base=query_pib,
    main_dir=main_dir,
    limit=limit
    )


