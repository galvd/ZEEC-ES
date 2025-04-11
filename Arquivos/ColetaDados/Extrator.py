"""
Bibliotecas Importadas:

basedosdados para realizar consultas SQL no Google BigQuery.
pandas para manipulação de dados.
os, sys, json, gc para manipulação de arquivos e configurações.
datetime para manipulação de datas.
Configuração:

O arquivo de configuração (config.json) é carregado para obter variáveis de ambiente e caminhos.
Funções:

extrair_transf_url(url: str, table_name: str, main_dir: str = None, cidades: list = [], ufs: list = [], anos = [])

Extrai dados de uma URL no formato CSV, aplica filtros e transforma os dados para o formato Parquet.

Parâmetros:

url: URL para download do arquivo CSV.
table_name: Nome da tabela, usado para exibição e identificação dos dados.
main_dir: Diretório principal para salvar o arquivo Parquet.
cidades: Lista de cidades para filtrar os dados.
ufs: Lista de UFs (Unidades Federativas) para filtrar os dados.
anos: Lista de anos para filtrar as colunas de dados.
Processos:

Faz o download e leitura do CSV.
Filtra dados com base em cidades e UFs.
Utiliza pd.melt para transformar colunas de anos em uma coluna única chamada ano.
Limpa os dados e salva no formato Parquet se a lista de anos estiver vazia.
extrair_dados_sql(table_name: str, query_base: str, main_dir: str = None, cidades: list = [], ufs: list = [], anos: list = [], mes: int = None, limit: str = "")

Extrai dados de uma consulta SQL e salva os resultados em um arquivo Parquet.

Parâmetros:

table_name: Nome da tabela, usado para exibição e identificação dos dados.
query_base: Query SQL base com placeholders.
main_dir: Diretório principal para salvar o arquivo Parquet.
cidades, ufs, anos, mes, limit: Parâmetros de filtragem e limitações para a consulta SQL.
Processos:

Constrói e executa a consulta SQL.
Filtra os dados com base em cidades, UFs, anos e meses.
Salva os dados em formato Parquet.
baixar_e_processar_cnpjs(url_template: str, main_dir: str, cidades: list, ufs:list, method: function, anos: list = [])

Faz o download e processamento de arquivos de CNPJ da Receita Federal, aplicando o método de processamento especificado.

Parâmetros:

url_template: Modelo da URL para download dos arquivos de CNPJ.
main_dir: Diretório principal para salvar os arquivos.
cidades, ufs, anos: Listas de cidades, UFs e anos para filtragem.
method: Função que define o método de processamento (ex: multi_thread).
Processos:

Verifica e cria diretórios necessários.
Loop pelos anos e meses para processar os arquivos.
Verifica se o arquivo Parquet já foi gerado; se não, faz o download, descompacta e processa os arquivos.
Unifica os arquivos Parquet após o processamento.
Uso das Funções:

As funções são utilizadas para diferentes etapas do processo de coleta e transformação de dados, dependendo da fonte e do formato dos dados.
A função baixar_e_processar_cnpjs é responsável por coordenar o fluxo de trabalho para dados de CNPJ, enquanto extrair_transf_url e extrair_dados_sql são usadas para dados provenientes de arquivos CSV e consultas SQL, respectivamente.
Notas:

As funções fazem uso extensivo de mensagens de log (print) para acompanhar o progresso e diagnosticar possíveis erros.
O script utiliza o módulo gc para a coleta de lixo, liberando memória após operações pesadas.
"""



from __future__ import annotations
import basedosdados as bd
import polars as pl 
import pandas as pd
import os, sys, json, gc
from datetime import datetime, date
import requests
import zipfile
import io
import gc

with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

from Arquivos.ColetaDados.ToolsColeta import save_parquet, clean_dots, CnpjTreatment
from Arquivos.TratamentoDados.ToolsTratamento import CnpjProcess

cloud_id = config['cloud_id']


def extrair_antenas_url(url: str, table_name: str, main_dir: str = None, cidades_ibge: list = [], ufs: list = []):
    """
    Faz o download do arquivo zip a partir da URL ou utiliza o arquivo existente no diretório informado,
    extrai o CSV "Estacoes_Mosaico_STEL.csv", filtra os registros de antenas com base nas UFs e,
    opcionalmente, nos códigos IBGE dos municípios de interesse, e salva os dados filtrados em um arquivo Parquet.

    :param url: URL do arquivo zip a ser baixado.
    :param table_name: Nome da tabela/arquivo que será usado para salvar os dados processados.
    :param main_dir: Diretório de destino para salvar o arquivo.
    :param cidades_ibge: Lista de códigos IBGE dos municípios de interesse.
    :param ufs: Lista de UFs de interesse.
    """
    header_msg = " ".join([word.capitalize() for word in table_name.split("_")])
    print("Iniciando processamento do zip para " + header_msg)
    
    # Define o caminho para salvar o zip (dinamizado)
    # Se main_dir for informado, constrói um caminho mais elaborado; caso contrário, usa o diretório corrente.
    if main_dir:
        zip_filename = os.path.join(main_dir, "Dados", table_name.title().replace("_", " "), os.path.basename(url))
        os.makedirs(os.path.dirname(zip_filename), exist_ok=True)
    else:
        zip_filename = os.path.basename(url)
    
    # Verifica se o arquivo já existe localmente; se sim, lê o conteúdo, caso contrário faz o download.
    if os.path.exists(zip_filename):
        print(f"O arquivo '{zip_filename}' já existe. Pulando o download.")
        with open(zip_filename, "rb") as f:
            zip_content = f.read()
    else:
        response = requests.get(url)
        if response.status_code != 200:
            print("Falha no download. Código de status:", response.status_code)
            return None
        print("Download concluído com sucesso.")
        zip_content = response.content
        
        # Salva o zip localmente para uso futuro
        with open(zip_filename, "wb") as f:
            f.write(zip_content)
            print(f"Arquivo '{zip_filename}' salvo.")
    
    # Abre o arquivo zip em memória
    with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
        arquivos = z.namelist()
        print("Arquivos encontrados no zip:", arquivos)

        # Processa apenas o CSV desejado: "Estacoes_Mosaico_STEL.csv"
        for nome_arquivo in arquivos:
            if os.path.basename(nome_arquivo).lower() != "Estacoes_SMP.csv".lower():
                continue
            
            print("Processando o arquivo:", nome_arquivo)
            # Tenta ler o CSV com encoding 'utf8'; se falhar, tenta 'latin1'
            try:
                with z.open(nome_arquivo) as arquivo:
                    df = pl.read_csv(arquivo, separator=';', encoding='utf8', ignore_errors=True)
            except Exception as e:
                print("Erro ao ler o CSV com utf8. Tentando encoding latin1...", e)
                with z.open(nome_arquivo) as arquivo:
                    df = pl.read_csv(arquivo, separator=';', encoding='latin1', ignore_errors=True)
            
            print("Visualizando os primeiros registros:")
            print(df.head())
            
            # Filtra os registros conforme UF e Código IBGE (das cidades), se aplicável
            if "UF" in df.columns and ufs:
                df = df.filter(pl.col("UF").is_in(ufs))
            if "Código IBGE" in df.columns and cidades_ibge:
                # Converte os valores para string para garantir a compatibilidade na comparação
                df = df.filter(pl.col("Código IBGE").cast(pl.Utf8).is_in(cidades_ibge))
            
            # Lista das colunas que precisam ser convertidas para string
            columns_to_str = [
                "Cep",
                "CNPJ ou CPF",
                "Número da Estação",
                "Número do Ato de RF",
                "Código do Tipo Antena",
                "Ganho da Antena",
                "Frente Costa da Antena"
                "Código de Homologação da Antena"
            ]
            # Converte dinamicamente as colunas existentes para string (Utf8)
            colunas_existentes = [col for col in columns_to_str if col in df.columns]
            if colunas_existentes:
                df = df.with_columns([pl.col(col).cast(pl.Utf8) for col in colunas_existentes])
            
            print("Dados após conversões:")
            print(df.head())

            # Remove as colunas indesejadas, se existirem
            cols_remover = ['Número Fistel', 'NumCnpjCpf', 'NumServico', 'Frequência (MHz)',
                'Banda_MHZ', 'Frequência Inicial', 'Frequência Final', 'FreqTxMHz', 'FreqRxMHz',
                'Designação Emissão', 'Entidade', 'NumSetor', 'EnderecoEstacao',
                'EndBairro', 'EndNumero', 'EndComplemento', 'ClassInfraFisica', 'AnoMesLic',
                'Situacao', 'Caráter', 'Número Ato', "Data Validade", "Data Licenciamento",
                'Código Nacional', 'Nome da UF', 'Latitude decimal', 'Longitude decimal',
            ]
            cols_existentes_remover = [col for col in cols_remover if col in df.columns]
            if cols_existentes_remover:
                df = df.drop(cols_existentes_remover)
            
            df = df.sort(by=["UF", "Código IBGE", 'Latitude', 'Longitude'])
            

            # Dropando linhas duplicadas
            df = df.unique(subset=["UF", "Código IBGE", 
                                   'Número Estação', 'Subfaixa Estação'], keep="first")
            
            print("Dados após remoção das colunas indesejadas:")
            print(df.head())
            print("Número de linhas final:", df.height)
            # Converte o DataFrame de Polars para pandas antes de salvar
            df = df.to_pandas()

            
            
            # Salva o DataFrame filtrado em um arquivo Parquet
            save_parquet(main_dir=main_dir, table_name=table_name, df=df)
                
            # Libera a memória utilizada pelo DataFrame
            del df
            gc.collect()
                
    print("Processamento dos dados de " + header_msg + " completo!")


def extrair_transf_url(url: str, table_name: str, main_dir: str = None, cidades: list = [], ufs: list = [], anos = []):

    # Ler o arquivo CSV diretamente da URL
    print("Iniciando download do csv do ".join([word.capitalize() for word in table_name.split(sep="_")]))
    years = {year:float for year in range(2007, datetime.today().year)}
    dtypes = {'COD_MUN': str, 'Município': str,  'UF Município - UF': str,   'Mês': int}.update(years)
    try:
        df_raw = pd.read_csv(url, delimiter=";", decimal=',' ,thousands='.',  dtype=dtypes)
    except: 
        try:
            print("Erro ao ler csv como UTF-8. Iniciando tentativa com Latin1")
            df_raw = pd.read_csv(url, delimiter=";", encoding="ISO-8859-1", decimal=',' ,thousands='.',  dtype=dtypes)
        except:
            return print("Não foi possível ler o arquivo csv do link fornecido.")

    # Filtrando os municipios de interesse
    df_raw = df_raw[df_raw['Município'].isin(cidades) & df_raw['UF'].isin(ufs)]
    # Verificar se o DataFrame não ficou vazio
    print(f"Número de linhas após filtro de municípios e UFs: {df_raw.shape[0]}")
    if df_raw.empty:
        print("DataFrame vazio após filtragem de municípios e UFs.")
        return
    

    # Usar melt para transformar as colunas de ano em uma coluna única chamada 'Ano'
    year_columns = [str(year) for year in anos]
    # for col in year_columns:
    #     print(f"Coluna {col} antes de aplicar clean_dots:")
    #     print(df_raw[col].head())  # Mostrar primeiros valores da coluna
    #     df_raw[col] = df_raw[col].apply(clean_dots)
    #     print(f"Coluna {col} após aplicar clean_dots:")
    #     print(df_raw[col].head())
    
    # # Verificar se todas as colunas de ano estão vazias após clean_dots
    # print("Verificando valores nulos nas colunas de ano após clean_dots:")
    # print(df_raw[year_columns].isnull().sum())
    
    # # Se todas as colunas tiverem valores nulos, print uma mensagem de erro
    # if df_raw[year_columns].isnull().all().all():
    #     print("Todas as colunas de ano estão nulas após aplicar clean_dots.")
    #     return
    
    print('DataFrame após aplicação de clean_dots:', df_raw.head())


    fixed_columns = ['COD_MUN', 'Município', 'UF', 'Município - UF', 'Mês']
    
    print(df_raw.dtypes)
    df = pd.melt(df_raw, id_vars=fixed_columns, value_vars=year_columns, 
                        var_name='ano', value_name='transferencias')
    print('df_clean', df)
    if len(df) == 0:
        print(f"Tabela vazia do " + " ".join([word.capitalize() for word in table_name.split(sep="_")]))
    
    else:
        print(f"Dados baixados com sucesso!")
    
    # Deleta e libera a memória do df cru
    del df_raw
    gc.collect()

    # Renomear as colunas
    df.rename(columns={
        'Município': 'id_municipio_nome',
        'UF': 'uf',
        'Mês': 'mes'
    }, inplace=True)

    # Excluir a coluna "Município - UF"
    df.drop(columns=['Município - UF', 'COD_MUN'], inplace=True)
    df['mes'] = df['mes'].astype(int)
    print(df.head())

    if anos == []:
        save_parquet(main_dir=main_dir, table_name=table_name, df=df)

    print("Processamento dos dados do " + " ".join([word.capitalize() for word in table_name.split(sep="_")]) + " completo!")


def extrair_dados_sql(table_name: str,  query_base: str, main_dir: str = None, 
                      cidades: list = [], ufs: list = [], anos: list = [''], 
                      mes: int = None, limit: str = ""):
    
    print(f"Iniciando download dos dados do " + " """.join([word.capitalize() for word in table_name.split(sep="_")]))

    # Converte as listas em strings apropriadas para uso na query SQL
    cidades_sql = ", ".join(f"'{cidade}'" for cidade in cidades)
    ufs_sql = ", ".join(f"'{uf}'" for uf in ufs)

    for ano in anos:
        # Caminho para verificar se o arquivo Parquet já existe
        dir_path = os.path.join(main_dir, "Dados", " ".join([word.capitalize() for word in table_name.split(sep="_")]))
        file_suffix = f"_{ano}"
        file_path = os.path.join(dir_path, f'{table_name}{file_suffix}.parquet')

        # Checagem se o arquivo Parquet do ano já existe
        if os.path.exists(file_path):
            print(f"Arquivo Parquet para {ano} já existe em {file_path}. Pulando a extração desse ano.")
            continue

        # Substitui placeholders no query_base
        query = query_base.format(ano=ano, cidades=cidades_sql, ufs=ufs_sql)
        
        if cidades != [] and query_base.find("ano = {ano}") != -1 and table_name not in ["enem", "educ_base"]:
            query+= f'AND diretorio_id_municipio.nome IN ({cidades_sql}) \n'
        
        if cidades != [] and query_base.find("ano = {ano}") == -1 and table_name not in ["enem", "educ_base", "cnpj_empresas"]:
            query+= f'diretorio_id_municipio.nome IN ({cidades_sql}) \n'
        
        if cidades != [] and query_base.find("ano = {ano}") == -1 and table_name in ["cnpj_empresas"]:
            query+= f'AND dados.id_municipio IN ({cidades_sql})\n'
            
        
        if ufs != [] and table_name not in ["enem", "cnpj_empresas",  "cnpj_joined", 'caged_ES']:
            query += f' AND sigla_uf in ({ufs_sql}) \n'

        if mes != None:
            query += f' AND mes = {mes} \n'
        if limit != "":
            query += f' {limit} \n'

        print(query)

        # Executa a consulta
        df = bd.read_sql(query=query, billing_project_id=cloud_id)

        print(df.head())
        
        if len(df) == 0:
            print(f"Tabela vazia para {ano} do " + " ".join([word.capitalize() for word in table_name.split(sep="_")]))
            continue
        
        else:
            print(f"Dados de {ano} baixados com sucesso!")

        if ano == None:
            save_parquet(main_dir=main_dir, table_name=table_name, df=df)
        else:
            save_parquet(main_dir=main_dir, table_name=table_name, df=df, ano = ano)
    
    print("Processamento dos dados do " + " ".join([word.capitalize() for word in table_name.split(sep="_")]) + " completo!")
    

def baixar_e_processar_cnpjs(url_template: str, main_dir: str, cidades: list, ufs:list, method: function, fonte: str, anos: list = []):
        treat = CnpjTreatment()
        process = CnpjProcess()
        cnpj_dir = os.path.join(main_dir, "Dados", f"Cnpj {fonte.title()}")

        print('Iniciando download dos arquivos de CNPJs da Receita Federal')

        mes_atual = datetime.today().month
        lista_meses = [f"{mes:02d}" for mes in range(mes_atual-5, mes_atual)]
            
        #loop em anos, pegar meses com datetime
        for ano in anos:
            if ano != datetime.today().year:
                print(f'Ano de {ano} inexistente no repositório. Processo para {ano} finalizado.')
                continue

            for mes in lista_meses:

                # Checando se o parquet objetivo do ano_mes já foi gerado
                parquet_final = os.path.exists(os.path.join(cnpj_dir, f'cnpjs_{ano}_{mes}.parquet'))

                if parquet_final:
                    print(f'O arquivo objetivo cnpjs_{ano}_{mes}.parquet já foi gerado, removendo arquivos e indo para o próximo mês')

                    # deleta zips e csv
                    process.limpar_residuo(ano= ano, mes= mes, cnpj_dir= cnpj_dir, fonte= fonte)
                    continue
                
                url_formatada = url_template.format(ano=ano, mes=mes)
                soup, file_count = treat.check_url(url= url_formatada, cnpj_dir= cnpj_dir, ano= ano, mes= mes, fonte= fonte)

                # print(f'Iniciando processamento com número de threads alocadas: {thread_num}')

                print(url_formatada, cnpj_dir, cidades, ufs, anos)

                # funções de method em Arquivos.TratamentoDados.ToolsTratamento
                # Função que realiza o loot para o número de arquivos na url
                method(cnpj_dir, ano, mes, url_formatada, file_count, cidades, ufs, fonte) # Exemplo de method: ToolsTratamento.one_thread
                print('saiu de method')
                
                
                print('Unificando parquets')
                process.unify_parquet(ano= ano, mes= mes, cnpj_dir= cnpj_dir, fonte= fonte)
                process.cnpj_treat(ano= ano, main_dir= main_dir, mes= mes, fonte= fonte)
                # Limpar resíduos após o processamento
                process.limpar_residuo(ano= ano, mes= mes, cnpj_dir= cnpj_dir, fonte= fonte)

