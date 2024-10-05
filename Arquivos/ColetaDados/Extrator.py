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
import pandas as pd 
import os, sys, json, gc
from datetime import datetime

with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

from Arquivos.ColetaDados.ToolsColeta import save_parquet, clean_dots, CnpjTreatment
from Arquivos.TratamentoDados.ToolsTratamento import CnpjProcess

cloud_id = config['cloud_id']



def extrair_transf_url(url: str, table_name: str, main_dir: str = None, cidades: list = [], ufs: list = [], anos = []):

    # Ler o arquivo CSV diretamente da URL
    print("Iniciando download do csv do ".join([word.capitalize() for word in table_name.split(sep="_")]))
    try:
        df_raw = pd.read_csv(url, delimiter=";")
    except: 
        try:
            print("Erro ao ler csv como UTF-8. Iniciando tentativa com Latin1")
            df_raw = pd.read_csv(url, delimiter=";", encoding="ISO-8859-1")
        except:
            return print("Não foi possível ler o arquivo csv do link fornecido.")

    # Filtrando os municipios de interesse
    df_raw = df_raw[df_raw['Município'].isin(cidades) & df_raw['UF'].isin(ufs)]

    # Usar melt para transformar as colunas de ano em uma coluna única chamada 'Ano'
    year_columns = [str(year) for year in anos]  # Colunas de 2014 a 2024
    for col in year_columns:
        df_raw[col] = df_raw[col].apply(clean_dots)

    fixed_columns = ['COD_MUN', 'Município', 'UF', 'Município - UF', 'Mês']
    
    
    df = pd.melt(df_raw, id_vars=fixed_columns, value_vars=year_columns, 
                        var_name='ano', value_name='transferencias')
        
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


def extrair_dados_sql(table_name: str,  query_base: str, main_dir: str = None, cidades: list = [], ufs: list = [], anos: list = [], mes: int = None, limit: str = ""):
    
    print(f"Iniciando download dos dados do " + " "" ".join([word.capitalize() for word in table_name.split(sep="_")]))

    # Converte as listas em strings apropriadas para uso na query SQL
    cidades_sql = ", ".join(f"'{cidade}'" for cidade in cidades)
    uf_sql = ", ".join(f"'{uf}'" for uf in ufs)

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
        query = query_base.format(ano=ano, cidades=cidades_sql, ufs=uf_sql)
        
        if cidades != [] and query_base.find("ano = {ano}") != -1 and table_name not in ["enem", "educ_base"]:
            query+= f'AND diretorio_id_municipio.nome IN ({cidades_sql}) \n'
        
        if cidades != [] and query_base.find("ano = {ano}") == -1 and table_name not in ["enem", "educ_base", "cnpj_empresas"]:
            query+= f'diretorio_id_municipio.nome IN ({cidades_sql}) \n'
        
        if cidades != [] and query_base.find("ano = {ano}") == -1 and table_name in ["cnpj_empresas"]:
            query+= f'AND dados.id_municipio IN ({cidades_sql})\n'
            
        
        if ufs != [] and table_name not in ["enem", "cnpj_empresas"]:
            query += f' AND sigla_uf in ({uf_sql}) \n'

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
    

def baixar_e_processar_cnpjs(url_template: str, main_dir: str, cidades: list, ufs:list, method: function, anos: list = []):
        treat = CnpjTreatment()
        process = CnpjProcess()
        cnpj_dir = os.path.join(main_dir, "Dados", "Cnpj Estabelecimentos")

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
                    process.limpar_residuo(ano, mes, cnpj_dir)
                    continue
                
                url_formatada = url_template.format(ano=ano, mes=mes)
                soup, file_count = treat.check_url(url= url_formatada, cnpj_dir= cnpj_dir, ano= ano, mes= mes)

                # print(f'Iniciando processamento com número de threads alocadas: {thread_num}')

                print(url_formatada, cnpj_dir, cidades, ufs, anos)

                # funções de method em Arquivos.TratamentoDados.ToolsTratamento
                # Função que realiza o loot para o número de arquivos na url
                method(cnpj_dir, ano, mes, url_formatada, file_count, cidades, ufs) # Exemplo de method: CnpjProcess.MultiThread.routine

                # Limpar resíduos após o processamento
                process.limpar_residuo(ano, mes, cnpj_dir)
                print('Unificando parquets')
                process.unify_parquet(ano= ano, mes= mes, cnpj_dir= cnpj_dir)
                process.estabelecimentos_treat(ano = ano, main_dir=main_dir, mes=mes)

