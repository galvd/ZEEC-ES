from __future__ import annotations
import basedosdados as bd
import pandas as pd
from bs4 import BeautifulSoup

import requests, wget, os, sys, time, glob, re, json, gc

with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

from Arquivos.ColetaDados.Tools import save_parquet, clean_dots

cloud_id = config['cloud_id']


def extrair_transf_url(url: str, table_name: str, save_dir: str = None, cidades: list = [], ufs: list = [], anos = []):


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
        save_parquet(save_dir=save_dir, table_name=table_name, df=df)

    print("Processamento dos dados do " + " ".join([word.capitalize() for word in table_name.split(sep="_")]) + " completo!")



def extrair_dados_sql(table_name: str,  query_base: str, save_dir: str = None, cidades: list = [], ufs: list = [], anos: list = [], mes: int = None, limit: str = ""):
    
    print(f"Iniciando download dos dados do " + " "" ".join([word.capitalize() for word in table_name.split(sep="_")]))

    # Converte as listas em strings apropriadas para uso na query SQL
    cidades_sql = ", ".join(f"'{cidade}'" for cidade in cidades)
    uf_sql = ", ".join(f"'{uf}'" for uf in ufs)
    

    for ano in anos:
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
            save_parquet(save_dir=save_dir, table_name=table_name, df=df)
        else:
            save_parquet(save_dir=save_dir, table_name=table_name, df=df, ano = ano)
        return df
    
    print("Processamento dos dados do " + " ".join([word.capitalize() for word in table_name.split(sep="_")]) + " completo!")
    



def extrair_cnpj_url(url: str, ano: str, mes: str, dir: str = None, count: int = 0):
    
    # requisição da página
    page = requests.get(url)   
    data = page.text
    soup = BeautifulSoup(data)

    # Pasta de destino para os arquivos zip
    pasta_compactados = dir  # Local dos arquivos zipados da Receita

    # Verifica se a pasta existe, se não, cria a pasta
    if not os.path.exists(pasta_compactados):
        os.makedirs(pasta_compactados)

    print('Relação de Arquivos em ' + url)

    def get_file_size(url):
        """Função para obter o tamanho do arquivo via Content-Length no cabeçalho HTTP"""
        response = requests.head(url)
        size = response.headers.get('Content-Length')
        if size:
            size = int(size)
            # Converte bytes para MB
            size_in_mb = size / (1024 * 1024)
            return size_in_mb
        return None
    
    # Função para verificar se o arquivo já existe
    def arquivo_existe(arquivo_original, arquivo_novo):
        arq_original = os.path.exists(os.path.join(pasta_compactados, arquivo_original))
        arq_novo = os.path.exists(os.path.join(pasta_compactados, arquivo_novo))
        if arq_original or arq_novo:
            return True
        else:
            return False

    for link in soup.find_all('a'):
        # Verifica se o link é de um arquivo estabelecimentos\d.zip
        if re.search(fr'stabelecimentos{count}\.zip$', str(link.get('href'))):
            cam = link.get('href')
            full_url = cam if cam.startswith('http') else url + cam
            
            nome_arquivo_original = os.path.basename(full_url)  # Nome original do arquivo
            nome_arquivo_novo = f'Estabelecimentos{count}_{ano}_{mes}.zip'  # Nome novo do arquivo

            # Verifica se o arquivo já existe
            if arquivo_existe(nome_arquivo_original, nome_arquivo_novo):
                print(f"O arquivo {nome_arquivo_novo} já existe. Pulando download.")
                return nome_arquivo_novo # Sai da função, pois o arquivo já existea
            
            
            # Obter o tamanho do arquivo
            file_size = get_file_size(full_url)
            
            if file_size:
                print(f"{full_url} - {file_size:.2f} MB")
            else:
                print(f"{full_url} - Tamanho: Não disponível")
                
        
    def bar_progress(current, total, width=80):
        if total>=2**20:
            tbytes='Megabytes'
            unidade = 2**20
        else:
            tbytes='kbytes'
            unidade = 2**10
        progress_message = f"Download status: %d%% [%d / %d] {tbytes}" % (current / total * 100, current//unidade, total//unidade)
        sys.stdout.write("\r" + progress_message)
        sys.stdout.flush()
    
    # Download do arquivo
    caminho_arquivo_original = os.path.join(pasta_compactados, nome_arquivo_original)
    print(f'\n{time.asctime()} - Iniciando download do item {count}: {full_url}')
    wget.download(full_url, out=caminho_arquivo_original, bar=bar_progress)

    # Renomeia o arquivo baixado para o novo nome
    caminho_arquivo_novo = os.path.join(pasta_compactados, nome_arquivo_novo)
    try:
        os.rename(caminho_arquivo_original, caminho_arquivo_novo)
        print(f"\nArquivo baixado e renomeado para {caminho_arquivo_novo}")
        return nome_arquivo_novo
    except OSError as e:
        print(f"\nErro ao renomear o arquivo {caminho_arquivo_original}: {e}")
        return None


#lista dos arquivos
r'http://200.152.38.155/CNPJ/dados_abertos_cnpj/AAAA-mm/Estabelecimentos\d.zip'




