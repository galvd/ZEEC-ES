from __future__ import annotations
import json
import sys
import os
import basedosdados as bd
import gc
import pandas as pd

with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

cloud_id = config['cloud_id']



def save_parquet(save_dir: str, table_name: str, df: pd.DataFrame):

    # Garante que o diretório existe
    dir_path = os.path.join(save_dir, "Dados", " ".join([word.capitalize() for word in table_name.split(sep="_")]))
    os.makedirs(dir_path, exist_ok=True)
    
    # Define o caminho completo do arquivo Parquet e o temporário
    file_path = os.path.join(dir_path, f'{table_name}.parquet')
    temp_file_path = os.path.join(dir_path, f'{table_name}_temp.parquet')

    # Salva o DataFrame no arquivo temporário
    df.to_parquet(temp_file_path, index=False)
    print(f"Arquivo temporário salvo em: {temp_file_path}")
    
    # Verifica se o arquivo original já existe e o remove
    if os.path.exists(file_path):
        print(f"Arquivo original encontrado. Removendo: {file_path}")
        os.remove(file_path)
    
    # Renomeia o arquivo temporário para o nome final
    os.rename(temp_file_path, file_path)
    print(f"Arquivo final salvo em: {file_path}")



def extrair_url(url: str, table_name: str, save_dir: str = None, cidades: list = [], ufs: list = [], anos = []):

    # Função para limpar a formatação de separadores de milhar e decimais
    def limpar_valor(valor):
        if isinstance(valor, str):
            # Remover os pontos como separadores de milhar e substituir os espaços por ponto decimal
            valor_limpo = valor.replace('.', '').replace(',', '.').strip()
            try:
                return float(valor_limpo)
            except ValueError:
                return None
        return valor

    # Ler o arquivo CSV diretamente da URL
    print("Iniciando download do csv do ".join([word.capitalize() for word in table_name.split(sep="_")]))
    try:
        df_raw = pd.read_csv(url, delimiter=";")
    except: 
        try:
            print("Erro ao ler csv como UTF8. Iniciando tentativa com Latin1")
            df_raw = pd.read_csv(url, delimiter=";", encoding="ISO-8859-1")
        except:
            return print("Não foi possível ler o arquivo csv do link fornecido.")

    # Filtrando os municipios de interesse
    df_raw = df_raw[df_raw['Município'].isin(cidades)]

    # Usar melt para transformar as colunas de ano em uma coluna única chamada 'Ano'
    year_columns = [str(year) for year in anos]  # Colunas de 2014 a 2024
    for col in year_columns:
        df_raw[col] = df_raw[col].apply(limpar_valor)

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

    save_parquet(save_dir=save_dir, table_name=table_name, df=df)

    print("Processamento dos dados do " + " ".join([word.capitalize() for word in table_name.split(sep="_")]) + " completo!")



def extrair_dados_sql(table_name: str, anos: list,  query_base: str, save_dir: str = None, cidades: list = [], ufs: list = [], mes: int = None, limit: str = ""):
    
    print(f"Iniciando download dos dados do " + " "" ".join([word.capitalize() for word in table_name.split(sep="_")]))

    # Converte as listas em strings apropriadas para uso na query SQL
    cidades_sql = ", ".join(f"'{cidade}'" for cidade in cidades)
    uf_sql = ", ".join(f"'{uf}'" for uf in ufs)
    

    for ano in anos:
        # Substitui placeholders no query_base
        query = query_base.format(ano=ano, cidades=cidades_sql, ufs=uf_sql)
        
        if cidades != [] and query_base.find("ano = {ano}") != -1 and table_name not in ["enem", "educ_base"]:
            query+= f'AND diretorio_id_municipio.nome IN ({cidades_sql}) \n'
        
        if cidades != [] and query_base.find("ano = {ano}") == -1 and table_name not in ["enem", "educ_base"]:
            query+= f'diretorio_id_municipio.nome IN ({cidades_sql}) \n'
        
        if ufs != [] and table_name not in ["enem"]:
            query += f' AND sigla_uf in ({uf_sql}) \n'
        if mes != None:
            query += f' AND mes = {mes} \n'
        if limit != "":
            query += f' {limit} \n'

        # print(query)

        # Executa a consulta
        df = bd.read_sql(query=query, billing_project_id=cloud_id)

        print(df.head())
        
        if len(df) == 0:
            print(f"Tabela vazia para {ano} do " + " ".join([word.capitalize() for word in table_name.split(sep="_")]))
            continue
        
        else:
            print(f"Dados de {ano} baixados com sucesso!")

        save_parquet(save_dir=save_dir, table_name=table_name, df=df)
    
    print("Processamento dos dados do " + " ".join([word.capitalize() for word in table_name.split(sep="_")]) + " completo!")
