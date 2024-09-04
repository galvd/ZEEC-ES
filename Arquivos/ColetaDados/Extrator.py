from __future__ import annotations
import json
import sys
import os
import basedosdados as bd



with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

cloud_id = config['cloud_id']

def extrair_dados(table_name: str, anos: list,  query_base: str, save_dir: str = None, cidades: list = [], ufs: list = [], mes: int = None, limit: str = ""):
    
    print(f"Iniciando download dos dados do " + " "" ".join([word.capitalize() for word in table_name.split(sep="_")]))

    # Converte as listas em strings apropriadas para uso na query SQL
    cidades_sql = ", ".join(f"'{cidade}'" for cidade in cidades)
    uf_sql = ", ".join(f"'{uf}'" for uf in ufs)
    
    df_matriz = []

    for ano in anos:
        # Substitui placeholders no query_base
        query = query_base.format(ano=ano)
        
        if cidades != [] and query_base.find("ano = {ano}") != -1:
            query+= f'AND diretorio_id_municipio.nome IN ({cidades_sql}) \n'
        
        if cidades != [] and query_base.find("ano = {ano}") == -1:
            query+= f'diretorio_id_municipio.nome IN ({cidades_sql}) \n'
        
        if ufs != []:
            query += f' AND sigla_uf in ({uf_sql}) \n'
        if mes != None:
            query += f' AND mes = {mes} \n'
        if limit != "":
            query += f' {limit} \n'

        # Executa a consulta
        df = bd.read_sql(query=query, billing_project_id=cloud_id)
        
        if len(df) == 0:
            print(f"Tabela vazia para {ano} do" + " ".join([word.capitalize() for word in table_name.split(sep="_")]))
        
        else:
            print(f"Dados de {ano} baixados com sucesso!")

        df_matriz.append(df)

        if save_dir:
            # Garante que o diretório existe
            dir_path = os.path.join(save_dir, "Dados", " ".join([word.capitalize() for word in table_name.split(sep="_")]))
            os.makedirs(dir_path, exist_ok=True)
            
            # Define o caminho completo do arquivo Parquet e o temporário
            file_path = os.path.join(save_dir, f'{table_name}_{ano}.parquet')
            temp_file_path = os.path.join(save_dir, f'{table_name}_{ano}_temp.parquet')

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
    
    print("Processamento dos dados do " + " ".join([word.capitalize() for word in table_name.split(sep="_")]) + " completo!")
    return df_matriz
