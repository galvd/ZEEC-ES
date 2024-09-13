# -*- coding: utf-8 -*-


import pandas as pd, sqlalchemy, glob, time, dask.dataframe as dd
import os, sys, zipfile


dataReferencia = 'xx/xx/2024' #input('Data de referência da base dd/mm/aaaa: ')
pasta_compactados = os.getcwd() + r"\\Dados\\Cnpj Empresas" #local dos arquivos zipados da Receita
pasta_saida = os.getcwd() + r"\\Dados\\db" #esta pasta deve estar vazia. 

cam = os.path.join(pasta_saida, 'cnpj.db') 
print(cam)
if os.path.exists(cam):
    input(f'O arquivo {cam} já existe. Apague-o primeiro e rode este script novamente.')
    sys.exit()

print('Início:', time.asctime())

engine = sqlalchemy.create_engine(f'sqlite:///{cam}')

arquivos_zip = list(glob.glob(os.path.join(pasta_compactados,r'*.zip')))

print(arquivos_zip)

for arq in arquivos_zip:
    print(time.asctime(), 'descompactando ' + arq)
    with zipfile.ZipFile(arq, 'r') as zip_ref:
        zip_ref.extractall(pasta_saida)


def sqlCriaTabela(nomeTabela, colunas):
    sql = 'CREATE TABLE ' + nomeTabela + ' ('
    for k, coluna in enumerate(colunas):
        sql += '\n' + coluna + ' TEXT'
        if k+1<len(colunas):
            sql+= ',' #'\n'
    sql += ')\n'
    return sql


colunas_estabelecimento = ['cnpj_basico','cnpj_ordem', 'cnpj_dv','matriz_filial', 
              'nome_fantasia',
              'situacao_cadastral','data_situacao_cadastral', 
              'motivo_situacao_cadastral',
              'nome_cidade_exterior',
              'pais',
              'data_inicio_atividades',
              'cnae_fiscal',
              'cnae_fiscal_secundaria',
              'tipo_logradouro',
              'logradouro', 
              'numero',
              'complemento','bairro',
              'cep','uf','municipio',
              'ddd1', 'telefone1',
              'ddd2', 'telefone2',
              'ddd_fax', 'fax',
              'correio_eletronico',
              'situacao_especial',
              'data_situacao_especial']   


sql = sqlCriaTabela('estabelecimento', colunas_estabelecimento)


def carregaTipo(nome_tabela, tipo, colunas):
    #usando dask, bem mais rápido que pandas
    arquivos = list(glob.glob(os.path.join(pasta_saida, '*' + tipo)))
    for arq in arquivos:
        print(f'carregando: {arq=}')
        print('lendo csv ...', time.asctime())
        ddf = dd.read_csv(arq, sep=';', header=None, names=colunas, encoding='latin1', dtype=str, na_filter=None)
        #dask possibilita usar curinga no nome de arquivo, por ex: 
        #ddf = dd.read_csv(pasta_saida+r'\*' + tipo, sep=';', header=None, names=colunas ...
        ddf.to_sql(nome_tabela, str(engine.url), index=None, if_exists='append', dtype=sqlalchemy.sql.sqltypes.TEXT)



carregaTipo('estabelecimento', '.ESTABELE', colunas_estabelecimento)


#ajusta capital social e indexa as colunas

sqls = '''

ALTER TABLE estabelecimento ADD COLUMN cnpj text;
Update estabelecimento
set cnpj = cnpj_basico||cnpj_ordem||cnpj_dv;


CREATE  INDEX idx_estabelecimento_cnpj_basico ON estabelecimento (cnpj_basico);
CREATE  INDEX idx_estabelecimento_cnpj ON estabelecimento (cnpj);
CREATE  INDEX idx_estabelecimento_nomefantasia ON estabelecimento (nome_fantasia);


DROP TABLE IF EXISTS socios_original;


CREATE TABLE "_referencia" (
	"referencia"	TEXT,
	"valor"	TEXT
);
'''

print('Inicio sqls:', time.asctime())
ktotal = len(sqls.split(';'))
for k, sql in enumerate(sqls.split(';')):
    print('-'*20 + f'\nexecutando parte {k+1}/{ktotal}:\n', sql)
    engine.execute(sql)
    print('fim parcial...', time.asctime())
print('fim sqls...', time.asctime())

#inserir na tabela referencia_

qtde_cnpjs = engine.execute('select count(*) as contagem from estabelecimento;').fetchone()[0]

engine.execute(f"insert into _referencia (referencia, valor) values ('CNPJ', '{dataReferencia}')")
engine.execute(f"insert into _referencia (referencia, valor) values ('cnpj_qtde', '{qtde_cnpjs}')")

#print('Aplicando VACUUM para diminuir o tamanho da base--------------------------------', time.ctime())
#engine.execute('VACUUM')
#print('Aplicando VACUUM-FIM-------------------------------', time.ctime())

# print('compactando... ', time.ctime())
# with zipfile.ZipFile(cam + '.7z', 'w',  zipfile.ZIP_DEFLATED) as zipf:
#     zipf.write(cam, os.path.split(cam)[1])    
# print('compactando... FIM ', time.ctime())

# import py7zr
# print(time.ctime(), 'compactando... ')
# with py7zr.SevenZipFile(cam + '.7z', 'w') as archive:
#     archive.writeall(cam, os.path.split(cam)[1])
# print(time.ctime(), 'compactando... Fim')

print('-'*20)
print(f'Foi criado o arquivo {cam}, com a base de dados no formato SQLITE.')
print('Qtde de estabelecimentos (matrizes e fiiais):', engine.execute('SELECT COUNT(*) FROM estabelecimento').fetchone()[0])


print('FIM!!!', time.asctime())