from __future__ import annotations
import json
import sys
with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])
from os import getcwd, chdir
from datetime import datetime


from Arquivos.ColetaDados.ExtratorRais import extrair_rais
from Arquivos.ColetaDados.ExtratorCaged import extrair_caged
from Arquivos.ColetaDados.ExtratorEnem import extrair_enem
from Arquivos.ColetaDados.ExtratorEdubase import extrair_edu_base
from Arquivos.ColetaDados.ExtratorPib import extrair_pib_cidades
from Arquivos.ColetaDados.ExtratorIes import extrair_ies, extrair_cursos_sup
from Arquivos.ColetaDados.ExtratorCNPJ import extrair_empresas
from Arquivos.ColetaDados.ExtratorConectividade import extrair_internet_acs, extrair_internet_dens
from Arquivos.ColetaDados.ExtratorCenso import extrair_censo_agua, extrair_censo_esgoto, extrair_censo_pop, extrair_censo_alfabetizados
from Arquivos.ColetaDados.ExtratorTransferencias import extrair_transferencias_fex, extrair_transferencias_fpm, extrair_transferencias_fundeb


beg = datetime.now()


proj_dir = getcwd()
anos_relatorio = [2021, 2022]
uf = ["ES"]
cidades_zeec = {
    'Marataízes': '3203320',
    'Itapemirim': '3202900',
    'Cachoeiro de Itapemirim': '3201209',
    'Presidente Kennedy': '3204301',
    'Piúma': '3204202',
    'Anchieta': '3200409',
    'Guarapari': '3202405',
    'Viana': '3205100',
    'Vila Velha': '3205209',
    'Cariacica': '3201308',
    'Vitória': '3205308',
    'Serra': '3205001',
    'Fundão': '3202207',
    'Aracruz': '3200607',
    'Linhares': '3203205',
    'Sooretama': '3205019',
    'Jaguaré': '3203056',
    'São Mateus': '3204906',
    'Conceição da Barra': '3201506'
}

limit = "LIMIT 10" # argumento opcional para teste


### Extração dos dados

## RAIS

# extrair_rais(
#     anos = anos_relatorio,
#     cidades = cidades_zeec.keys(),
#     save_dir = proj_dir,
#     ufs = uf,
#     limit = limit
#     )


# ## CAGED

# extrair_caged(
#     anos = anos_relatorio,
#     cidades = cidades_zeec.keys(),
#     save_dir = proj_dir,
#     ufs = uf,
#     mes = 12,
#     limit = limit
#     )


# ## Censo

# extrair_censo_pop(
#     cidades = cidades_zeec.keys(),
#     save_dir = proj_dir,
#     limit = limit
#     )

# extrair_censo_agua(
#     cidades = cidades_zeec.keys(),
#     save_dir = proj_dir,
#     limit = limit
#     )

# extrair_censo_esgoto(
#     cidades = cidades_zeec.keys(),
#     save_dir = proj_dir,
#     limit = limit
#     )

# extrair_censo_alfabetizados(
#     cidades = cidades_zeec.keys(),
#     save_dir = proj_dir,
#     limit = limit
#     )


# # PIB Municípios

# extrair_pib_cidades(
#     anos = anos_relatorio,
#     cidades = cidades_zeec.keys(),
#     save_dir = proj_dir,
#     limit = limit
#     )


# ## Acesso à Internet Banda Larga

# extrair_internet_acs(
#     anos = anos_relatorio,
#     cidades = cidades_zeec.keys(),
#     save_dir = proj_dir,
#     ufs = uf,
#     mes = 12,
#     limit = limit
#     )

# extrair_internet_dens(
#     anos = anos_relatorio,
#     cidades = cidades_zeec.keys(),
#     save_dir = proj_dir,
#     ufs = uf,
#     mes = 12,
#     limit = limit
#     )


# ## Ensino Superior
# extrair_cursos_sup(
#     anos = anos_relatorio,
#     cidades = cidades_zeec.keys(),
#     save_dir = proj_dir,
#     ufs = uf,
#     limit = limit
#     )

# extrair_ies(
#     anos = anos_relatorio,
#     cidades = cidades_zeec.keys(),
#     save_dir = proj_dir,
#     ufs = uf,
#     limit = limit
#     )


# ## Educação Básica
# # # municípios indexados pelo código ibge
# extrair_edu_base(
#     anos = anos_relatorio,
#     cidades = cidades_zeec.values(),
#     save_dir = proj_dir,
#     ufs = uf,
#     limit = limit
#     )


## CNPJs
extrair_empresas(
    anos = anos_relatorio,
    cidades = cidades_zeec.values(),
    save_dir = proj_dir,
    ufs = uf,
    limit = limit
    )


# ## ENEM
# # municípios indexados pelo código ibge
# extrair_enem(
#     anos = anos_relatorio,
#     cidades = cidades_zeec.values(),
#     save_dir = proj_dir,
#     ufs = uf,
#     limit = limit
#     )

## Transferëncias Municipais

# extrair_transferencias_fex(cidades= cidades_zeec.keys(), 
#                            save_dir = proj_dir,
#                            ufs = uf)

# extrair_transferencias_fundeb(cidades= cidades_zeec.keys(), 
#                            save_dir = proj_dir,
#                            ufs = uf)

# extrair_transferencias_fpm(cidades= cidades_zeec.keys(), 
#                            save_dir = proj_dir,
#                            ufs = uf)

print("Processamento dos dados completo")

end_time = datetime.now()
time_length = end_time - beg
print(f'Finished at {end_time}.\nTime elapsed: {time_length}.')