from __future__ import annotations
import json
import sys
with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])
from os import getcwd
from datetime import datetime

from Arquivos.ColetaDados.Tools import MainParameters
from Arquivos.ColetaDados.ExtratorRais import extrair_rais
from Arquivos.ColetaDados.ExtratorCaged import extrair_caged
from Arquivos.ColetaDados.ExtratorEnem import extrair_enem
from Arquivos.ColetaDados.ExtratorEdubase import extrair_edu_base
from Arquivos.ColetaDados.ExtratorPib import extrair_pib_cidades
from Arquivos.ColetaDados.ExtratorIes import extrair_ies, extrair_cursos_sup
from Arquivos.ColetaDados.ExtratorCNPJ import extrair_empresas_bd, extrair_empresas_dict
from Arquivos.ColetaDados.ExtratorConectividade import extrair_internet_acs, extrair_internet_dens
from Arquivos.ColetaDados.ExtratorCenso import extrair_censo_agua, extrair_censo_esgoto, extrair_censo_pop, extrair_censo_alfabetizados
from Arquivos.ColetaDados.ExtratorTransferencias import extrair_transferencias_fex, extrair_transferencias_fpm, extrair_transferencias_fundeb


beg = datetime.now()


proj_dir = getcwd()
parametros = MainParameters()
anos_relatorio = parametros.anos_analise()
uf = parametros.ufs()
cod_ibge = parametros.cod_ibge()
nome_mun = parametros.nome_mun()


limit = "LIMIT 10" # argumento opcional para teste


### Extração dos dados

## RAIS

# extrair_rais(
#     anos = anos_relatorio,
#     cidades = nome_mun,
#     save_dir = proj_dir,
#     ufs = uf,
#     limit = limit
#     )


# ## CAGED

# extrair_caged(
#     anos = anos_relatorio,
#     cidades = nome_mun,
#     save_dir = proj_dir,
#     ufs = uf,
#     mes = 12,
#     limit = limit
#     )


# ## Censo

# extrair_censo_pop(
#     cidades = nome_mun,
#     save_dir = proj_dir,
#     limit = limit
#     )

# extrair_censo_agua(
#     cidades = nome_mun,
#     save_dir = proj_dir,
#     limit = limit
#     )

# extrair_censo_esgoto(
#     cidades = nome_mun,
#     save_dir = proj_dir,
#     limit = limit
#     )

# extrair_censo_alfabetizados(
#     cidades = nome_mun,
#     save_dir = proj_dir,
#     limit = limit
#     )


# # PIB Municípios

# extrair_pib_cidades(
#     anos = anos_relatorio,
#     cidades = nome_mun,
#     save_dir = proj_dir,
#     limit = limit
#     )


# ## Acesso à Internet Banda Larga

# extrair_internet_acs(
#     anos = anos_relatorio,
#     cidades = nome_mun,
#     save_dir = proj_dir,
#     ufs = uf,
#     mes = 12,
#     limit = limit
#     )

# extrair_internet_dens(
#     anos = anos_relatorio,
#     cidades = nome_mun,
#     save_dir = proj_dir,
#     ufs = uf,
#     mes = 12,
#     limit = limit
#     )


# ## Ensino Superior
# extrair_cursos_sup(
#     anos = anos_relatorio,
#     cidades = nome_mun,
#     save_dir = proj_dir,
#     ufs = uf,
#     limit = limit
#     )

# extrair_ies(
#     anos = anos_relatorio,
#     cidades = nome_mun,
#     save_dir = proj_dir,
#     ufs = uf,
#     limit = limit
#     )


# ## Educação Básica
# # # municípios indexados pelo código ibge
# extrair_edu_base(
#     anos = anos_relatorio,
#     cidades = cod_ibge,
#     save_dir = proj_dir,
#     ufs = uf,
#     limit = limit
#     )


## CNPJs
# extrair_empresas_bd(
#     anos = anos_relatorio,
#     cidades = cod_ibge,
#     save_dir = proj_dir,
#     ufs = uf,
#     limit = limit
#     )

## Dicionário de municípios com código IBGE e Código RF
# extrair_empresas_dict(
#     save_dir = proj_dir,
#     )

# ## ENEM
# municípios indexados pelo código ibge
# extrair_enem(
#     anos = anos_relatorio,
#     cidades = cod_ibge,
#     save_dir = proj_dir,
#     ufs = uf,
#     limit = limit
#     )

## Transferëncias Municipais

# extrair_transferencias_fex(cidades= nome_mun, 
#                            save_dir = proj_dir,
#                            ufs = uf)

# extrair_transferencias_fundeb(cidades= nome_mun, 
#                            save_dir = proj_dir,
#                            ufs = uf)

# extrair_transferencias_fpm(cidades= nome_mun, 
#                            save_dir = proj_dir,
#                            ufs = uf)

print("Processamento dos dados completo")

end_time = datetime.now()
time_length = end_time - beg
print(f'Finished at {end_time}.\nTime elapsed: {time_length}.')


