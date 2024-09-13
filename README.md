# Zoneamento Ecológico-Econômico da Zona Costeira do Estado do Espírito Santo para 2025 (ZEEC-ES)
Script em python para extrair os arquivos de dados públicos das seguintes bases de dados e relatórios:
- Caged
- Rais
- Censo 2022 (Abastecimento de água; Acesso a saneamento; Demografia)
-  Acessos e Densidade de Acessos à internet banda larga
-  INEP (notas Enem; Escolas de Educação Básica; Censo da Educação Superior)
-  PIB Municipal
-  Transferências de recursos para municípios (FPM; FEX; FUNDEB)
-  Dados públicos de CNPJs da Receita Federal

## Obtenção dos Dados públicos:
Os dados são obtidos (exceto Transferências e dados recentes de CNPJs - até a v0.7.1 do código) via Base dos Dados (https://basedosdados.org/). A lib da BD recebe queries em SQL, envia-as para o Big Query da plataforma e retorna os dados em pandas Dataframe. Os dados de transferências municipais foram obtidos via site do Tesouro Nacional (https://www.tesourotransparente.gov.br/ckan/dataset/3b5a779d-78f5-4602-a6b7-23ece6d60f27). Os dados recentes de CNPJs foram obtidos através do site Dados Abertos (http://200.152.38.155/CNPJ/dados_abertos_cnpj) <br><br>

## Pré-requisitos:
Python 3.9 ou posterior;<br>
Bibliotecas pandas, dask, wget, basedosdados.<br><br>

A soma das bases coletadas para todo o Brasil pode ultrapassar 1TB, entretanto, o escopo do relatório se limita aos municípios da costa do ES. (Até o momento, espera-se que os dados some aproximadamente 10GB)


## Processamento dos dados

<s>O download no site da Receita é lento, pode demorar várias horas, por isso o código tentará muitas vezes.<br>

O download e processamento dos dados da Receita pode demorar até 8h para ser concluído (coletando os últimos 4 meses de dados disponíveis, o resto do histórico de 2021 em diante é obtido via BD).

O código gera arquivos em parquet que serão psoteriormente transferidos para um db em SQLite (ainda não construído).<br>


## Problemas recorrentes
Ainda não identificados.



## Histórico de versões
Em construção.

versão 0.1 (setembro/2024)
- primeira versão
