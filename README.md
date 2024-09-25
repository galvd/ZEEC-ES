# Zoneamento Ecológico-Econômico Costeiro do Estado do Espírito Santo para 2025 (ZEEC-ES)

Este projeto em Python visa extrair e processar dados públicos para o Zoneamento Ecológico-Econômico Costeiro do Estado do Espírito Santo para 2025 (ZEEC-ES). O script coleta informações de diversas fontes e gera arquivos no formato Parquet para análise e integração em um banco de dados SQLite.

## Fontes de Dados

Os dados são extraídos das seguintes bases e relatórios:

- **CAGED**: Cadastro Geral de Empregados e Desempregados.
- **RAIS**: Relação Anual de Informações Sociais.
- **Censo 2022**:
  - Abastecimento de Água
  - Acesso a Saneamento
  - Demografia
- **Internet Banda Larga**:
  - Quantidade e Densidade de Acessos.
- **INEP**:
  - Notas ENEM
  - Escolas de Educação Básica
  - Censo da Educação Superior
- **PIB Municipal**: Produto Interno Bruto dos Municípios.
- **Transferências de Recursos**:
  - FPM (Fundo de Participação dos Municípios)
  - FEX (Fundo de Exportação)
  - FUNDEB (Fundo de Manutenção e Desenvolvimento da Educação Básica)
- **Dados Públicos de CNPJs**: Cadastro Nacional da Pessoa Jurídica.

## Obtenção dos Dados

- **Base dos Dados**: Dados obtidos via Base dos Dados ([basedosdados.org](https://basedosdados.org/)). Utiliza a biblioteca `basedosdados` para enviar queries SQL ao BigQuery e retornar os dados em DataFrames do pandas.
- **Tesouro Nacional**: Dados de transferências municipais obtidos via site do Tesouro Nacional ([tesourotransparente.gov.br](https://www.tesourotransparente.gov.br/ckan/dataset/3b5a779d-78f5-4602-a6b7-23ece6d60f27)).
- **Dados Abertos**: Dados recentes de CNPJs obtidos através do site Dados Abertos ([200.152.38.155](http://200.152.38.155/CNPJ/dados_abertos_cnpj)).

## Pré-requisitos

Certifique-se de ter os seguintes requisitos instalados:

- **Python 3.9** (preferencialmente em venv)
- Bibliotecas Python:
  - `pandas`
  - `dask`
  - `wget`
  - `basedosdados`

## Processamento dos Dados

O processamento dos dados pode levar até 8 horas para ser concluído, principalmente ao coletar os últimos 4 meses de dados disponíveis. O restante do histórico a partir de 2021 é obtido via Base dos Dados. 

Os dados são processados e salvos no formato Parquet. Posteriormente, esses arquivos Parquet serão integrados em um banco de dados SQLite (a ser desenvolvido).

### Arquitetura do Projeto

- **`main.py`**: Script principal que coordena a extração e processamento dos dados. Chama funções de extração específicas para cada tipo de dado e organiza o fluxo de trabalho.
- **`ToolsColeta.py`**: Contém funções e classes para coleta e processamento inicial dos dados.
- **`ExtratorRais.py`**: Funções para extrair dados da RAIS.
- **`ExtratorCaged.py`**: Funções para extrair dados do CAGED.
- **`ExtratorEnem.py`**: Funções para extrair dados do ENEM.
- **`ExtratorEdubase.py`**: Funções para extrair dados da Educação Básica e Superior.
- **`ExtratorPib.py`**: Funções para extrair dados do PIB Municipal.
- **`ExtratorIes.py`**: Funções para extrair dados das Instituições de Ensino Superior e Cursos Superiores.
- **`ExtratorCNPJ.py`**: Funções para extrair dados de CNPJs.
- **`ExtratorConectividade.py`**: Funções para extrair dados sobre conectividade à internet.
- **`ExtratorCenso.py`**: Funções para extrair dados do Censo (Água, Esgoto, População, Alfabetizados).
- **`ExtratorTransferencias.py`**: Funções para extrair dados de transferências municipais.

## Observações

A soma dos dados coletados para todo o Brasil pode ultrapassar 1TB. No entanto, o escopo do relatório está limitado aos municípios da costa do Espírito Santo. Estima-se que o volume total de dados para esta região seja aproximadamente 10GB.

Para mais detalhes sobre a implementação e funcionamento de cada script, consulte a documentação correspondente dos arquivos Python no repositório.


## Problemas recorrentes
Ainda não identificados.


## Histórico de versões
Em construção.

versão 0.1 (setembro/2024)
- primeira versão

  
