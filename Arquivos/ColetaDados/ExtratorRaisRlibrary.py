# No RStudio instalar as seguintes bibliotecas:
# install.packages("remotes")
# remotes::install_github("ManuelHentschel/vscDebugger")

# Instalar o R no marketplacer do Visual Studio Code
# R, R Tools e R Debugger

# Instalar e carregar os pacotes necessários
install.packages("readr")
library(readr)

# Baixar o arquivo .7z
download.file("ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/2024/202401/CAGEDMOV202401.7z",
              destfile = "diretório/CAGEDMOV202401.7z",
              mode = "wb")

# Para descompactar o arquivo .7z, você precisará do pacote 'archive' ou de uma ferramenta externa como o 7-Zip.
# Se você tiver o pacote 'archive', instale e use o seguinte código para descompactar:

install.packages("archive")
library(archive)

# Instale o pacote R.utils se ainda não estiver instalado
install.packages("R.utils")
library(R.utils)

# Extraia o arquivo .7z na mesma pasta
archive_extract("diretório/CAGEDMOV202401.7z", dir = "diretório")

