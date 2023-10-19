# big-data-airlines - Eleflow

## Projeto - BigData Airlines

Este projeto foi originado de um desafio para a empresa Eleflow, a qual disponibilizou as fontes de dados que estão na pasta [src](https://github.com/matheusbudin/big-data-airlines/tree/development/src) deste projeto e foi requisitado realizar o tratamento das seguintes bases de dados: VRA (composta por arquivos CSV), AIR_CIA (composta por arquivos no formato JSON) e uma útlima base que foi chamada neste projeto de "dimensão API" que depende do resultado do tratamento da base VRA para retornar um arquivo tratado que vai ser a nossa dimensão com as características dos aerodromos. As tasks podem ser vizualidas no tópico "TO-DO" ou em detalhes no readme localizado a seguir: [readme-tasks](https://github.com/matheusbudin/big-data-airlines/tree/development/src#readme).

## Informações importante:
- O ambiente de prototipagem e desenvolvimento dos códigos escolhido foi o Google Colab, por possuir uma vantagem de ser praticamente "plug and play" basta ter uma conta @gmail e começar a usar. Vale salientar que para usar o Spark nesse ambiente, é necessário fazer a instalação por meio do comando :

``` !pip install pyspark ```

- Outro ponto importante para destacar é a proteção da chave da api que foi feita utilizando variáveis de ambiente, para tal foi necessario utilizar a biblioteca: 

```!pip install python-decouple```

- seguida dos comandos: ```!touch .env"``` para criar o arquivo .env no Colab seguido da configuração para uso desta chave: 

```
from decouple import config

API_KEY = config('API_KEY')
```
- Por fim, ao analisar os arquivos dos notebooks (.ipynb) localizados na pasta: [jupyter_notebooks](https://github.com/matheusbudin/big-data-airlines/tree/development/jupyter_notebooks_scripts) , pode-se notar que após o tratamento dos dados, é feita a exportação (salvar) utilizando o ```coalesce(1)``` para consolidar as partições em um arquivo e salvar, isso foi necessário pois ao particionar por 'ano' o ambiente do ```Google Colab``` ficou extremamente lento. Entretanto, é importante enfatizar que em um cenário de ```big data``` o particionamento é extremamente necessário e as regras devem ser de acordo com a definição da ```área de negócios```.

### TO-DO:

-1. Realizar o tratamento de dados da base VRA, que possui arquivos CSV como origem, trata as colunas para padrão "snake_case" de nomenclatura, em adição foi feito o tratamento dos caracteres especiais para as colunas e substituição dos valores "NULL" para string vazia;

-2. Realizar o tratamento de dados da base AIR_CIA, que possui arquivos origem em formato JSON, similarmente foi requisitado para trocar a nomenclatura das colunas que estava no padrão "kebabCase" passando para "snake_case", também foi feito o tratamento dos caracteres especiais, e em adição a coluna "ICAO IATA" foi separada em duas "icao" e "iata" com 
seus respectivos dados, utilizando o a metodologia de "slicing";

-3. Realizar a extração de dados da API [LINK DA API] que possui as informações dos aerodromos, e salvar o resultado dessa extração de dados da API.

-4. Criação das Views (tabelas) priorizando o uso do SQL, que no caso foi utilziado dentro do próprio spark por já possuir essa feature built-in e também por utilziar o ambiente do Google Colab, os arquivos finais, pós criação das Views, foram exportados em formato '.parquet' e '.json' e estão localizados na pasta: "resultados_query_exportados".

  -4.1 -Para cada companhia aérea trazer a rota mais utilizada com as seguintes informações:
        - Razão social da companhia aérea
        - Nome Aeroporto de Origem
        - ICAO do aeroporto de origem
        - Estado/UF do aeroporto de origem
        - Nome do Aeroporto de Destino
        - ICAO do Aeroporto de destino
        - Estado/UF do aeroporto de destino

  -4.2 -Para cada aeroporto trazer a companhia aérea com maior atuação no ano com as seguintes informações:
        - Nome do Aeroporto
        - ICAO do Aeroporto
        - Razão social da Companhia Aérea
        - Quantidade de Rotas à partir daquele aeroporto
        - Quantidade de Rotas com destino àquele aeroporto
        - Quantidade total de pousos e decolagens naquele aeroporto



# Resolução das tasks:

## Task-1

Para a realização desta task é possivel ver o desenvolvimento passo a passo no notebook [VRA_tratamento.ipynb](hhttps://github.com/matheusbudin/big-data-airlines/blob/development/jupyter_notebooks_scripts/VRA_tratamento.ipynb) , na qual, como já foi dito anteriormente, realizamos o tratamento de snake case para as colunas, retiramos os caracteres especiais pois podem ocasionar problemas quando essas tabelas forem usadas em um ```JOIN``` da ```task-4``` e exportamos o resultado para um arquivo parquet com compressão snappy.

A seguir temos o trecho do código retirado do notebook "VRA_tratamento.ipynb" o qual é responsável por realizar o tratamento de snake_case requierido na task, lembrando que inicialmente no arquivo ```de origem``` as colunas estavam no padrão ```KebabCase``` então definimos uma função para realizar tal conversão:


```
# converter para snake case, usamos o regex abaixo:
def kebab_to_snake(column_name):
    return re.sub(r'(?<=[a-z])(?=[A-Z0-9])|(?<=[0-9])(?=[A-Z])', '_', column_name).lower()

# retorna a lista de colunas do dataframe
columns = df_vra.columns

# aplica a funcao para snake_case acima coluna a coluna
for column in columns:
    new_column_name = kebab_to_snake(column)
    df_vra = df_vra.withColumnRenamed(column, new_column_name)

# verificacao da transformacao
df_vra.show()

```
PRINT DO RESULTADO FINAL

Como podemos ver do resultado acima as colunas ```icaoaerodromo_origem , icaoaerodromo_destino e icaoempresa_aerea``` não foram transformadas pelo Regex da função, mas como são apenas 3 colunas foi mais fácil converte-las usando o spark puro conforme código abaixo:

```
#tranformacoes adicionais

df_vra = df_vra.withColumnRenamed("icaoaerodromo_destino", "icao_aerodromo_destino")
df_vra = df_vra.withColumnRenamed("icaoaerodromo_origem", "icao_aerodromo_origem")
df_vra = df_vra.withColumnRenamed("icaoempresa_aerea", "icao_empresa_aerea")



df_vra.show(truncate=False)

```

Por fim, como tratamento adicional para essa task, foi feita a conversão de alguns data types das tabelas, pois arquivos csv fazem o spark assumir o schema como todas as colunas sendo do tipo ```string```. Porém, observando os resultados dos previews (```dataframe.show()```) podemos notar que alguns dados seriam ```integer``` ou do tipo ```timestamp```. Dessa forma realizamos a transformação adicional:

```
# Conversao das colunas de data pata timestamp
# (conforme observação da saida dos dados) vista nos previews que mostra
# a precisao de hora, minuto e segundo:
df_vra = df_vra.withColumn("chegada_prevista", col("chegada_prevista").cast(TimestampType()))
df_vra = df_vra.withColumn("chegada_real", col("chegada_prevista").cast(TimestampType()))
df_vra = df_vra.withColumn("partida_prevista", col("partida_prevista").cast(TimestampType()))
df_vra = df_vra.withColumn("partida_real", col("partida_real").cast(TimestampType()))

#converte 'numero_voo' para tipo inteiro
df_vra = df_vra.withColumn("numero_voo", col("numero_voo").cast(IntegerType()))

```
Tendo o Schema resultante abaixo para os dados de VRA:

PRINT DO SCHEMA.

PRINT DO RESULTADO FINAL PARA EXPORTAR

ESPECIFICAR QUAL PASTA CONTEM O ARQUIVO EXPORTADO


## Task-2
 [DESCREVER A TASK IGUAL FOI FEITA NA TASK 1]
  Para a realização desta task é possivel ver o desenvolvimento passo a passo no notebook "AIR_CIA_tratamento.ipynb" [TODOS ESSES COLOCAR O LINK]

COLOCAR TRECHO DO CODIGO COM TRATAMENTO SNAKE_CASE E OUTRO COM CODIGO DO SLICING + NOENCLATURA ESPECIFICA DE ALGUMAS COLUNAS

PRINT DO RESULTADO FINAL (.SHOW() DO DATAFRAME TRATADO)

ESPECIFICAR QUAL PASTA CONTEM O ARQUIVO EXPORTADO


## Task-3

Para a realização desta task é possivel ver o desenvolvimento passo a passo no notebook "dimensao_api.ipynb" [TODOS ESSES COLOCAR O LINK]

COLOCAR O TRATAMENTO DO CODIGO QUE ORIGINOU A LISTA DOS CODIGOS UNICOS E DISTINTOS PARA CONSULTAR A API

COM O PRINT DISSO AQUI NO RESULTADO:

COLOCAR QUE FOI UTILIZADO O METODO HTTP.CLIENT POIS O REQUEST OCASINOU PROBLEMAS, DESCREVER COMO A API RETORNOU DE PRIMEIRA E COLOCAR O CODIGO PARA DEIXAR O DATAFRAME DO JEITO Q A GENTE PRECISAVA JUNTO DA PRINT FINAL


## Task-4

Primeiramente carregamos os arquivos que foram tratados das tarefas anteriores, e criamos um data frame para cada uma delas, juntamente com a sua respectiva temp view, conforme é mostrado no código a seguir:

''' codigo de carregar os DFs, e criacao das temp views '''

Dessa forma foi possivel iniciar as querys em spark.SQL para responder as perguntas das áreas de negócios.

-4.1 -Para cada companhia aérea trazer a rota mais utilizada com as seguintes informações:
    - Razão social da companhia aérea
    - Nome Aeroporto de Origem
    - ICAO do aeroporto de origem
    - Estado/UF do aeroporto de origem
    - Nome do Aeroporto de Destino
    - ICAO do Aeroporto de destino
    - Estado/UF do aeroporto de destino

    ''' código da query'''

    print do resultado da query (primeiro resultado)

  Entretanto como o resultado anterior nao tras os valores unicos por empresa, foi ajustada a query para poder ter esse resultado, conforme o codigo abaixo:


  ''' codigo da query '''
  PRINT RESULTADO DA QUERY.



-4.2- seguir modelo da task 4.1




# Extras

Desenho de arquitetura medaliao com delta
Colocar o texto aqui