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

-1. Realizar o tratamento de dados da base VRA, que possui arquivos CSV como origem, tratar as colunas para padrão "snake_case" de nomenclatura que na origem estão no formato "KebabCase", em adição foi feito o tratamento dos caracteres especiais para as colunas e substituição dos valores "NULL" para string vazia;

-2. Realizar o tratamento de dados da base AIR_CIA, que possui arquivos origem em formato JSON, similarmente foi requisitado para converter a nomenclatura das colunas para "snake_case", também foi feito o tratamento dos caracteres especiais, e em adição a coluna "ICAO IATA" foi separada em duas "icao" e "iata" com seus respectivos dados, utilizando o a metodologia de "slicing";

-3. Realizar a extração de dados da API [https://rapidapi.com/Active-api/api/airport-info/] que possui as informações dos aerodromos, e salvar o resultado dessa extração de dados da API.

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

Para a realização desta task é possivel ver o desenvolvimento passo a passo no notebook [VRA_tratamento.ipynb](https://github.com/matheusbudin/big-data-airlines/blob/development/jupyter_notebooks_scripts/VRA_tratamento.ipynb) , na qual, como já foi dito anteriormente, realizamos o tratamento de snake case para as colunas, retiramos os caracteres especiais pois podem ocasionar problemas quando essas tabelas forem usadas em um ```JOIN``` da ```task-4``` e exportamos o resultado para um arquivo parquet com compressão snappy.

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

Para a realização da task-2 é possivel ver o desenvolvimento passo a passo no notebook [AIR_CIA_tratamento.ipynb](https://github.com/matheusbudin/big-data-airlines/blob/development/jupyter_notebooks_scripts/AIR_CIA_tratamento.ipynb). Onde foi realizada a converção da nomenclatura das colunas para snake_case e a operação de slicing para a coluna ```ICAO IATA``` que originou duas colunas separadas ```icao e iata ```. A seguir temos os códigos que realizam, respectivamente, as transformações:

-Conversão para snake_case:
```

# conversao para snake case
for column in df_air_cia.columns:
    new_column_name = column.lower().replace(" ", "_").replace("-", "_")
    df_air_cia = df_air_cia.withColumnRenamed(column, new_column_name)

```

- Separação das colunas ```icao e iata```:

```
# Operacao de slice da coluna 'icao_iata' para 'icao' e 'iata'
df_air_cia = df_air_cia.withColumn("icao_iata", split("icao_iata", " "))
df_air_cia = df_air_cia.withColumn("icao", df_air_cia["icao_iata"].getItem(0))
df_air_cia = df_air_cia.withColumn("iata", when(df_air_cia["icao_iata"].getItem(1).isNotNull(), df_air_cia["icao_iata"].getItem(1)))

# trazendo as colunas 'icao' e 'iata' obedecendo a ordem do dataframe original
df_air_cia = df_air_cia.select("razao_social", "icao", "iata", "cnpj", "atividades_aereas", "endereco_sede", "telefone", "e_mail", "decisao_operacional", "data_decisao_operacional", "validade_operacional")
# substitui os valores nulos por uma string vazia
df_air_cia = df_air_cia.na.fill(' ')
# preview final do data frame antes de exporta-lo
df_air_cia.show(truncate=False)
```

PRINT DO RESULTADO FINAL (.SHOW() DO DATAFRAME TRATADO)

Após os tratamentos, o arquivo referente aos dados da base AIR_CIA está localizado na pasta: [AIR_CIA_tratamento.ipynb](https://github.com/matheusbudin/big-data-airlines/tree/development/converted_data/AIR_CIA)


## Task-3

Para a realização desta task é possivel ver o desenvolvimento passo a passo no notebook  [dimensao_api.ipynb](https://github.com/matheusbudin/big-data-airlines/tree/development/converted_data/AIR_CIA). Primeiramente foi necessário criar um registro com todos os códigos ```icao``` distintos, presentes tanto para ```icao_aerodromo_origem``` quanto para ```icao_aerodromo_destino```, conforme podemos ver a seguir:

```
# cria um dataframe com todos os codigos 'icao' encontrados em cada uma das
# colunas 'icao_aerodromo_origem' e 'icao_aerodromo_destino'
unique_codes = (
    df_vra.select("icao_aerodromo_origem")
    .union(df_vra.select("icao_aerodromo_destino"))
    .distinct()
)

```

Em seguida, levamos esses dados de códigos ```icao``` para trazer o retorno referente à cada um para formarmos uma ```tabela_dimensao``` dessa API que vai ser usada na task4. A seguir temos os códigos de extração de dados da APi seguido do tratamento do dataframe para trazer o resultado como precisamos (conforme pode ser visto na print a seguir também)

```

# popula o dataframe com as respostas retornadas da API
for row in unique_codes.collect():
    icao_code = row["icao"]
    api_response = fetch_data(icao_code)
    # foi necessario forcar um numero de colunas para evitar o erro 'mismatch number of columns'
    if api_response:
        api_dataframe = api_dataframe.union(spark.createDataFrame([[None, None, icao_code, None, None, None, None, None, None, None, None, None, None, None, None, None, None, api_response]], schema=schema))

# adiciona uma coluna no final para mostrar a correspondencia que deu match e que tem origem no df_vra
api_dataframe = api_dataframe.withColumn("icao_code_source", lit("icao_code"))

##formatando o dataframe para o modelo que precisamos:

# Extracao dos dados presente no JSON em cada linha da coluna 'website'
expanded_data = api_dataframe.withColumn("website_data", from_json(col("website"), json_schema))

# filtramos o data frame para retornar somente as colunas nao nulas
expanded_data = expanded_data.filter(col("website_data.id").isNotNull())

# forçamos a selecao das colunas para garatir o schema necessário
expanded_data = expanded_data.select(
    "website_data.id",
    "website_data.iata",
    "website_data.icao",
    "website_data.name",
    "website_data.location",
    "website_data.street_number",
    "website_data.street",
    "website_data.city",
    "website_data.county",
    "website_data.state",
    "website_data.country_iso",
    "website_data.country",
    "website_data.postal_code",
    "website_data.phone",
    "website_data.latitude",
    "website_data.longitude",
    "website_data.uct",
    "website_data.website",
    "icao_code_source"
)

# finalmente temos o dataframe que desejávamos desde o inicio.
expanded_data.show(truncate=False)

```
[PRINT DO RESULTADO]

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