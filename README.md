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

Para a realização da task-2 é possivel ver o desenvolvimento passo a passo no notebook [AIR_CIA_tratamento.ipynb](https://github.com/matheusbudin/big-data-airlines/blob/development/jupyter_notebooks_scripts/AIR_CIA_tratamento.ipynb). Onde foi realizada a conversão da nomenclatura das colunas para snake_case e a operação de slicing para a coluna ```ICAO IATA``` que originou duas colunas separadas ```icao e iata ```. A seguir temos os códigos que realizam, respectivamente, as transformações:

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

# finalmente temos o dataframe no formato desejado
expanded_data.show(truncate=False)

```
[PRINT DO RESULTADO]

## Task-4.1
Para vizualizar o arquivo jupyter notebook que contempla a criação das views de maneira completa, favor acessar no link a seguir: [create_views.ipynb](https://github.com/matheusbudin/big-data-airlines/blob/development/jupyter_notebooks_scripts/create_views_sql.ipynb)

Primeiramente carregamos os arquivos que foram tratados das tarefas anteriores, e criamos um data frame para cada uma delas, juntamente com a sua criação da sua respectiva temp view que será usada nas operações ```JOIN```, conforme é mostrado no código a seguir:

``` 
# Leitura dos parquets que sao resultados das tasks anteriores
df_vra = spark.read.parquet("/content/VRA")
df_air_cia = spark.read.parquet("/content/AIR_CIA")
df_api = spark.read.parquet("/content/API")

#criacao das tempviews
df_vra.createOrReplaceTempView("vra")
df_air_cia.createOrReplaceTempView("air_cia")
df_api.createOrReplaceTempView("api")
```

Dessa forma foi possivel iniciar as querys em spark.SQL (onde foi priorizado o uso de ```CTEs``` para performance e organização otimizados) para responder as perguntas das áreas de negócios.

-A primeira Query tem como objetivo criar uma view trazendo a rota mais utilizada por companhia aérea:

```

df_rota_mais_utilizada = spark.sql("""

WITH RotasMaisUtilizadas AS (
        SELECT
            vo.icao_empresa_aerea AS icao_empresa_aerea,
            vo.icao_aerodromo_origem AS icao_origem,
            vo.icao_aerodromo_destino AS icao_destino,
            COUNT(*) AS total_rotas
        FROM vra vo
        GROUP BY vo.icao_empresa_aerea, vo.icao_aerodromo_origem, vo.icao_aerodromo_destino
    ),
    RotasMaisUtilizadasRank AS (
        SELECT
            icao_empresa_aerea,
            icao_origem,
            icao_destino,
            total_rotas,
            ROW_NUMBER() OVER (PARTITION BY icao_empresa_aerea ORDER BY total_rotas DESC) AS rank
        FROM RotasMaisUtilizadas
    )
    SELECT
        ac.razao_social AS razao_social_companhia,
        r.icao_empresa_aerea,
        vo.icao_aerodromo_origem AS icao_origem,
        ap_origem.state AS estado_origem,
        vo.icao_aerodromo_destino AS icao_destino,
        ap_destino.state AS estado_destino,
        ap_origem.name AS nome_aeroporto_origem,
        ap_destino.name AS nome_aeroporto_destino
    FROM RotasMaisUtilizadasRank r
    JOIN vra vo ON r.icao_empresa_aerea = vo.icao_empresa_aerea
    JOIN air_cia ac ON r.icao_empresa_aerea = ac.icao
    JOIN api ap_origem ON r.icao_origem = ap_origem.icao
    JOIN api ap_destino ON r.icao_destino = ap_destino.icao
    WHERE r.rank = 1

""")
df_rota_mais_utilizada.createOrReplaceTempView("rota_mais_utilizada")
```

-O resultado dessa query pode ser mostrado a seguir:
[PRINTTT]

Como podemos ver, ainda restam ajustes, queremos que seja impresso apenas um registro por companhia, dessa forma, utilizamos a TempView da query anterior e reorganizamos a consulta:

```

# Consulta para trazer o registro mais frequente por "icao_empresa_aerea" com código distinto
df_rota_mais_utilizada_por_empresa = spark.sql( """
    WITH RotasMaisUtilizadas AS (
        SELECT
            vo.icao_empresa_aerea AS icao_empresa_aerea,
            vo.icao_aerodromo_origem AS icao_origem,
            vo.icao_aerodromo_destino AS icao_destino,
            COUNT(*) AS total_rotas
        FROM vra vo
        GROUP BY vo.icao_empresa_aerea, vo.icao_aerodromo_origem, vo.icao_aerodromo_destino
    ),
    RotasMaisUtilizadasRank AS (
        SELECT
            r.*,
            ROW_NUMBER() OVER (PARTITION BY r.icao_empresa_aerea ORDER BY r.total_rotas DESC) AS rank
        FROM RotasMaisUtilizadas r
    )
    SELECT DISTINCT
        ac.razao_social AS razao_social_companhia,
        r.icao_empresa_aerea,
        r.icao_origem,
        ap_origem.state AS estado_origem,
        r.icao_destino,
        ap_destino.state AS estado_destino,
        ap_origem.name AS nome_aeroporto_origem,
        ap_destino.name AS nome_aeroporto_destino
    FROM RotasMaisUtilizadasRank r
    JOIN rota_mais_utilizada rota ON r.icao_empresa_aerea = rota.icao_empresa_aerea
    JOIN air_cia ac ON r.icao_empresa_aerea = ac.icao
    JOIN api ap_origem ON r.icao_origem = ap_origem.icao
    JOIN api ap_destino ON r.icao_destino = ap_destino.icao
    WHERE r.rank = 1
""")

# Exibir o resultado
df_rota_mais_utilizada_por_empresa.show(truncate=False)

```
Agora temos o resultado desejado, como podemos ver na print abaixo:

[PRINT]

# task-4.2: seguir modelo da task 4.1
- Para responder a pergunta de negócio contida nesta task, vamos reutilizar os dataframes e tempviews já carregados na tarefa anterior, essa é a razão de usarmos o mesmo jupyter notebook para a criação das duas ```Views```. Logo, para criar uma View que sintetize ```a área de maior atuação no ano por companhia aérea``` temos a seguinte query:


```
# CTE para calcular o numero de rotas por linha área de cada aeroporto
df_rotas_saindo = spark.sql("""
    WITH RotasSaindo AS (
        SELECT
            ap.icao AS icao_aeroporto,
            vo.icao_empresa_aerea AS icao_empresa_aerea,
            COUNT(*) AS qtd_rotas_saindo
        FROM vra vo
        INNER JOIN api ap ON vo.icao_aerodromo_origem = ap.icao
        GROUP BY ap.icao, vo.icao_empresa_aerea
    )
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY icao_aeroporto ORDER BY qtd_rotas_saindo DESC) AS rank_saindo
    FROM RotasSaindo
""")
df_rotas_saindo.createOrReplaceTempView("rotas_saindo")

# CTE para calcular o numero de rotas para cada companhia aera
# chegando em cada aeroporto

df_rotas_chegando = spark.sql("""
    WITH RotasChegando AS (
        SELECT
            ap.icao AS icao_aeroporto,
            vo.icao_empresa_aerea AS icao_empresa_aerea,
            COUNT(*) AS qtd_rotas_chegando
        FROM vra vo
        INNER JOIN api ap ON vo.icao_aerodromo_destino = ap.icao
        GROUP BY ap.icao, vo.icao_empresa_aerea
    )
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY icao_aeroporto ORDER BY qtd_rotas_chegando DESC) AS rank_chegando
    FROM RotasChegando
""")
df_rotas_chegando.createOrReplaceTempView("rotas_chegando")

# CTE para calcular o total de pousos e decolagens por aeroporto

df_pousos_decolagens = spark.sql("""
    WITH PousosDecolagens AS (
        SELECT
            ap.icao AS icao_aeroporto,
            COUNT(*) AS qtd_pousos_decolagens
        FROM vra vo
        INNER JOIN api ap ON vo.icao_aerodromo_destino = ap.icao OR vo.icao_aerodromo_origem = ap.icao
        GROUP BY ap.icao
    ),
    RotasSaindo AS (
        SELECT
            ap_saida.icao AS icao_aeroporto,
            COUNT(DISTINCT vo_saida.numero_voo) AS qtd_rotas_saindo
        FROM vra vo_saida
        INNER JOIN api ap_saida ON vo_saida.icao_aerodromo_origem = ap_saida.icao
        GROUP BY ap_saida.icao
    ),
    RotasEntrando AS (
        SELECT
            ap_entrada.icao AS icao_aeroporto,
            COUNT(DISTINCT vo_entrada.numero_voo) AS qtd_rotas_entrando
        FROM vra vo_entrada
        INNER JOIN api ap_entrada ON vo_entrada.icao_aerodromo_destino = ap_entrada.icao
        GROUP BY ap_entrada.icao
    )
    SELECT
        pd.icao_aeroporto,
        pd.qtd_pousos_decolagens,
        COALESCE(rs.qtd_rotas_saindo, 0) AS qtd_rotas_saindo,
        COALESCE(re.qtd_rotas_entrando, 0) AS qtd_rotas_entrando
    FROM PousosDecolagens pd
    LEFT JOIN RotasSaindo rs ON pd.icao_aeroporto = rs.icao_aeroporto
    LEFT JOIN RotasEntrando re ON pd.icao_aeroporto = re.icao_aeroporto
    ORDER BY pd.qtd_pousos_decolagens DESC
""")
df_pousos_decolagens.createOrReplaceTempView("pousos_decolagens")



# Combinando as CTE's para responder as perguntas de negócio da task 2:
df_resultado = spark.sql("""
    SELECT DISTINCT
        ap.name AS nome_aeroporto,
        ap.icao AS icao_aeroporto,
        rs.icao_empresa_aerea AS icao_empresa_aerea,
        rs.qtd_rotas_saindo AS qtd_rotas_saindo,
        rc.qtd_rotas_chegando AS qtd_rotas_chegando,
        pd.qtd_pousos_decolagens AS qtd_pousos_decolagens
    FROM api ap
    LEFT JOIN rotas_saindo rs ON ap.icao = rs.icao_aeroporto AND rs.rank_saindo = 1
    LEFT JOIN rotas_chegando rc ON ap.icao = rc.icao_aeroporto AND rc.rank_chegando = 1
    LEFT JOIN pousos_decolagens pd ON ap.icao = pd.icao_aeroporto
""")
df_resultado.createOrReplaceTempView("resultado")

# Display do preview do resultado
df_resultado.show(truncate=False)

```
E o resultado conforme a print a seguir:
  
[Print]




# Extras

- Descrever qual estratégia você usaria para ingerir estes dados de forma incremental caso precise capturar esses dados a cada mes?
- Justifique em cada etapa sobre a escalabilidade da tecnologia utilizada.
- Justifique as camadas utilizadas durante o processo de ingestão até a disponibilização dos dados.

## Resposta:

Para ilustrar, temos uma arquitetura medalhão, muito utilizada em delta lakes e lakehouses:

![Arquitetura_Medalhao](https://github.com/matheusbudin/big-data-airlines/blob/development/assets_for_readme/arquitetura_medalhao.png)

# Estratégia de Ingestão Incremental de Dados:

-A estratégia de ingestão incremental de dados consiste em capturar, transformar e disponibilizar novos dados ou dados alterados em intervalos de tempo reglares (por exemplo, anualmente, mensalmente, diario). Essa abordagem é fundamental para manter as informações atualizadas sem sobrecarregar os recursos de processamento e armazenamento. A metodologia "MEDALHÃO" do Delta Lake aprimora essa estratégia, oferecendo recursos presentes nos arquivos tipo delta para gerenciamento de dados confiável e escalável, um recurso muito útil é o ```time travel``` que é possível verificar como o dado estava antes de ser transformado por uma nova carga, isso é possível graças aos logs criados pelos arquivos tipo ```delta```.

## 1. Extração de Dados

- **Abordagem Incremental:** A primeira etapa envolve a extração dos dados que foram criados ou modificados após a última extração. Isso pode ser feito por meio de marcações de data/hora ou outras informações de controle. A definição da estratégia de tempo a ser considerada deve ser documentada pela área de negócios e comunicada ao time de dados e desenvolvimento. A ingestão incremental reduz a carga de trabalho, tornando a escalabilidade mais eficiente. Delta Lake suporta essa abordagem e rastreia as alterações nos dados.

## 2. Transformação de Dados

- **Utilização de DataFrames e Spark:** Os dados extraídos são transformados usando DataFrames no Apache Spark, que é uma abordagem escalável e eficaz para processamento e transformação de dados em lote. É compatível com Apache Spark e mantém o desempenho em escala, garantindo que as transformações sejam eficientes. Além disso, podemos utilizar clusters nos cloud providers, que auxiliam ainda mais na escalabilidade horizontal do processamento.

## 3. Deduplicação de Dados

- **Processo de Deduplicação:** A deduplicação de dados é crucial para evitar registros duplicados. Delta Lake suporta a criação de chaves únicas e a remoção de duplicações.


## 4. Carregamento de Dados

- **Uso de Delta Lake:** Os dados transformados são carregados em tabelas Delta Lake, que são ideais para armazenamento escalável e confiável de dados. Delta Lake é altamente escalável e fornece controle de transações para operações de gravação, garantindo integridade e recuperação confiável de dados.

## 5. Gerenciamento de Metadados

- **Registros de Metadados:** Delta Lake mantém registros de metadados que rastreiam as operações realizadas nos dados, garantindo visibilidade total das alterações. A metodologia "MEDALHÃO" do Delta Lake oferece um registro completo de alterações, proporcionando auditoria e rastreamento escaláveis.

## 6. Automação de Otimização e Compactação

- **Auto-otimização:** Delta Lake inclui recursos de otimização automática, como compactação e coleta de estatísticas, para melhorar o desempenho de consultas. A Automação pode ser feita por orquestradores como ```Apache airflow``` ou pelo proprio ```data bricks workflows ; aws step functions; GCP composer```


## 7. Informações adicionais
- Vale salientar que a metodologia medalhão pode ser utilizada em diversos cloud providers, pois possui uma versão ```open source```
- De forma geral: a camada ```bronze``` contem os dados tranformados da camada ```raw``` para o formato ```delta```; a camada ```silver``` adiciona tratamentos de limpeza aos dados da camada ```bronze``` e por fim, a camada ```gold``` é formada adicionando as regras de negócio.

## 8. Tendência dos Data Warehouses em plataformas "ALL IN ONE" para desenvolvimento "END TO END" de projetos de Big Data e Analytics:
**Data Warehouses Modernos:**

- **Databricks:** O Databricks aceita desde a orquestração dos worflows para extração de dados, quanto a transformação utilizando spark e a carga em várias ferramentas de BI.

- **BigQuery:** O BigQuery tem se mostrado cada vez mais desenvolvido, possui recursos de governança, machine learning e recentemente o desenvolvimento com jupyter notebooks. Além disso possui conector direto com o Looker Studio para criação de BI's e também suporta consultas AD HOC.
