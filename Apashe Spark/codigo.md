# **CSI603 - BANCO DE DADOS II - Estrutura básica do repositório**

## Visualizando datasets de exemplos do databricks

```bash
display(dbutils.fs.ls("/databricks-datasets"))
```

## Lendo o arquivo de dados

```bash
arquivo = "dbsfs:/databricks-datasets/fligths"
```


## Esboçando o gráfico 

```bash
df1 = spark\
.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "True")\
.csv("dbfs:/databricks-datasets/flights/departuredelays.csv")
display(df1)
```

