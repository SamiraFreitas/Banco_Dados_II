# Databricks notebook source
# MAGIC %md Banco de dados II 
# MAGIC - Lendo um dataset 
# MAGIC - Conhecendo os datatypes de algumas colunas de um dataframe pyspark 
# MAGIC - Dataframe pyspark 
# MAGIC - Selecionando colunas e índices 
# MAGIC - Visualizando estatisticas descritivas de dados de dataframe 
# MAGIC - Adicionando colunas em Dataframe 
# MAGIC - Removendo colunas em dataframe 
# MAGIC - Esboçando gráficos 

# COMMAND ----------

#Visualizando datasets de exemplos do databricks 
display(dbutils.fs.ls("/databricks-datasets"))

# COMMAND ----------

##Lendo o arquivo de dados

arquivo = "dbfs:/databricks-datasets/fligths"

# COMMAND ----------

arquivo

# COMMAND ----------

#Ver os dados em formato de tabela 
df1 = spark\
.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "True")\
.csv("dbfs:/databricks-datasets/flights/departuredelays.csv")
display(df1)

# COMMAND ----------

df1 = spark\
.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "True")\
.csv("dbfs:/databricks-datasets/flights/departuredelays.csv")

display(df1.take(100))

# COMMAND ----------

#imprime a quantidade de linhas no dataframe 
df1.count()

# COMMAND ----------

# MAGIC %md Consultando os dados de um dataframe

# COMMAND ----------

from pyspark.sql.functions import max 
df1.select(max("delay")).take(1)

# COMMAND ----------

# Filtrando linhas de um dataframe usando filter 
df1.filter("delay < 2 ").show(2)

# COMMAND ----------

#Oderna o dataframe pela coluna delay
df1.sort("delay").show(5)

# COMMAND ----------

#visualizando estatísiticas descritivas 
df1.describe().show()

# COMMAND ----------

#Adiconando uma coluna ao dataframe 
df1= df1.withColumn('Nova Coluna', df1['delay']+2)
df1.show(10)

# COMMAND ----------

# Removendo coluna 
df1 = df1.drop('Nova Coluna')
df1.show(10)
