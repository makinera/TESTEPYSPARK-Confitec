import datetime
from pyspark.sql.functions import *

df_dados = spark.read.parquet('/FileStore/tables/testeMakinera/OriginaisNetflix.parquet')
df_dados.createOrReplaceTempView('tb_dados')

df_pergunta1 = df_dados.select(to_date('Premiere','dd-MMM-yy').alias('Premiere'),
                               col('dt_inclusao').cast('timestamp').alias('dt_inclusao_with_timezone'),
                               regexp_extract('dt_inclusao','(.)\-(.)',1).cast('timestamp').alias('dt_inclusao_without_timezone'))

df_pergunta2 = df_dados.orderBy(col('Active').desc(), col('Genre'))

df_pergunta3 = df_pergunta2\
            .withColumn('Seasons', when(col('Seasons') == 'TBA', 'a ser anunciado')\
            .otherwise(col('Seasons')))\
            .dropDuplicates()
            
df_pergunta4 = df_pergunta3.withColumn('Data_de_alteracao', current_timestamp())

l_columns_p5 = [col('Title').alias('Titulo'),
				col('Genre').alias('Genero'),
				col('GenreLabels').alias('Genero_marcacao'),
				col('Premiere').alias('Estreia'),
				col('Seasons').alias('Temporadas'),
				col('SeasonsParsed').alias('Temporadas_numero'),
				col('EpisodesParsed').alias('Episodios_numero'),
				col('Length').alias('Duracao'),
				col('MinLength').alias('Duracao_minima'),
				col('MaxLength').alias('Duracao_maxima'),
				col('Status').alias('Status'),
				col('Active').alias('Ativo_marcacao'),
				col('Table').alias('Categoria'),
				col('Language').alias('Lingua'),
				col('dt_inclusao').alias('Dt_inclusao'),
				col('Data_de_alteracao').alias('Data_de_alteracao')]

df_pergunta5 = df_pergunta4.select(l_columns_p5)

#Pergunta 6

try:
  spark.read.parquet('/FileStore/tables/testeMakinera/OriginaisNetflix.parquet')
except:
  print('Pasta com alguma problema')
  
#Pergunta 7

l_columns_p7 = [col('Titulo'),
				col('Genero'),
				col('Temporadas'),
				col('Estreia'),
				col('Lingua'),
				col('Ativo_marcacao'),
				col('Status'),
				col('Dt_inclusao'),
				col('Data_de_alteracao')]

df_pergunta7 = df_pergunta5.select(l_columns_p7)

df_pergunta7\
  .repartition(1)\
  .write.format('csv')\
  .mode('overwrite')\
  .option('header', 'true')\
  .option('delimiter', ';')\
  .save('S3_path') #Pergunta 8