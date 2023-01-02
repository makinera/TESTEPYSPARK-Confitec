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
  
  
#Matriz - Solução 1
 
import numpy as np

# cria uma matriz com numeros inteiros randomicos de 0 a 10
mA = np.random.randint(10, size=(4,4))

mB = np.random.randint(10, size=(4,4))

#multiplica a matriz A com a B
mProduto = np.matmul(mA, mB)

print('Matriz A:'+chr(10))
print(mA)

print(chr(10)+'Matriz B:'+chr(10))
print(mB)

print(chr(10)+'Matriz Producto, resultado da multiplicacao da A e B:'+chr(10))

print(mProduto)

#Matriz - Solução 2

import random
# funcao gera uma matriz quadrada
def matriz_quadrada():
    l = []
    for rr in range(0,4):
        #a.append()
        l_row = [random.randint(0,10) for x in range(0,4)]
        l.append(l_row)
    
    return l


mA = matriz_quadrada() # criando a matriz A
mB = matriz_quadrada() # criando a matriz B

#mA = [[1,2,3,4], [5,6,7,8], [1,2,3,4], [5,6,7,8]] hardcode valores do exemplo na questão 

#mB = [[1,2,3,4], [5,6,7,8], [1,2,3,4], [5,6,7,8]] hardcode valores do exemplo na questão 
print('Matriz A:')
for rr in mA:
    print(rr)

print(chr(10)+'Matriz B:')
for rr in mB:
    print(rr)




m_Produdo = []
l_total   = []
for index,l_mA in enumerate(mA):      # loop com o index e valor da matriz A
    l_total = []
    for rr in range(0,len(l_mA)):     # loop para varrer a linha da matriz A o numero de vezes do tamanho da matriz
        l_mB = [x[rr] for x in mB]    # loop para varrer a coluna da matriz B
        total = 0
        
        for lA, lB in zip(l_mA, l_mB): # loop para multiplicar a linha da matriz A com a coluna da matriz B
            total = total+lA*lB       
        
        l_total.append(total)
    m_Produdo.append(l_total)




# print do resultado:
print(chr(10)+"resultado da Matriz produto:"+chr(10))
for rr in m_Produdo:
    print(rr)
