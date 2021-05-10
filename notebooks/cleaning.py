from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from sqlalchemy import create_engine
from download import save
save()
spark = SparkSession \
    .builder \
    .appName("Postgress") \
    .config("spark.jars", "postgresql-42.2.18.jre7.jar") \
    .getOrCreate()

import psycopg2
from sqlalchemy import create_engine
def dropTable(name):
    engine = create_engine('postgresql+psycopg2://username:secret@db:5432/database')
    conn = psycopg2.connect(database="database",user="username", password="secret",host="db", port="5432")
    cur = conn.cursor()
    sql="""DROP TABLE """+f'{name}'+""";"""
    cur.execute(sql)
    conn.commit()

def state(x):
    x = str(x)
    if len(x) ==1:
        x = '0'+ x
    return x
def county(x):
    x = str(x)
    if len(x)==1:
        x = '00'+ x
    elif len(x)==2:
        x = '0' + x
    return x

def fips(x):
    x = str(x)
    if len(x)==4:
        x = '0'+x
    return x
state_convert = F.udf(lambda x: state(x)) 
county_convert = F.udf(lambda x: county(x)) 
fips_convert = F.udf(lambda x: fips(x))  


engine = create_engine('postgresql+psycopg2://username:secret@db:5432/database')
conn = psycopg2.connect(database="database",user="username", password="secret",host="db", port="5432")
cur = conn.cursor()
cur.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")

tables = []
for table in cur.fetchall():
    tables.append(table[0])
    

base = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://db:5432/database")\
    .option("dbtable", 'base_covid_tmp') \
    .option("user", "username") \
    .option("password", "secret") \
    .option("driver", "org.postgresql.Driver") \
    .load()


base = base.withColumn("fips", base["fips"].cast(IntegerType()))
base = base.withColumn("fips", base["fips"].cast(StringType()))
base = base.withColumnRenamed('fips','FIPS')
base = base.withColumn('FIPS',fips_convert(F.col('FIPS')))
base.count()

base.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://db:5432/database")\
    .option("dbtable", 'base') \
    .option("user", "username") \
    .option("password", "secret") \
    .option("driver", "org.postgresql.Driver") \
    .save()


education = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://db:5432/database")\
    .option("dbtable", "education_tmp") \
    .option("user", "username") \
    .option("password", "secret") \
    .option("driver", "org.postgresql.Driver") \
    .load()


education = education.withColumn("State_FIPS", state_convert(F.col("State_FIPS")))
education = education.withColumn("County_FIPS", county_convert(F.col("County_FIPS")))


col_list = ['State_Fips','County_FIPS']
education = education.withColumn('FIPS',F.concat(*col_list))
columns_to_drop = ['Geo_Name','State_Fips','County_Fips']
education = education.drop(*columns_to_drop)
education.count()
education.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://db:5432/database")\
    .option("dbtable", "education") \
    .option("user", "username") \
    .option("password", "secret") \
    .option("driver", "org.postgresql.Driver") \
    .save()


poverty = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://db:5432/database")\
    .option("dbtable", "poverty_tmp") \
    .option("user", "username") \
    .option("password", "secret") \
    .option("driver", "org.postgresql.Driver") \
    .load()
poverty= poverty.withColumn("State_FIPS", state_convert(F.col("State_FIPS")))
poverty = poverty.withColumn("County_FIPS", county_convert(F.col("County_FIPS")))
col_list = ['State_Fips','County_FIPS']
poverty= poverty.withColumn('FIPS',F.concat(*col_list))
columns_to_drop = ['Geo_Name','State_Fips','County_Fips']
poverty = poverty.drop(*columns_to_drop)
poverty.count()
poverty.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://db:5432/database")\
    .option("dbtable", "poverty") \
    .option("user", "username") \
    .option("password", "secret") \
    .option("driver", "org.postgresql.Driver") \
    .save()


population = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://db:5432/database")\
    .option("dbtable", "population_tmp") \
    .option("user", "username") \
    .option("password", "secret") \
    .option("driver", "org.postgresql.Driver") \
    .load()

col_list = ['STATE','COUNTY']


population = population.withColumn("STATE", state_convert(F.col("STATE")))
population = population.withColumn("COUNTY", county_convert(F.col("COUNTY")))

population = population.withColumn('FIPS',F.concat(*col_list))

population = population.filter(population.AGEGRP=='0')
selected = ['CTYNAME','TOT_POP','TOT_MALE','TOT_FEMALE','WA_MALE','WA_FEMALE','BA_MALE',
'BA_FEMALE','IA_MALE','IA_FEMALE','AA_MALE','AA_FEMALE','H_MALE','H_FEMALE','FIPS']
population = population[selected]
population = population.withColumn('State_Code',population['FIPS'].substr(1,2))
population.count()
population.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://db:5432/database")\
    .option("dbtable", "population") \
    .option("user", "username") \
    .option("password", "secret") \
    .option("driver", "org.postgresql.Driver") \
    .save()


daily = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://db:5432/database")\
    .option("dbtable", "daily_covid_tmp") \
    .option("user", "username") \
    .option("password", "secret") \
    .option("driver", "org.postgresql.Driver") \
    .load()

cols = ['date','state','positiveIncrease','death','fips']
daily = daily[cols]
daily = daily.withColumn("fips", daily["fips"].cast(StringType()))
daily = daily.withColumnRenamed('fips','FIPS')
daily.count()
daily.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://db:5432/database")\
    .option("dbtable", "daily") \
    .option("user", "username") \
    .option("password", "secret") \
    .option("driver", "org.postgresql.Driver") \
    .save()


for table in tables:
    dropTable(table)