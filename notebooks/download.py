import requests
import pandas as pd
import json
import psycopg2
from sqlalchemy import create_engine


def save_table(df,name):
    import io
    df.head(0).to_sql(f'{name}', engine, if_exists='replace',index=False) #truncates the table

    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, f'{name}' , null="") # null values become ''
    conn.commit()
    return

engine = create_engine('postgresql+psycopg2://username:secret@db:5432/database')
conn = psycopg2.connect(database="database",user="username", password="secret",host="db", port="5432")
cur = conn.cursor()

def save():
    data = 'https://api.covidtracking.com/v1/states/daily.json'
    d =requests.get(data)
    df = pd.read_json(d.text)
    save_table(df,'daily_covid_tmp')

    #https://github.com/nytimes/covid-19-data
    data = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
    df = pd.read_csv(data)
    save_table(df,'base_covid_tmp')

    #docs https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2010-2019/cc-est2019-alldata.pdf
    data = 'https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/asrh/cc-est2019-alldata.csv'
    df = pd.read_csv(data,encoding='ISO-8859-1')
    df = df.query('YEAR==12')
    save_table(df,'population_tmp')

    data = 'https://overflow.solutions/wp-content/uploads/2020/05/Poverty_County_18.csv'
    df = pd.read_csv(data)
    save_table(df,'poverty_tmp')

    #https://overflow.solutions/demographic-traits/education/what-u-s-counties-are-the-most-educated/
    data = 'https://overflow.solutions/wp-content/uploads/2020/04/Educ_Attain_County_18.csv'
    df = pd.read_csv(data)
    save_table(df,'education_tmp')
    
