import requests
import pandas as pd
import json
import psycopg2
from sqlalchemy import create_engine


def save_table(df,name):
    import io
    df.head(0).to_sql(f'{name}', engine, if_exists='replace',index=False) 

    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, f'{name}' , null="")
    conn.commit()
    return

engine = create_engine('postgresql+psycopg2://username:secret@db:5432/database')
conn = psycopg2.connect(database="database",user="username", password="secret",host="db", port="5432")
cur = conn.cursor()




def save():
    cur.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")
    counter = 0
    for table in cur.fetchall():
        counter +=1
    if counter ==0:
        #no tables are in database
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
    else:return


def update():
    base = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
    daily = 'https://api.covidtracking.com/v1/states/daily.json'
    d =requests.get(daily)
  

    base_new= pd.read_csv(base)
    daily_new = pd.read_json(d.text)
    act_base = pd.read_sql_query('select * from base_covid_tmp',con=engine)
    act_daily = pd.read_sql_query('select * from daily_covid_tmp',con=engine)
    my_dat = str(act_base.tail(1).date.values[0])
    new_dat = str(base_new.tail(1).date.values[0])
    if my_dat != new_dat:
        base = base_new[base_new['date']>my_dat]
        base = pd.concat([base,act_base],axis=1)
        save_table(base,'base_covid_tmp')
        dat_daily = int(my_dat.replace('-',''))
        daily = daily_new[daily_new['date']>dat_daily]
        daily = pd.concat([daily,act_daily],axis=1)
        save_table(daily,'daily_covid_tmp')
    else:return



