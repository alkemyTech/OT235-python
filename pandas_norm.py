import pandas as pd

"""Function to norm data and export txt"""
def lat_norm():
    #import dataframes
    df = pd.read_csv('lat_sociales_data.csv',sep=',')
    df_pc = pd.read_csv('codigos_postales.csv',sep=',')

    #normalized data
    df['universities'] = df['universities'].str.replace('-',' ').str.strip().str.lower()
    df.rename(columns={'universities': 'university', }, inplace=True)
    df['career'] = df['career'].str.replace('-',' ').str.strip().str.lower()
    df['inscription_date'] = pd.to_datetime(df['inscription_date'],format='%d-%m-%Y')
    df['inscription_date'] = df['inscription_date'].dt.strftime('%Y-%m-%d')
    df['last_name'] = df['last_name'].str.replace('-',' ').str.strip().str.lower()
    df['first_name'] = df['first_name'].str.replace('-',' ').str.strip().str.lower()
    df.loc[(df['gender'] == 'M'), 'gender'] = 'male'
    df.loc[(df['gender'] == 'F'), 'gender'] = 'female'
    df["age"] = df["age"].astype(int)
    df['postal_code'] = df_pc['codigo_postal']
    df['location'] = df['location'].str.replace('-',' ').str.strip().str.lower()
    df['email'] = df['email'].str.replace('-',' ').str.strip().str.lower()
    df = df.reindex(columns=['universities','career','inscription_date','first_name','last_name','gender','age','postal_code','location','email'])

    #export data
    df.to_csv('lat_sociales_data.txt')


def kennedy_norm():
    #import dataframe
    df = pd.read_csv('kennedy_data.csv',sep=',')

    #normalized data
    df['university'] = df['university'].str.replace('-',' ').str.replace('.',' ').str.strip().str.lower()
    df['career'] = df['career'].str.replace('-',' ').str.strip().str.lower()
    df['inscription_date'] = pd.to_datetime(df['inscription_date'],format = '%y-%b-%d')
    df['inscription_date'] = df['inscription_date'].dt.strftime('%Y-%m-%d')
    df['last_name'] = df['last_name'].str.replace('-',' ').str.strip().str.lower()
    df['first_name'] = df['first_name'].str.replace('-',' ').str.strip().str.lower()
    df.loc[(df['gender'] == 'm'), 'gender'] = 'male'
    df.loc[(df['gender'] == 'f'), 'gender'] = 'female'
    df["age"] = df["age"].astype(int)
    df['location'] = df['location'].str.replace('-',' ').str.replace('\n',' ').str.strip().str.lower().str.replace(',',' ')

    #export data
    df.to_csv('kennedy_data.txt')


if __name__ == "__main__":
    lat_norm()
    kennedy_norm()