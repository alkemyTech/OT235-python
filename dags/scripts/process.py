import os
from datetime import date

import pandas as pd

ROOT = os.path.abspath(os.getcwd())


# university: str minúsculas, sin espacios extras, ni guiones
# career: str minúsculas, sin espacios extras, ni guiones
# inscription_date: str %Y-%m-%d format
# first_name: str minúscula y sin espacios, ni guiones
# last_name: str minúscula y sin espacios, ni guiones
# gender: str choice(male, female)
# age: int
# postal_code: str
# location: str minúscula sin espacios extras, ni guiones
# email: str minúsculas, sin espacios extras, ni guiones


#list of abbreviations
def abbreviations():
    list_of_abbreviations = ['mrs', 'ms', 'mr' ,'dr' , 'jr']
    return list_of_abbreviations

#clear the name
def clean_name(row):
    list_of_abbreviations = abbreviations()
    full_name = row.lower().replace('-', '').replace('_', ' ').replace('.', '')
    # We remove abbreviations
    for name in full_name.split(' '):
        for abbre in list_of_abbreviations: 
            if abbre in name:
                full_name = full_name.replace(abbre, '')
    #remove spaces
    full_name= full_name.strip()
    return (full_name.split(' '))



def choice_gender(row):
    if row['gender'] == 'M':
        return 'male'
    elif row['gender'] == 'F':
        return 'female'
    else:
        return row['gender'].astype(str)


def calculate_age(born):
    born = pd.to_datetime(born, format='%Y-%m-%d').date()
    today = date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

def calculate_age_v(born):
    born = pd.to_datetime(born, format= '%d-%b-%y').date()
    today = date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

def get_df_cod_postal():
    df = pd.read_csv(ROOT + '/dags/helper/codigos_postales.csv')
    return df


def get_first_name(row):
    name = clean_name(row)
    first_name = name[0]
    return first_name
        

def get_last_name(row):
    name = clean_name(row)
    last_name = name[1]
    return last_name

def process_flores() -> str:
    """ normalize the data from universidad_f.csv and save in txt file
        return: fullpath of the file txt
     """
    # read the csv file
    df = pd.read_csv(ROOT + '/dags/files/universidad_f.csv')
    df_postal = get_df_cod_postal()

    df_join = pd.merge(df, df_postal, left_on='postal_code', right_on='codigo_postal', how='left').drop_duplicates()

    # normalize the data
    df_join['university'] = df_join['university'].str.lower().str.replace(' ', '').str.replace('-', '')
    df_join['career'] = df_join['career'].str.lower().str.replace(' ', '').str.replace('-', '')
    df_join['inscription_date'] = pd.to_datetime(df_join['inscription_date'], format= '%Y-%m-%d').astype(str)
    df_join['first_name'] = df_join['name'].apply(get_first_name)
    df_join['last_name'] = df_join['name'].apply(get_last_name)
    df_join['gender'] = df_join.apply(choice_gender, axis=1)
    df_join['location'] = df_join['localidad'].str.lower().str.replace(' ', '').str.replace('-', '')
    # calculate age
    df_join['age'] = df_join['age'].apply(calculate_age)
    df_join['email'] = df_join['email'].str.lower().str.replace(' ', '').str.replace('-', '')

    df_join = df_join[['university', 'career', 'inscription_date', 'first_name', 'last_name', 'gender',
                       'age','postal_code','location','email']]


    save_path = ROOT + '/dags/files/universidad_f_normalized.txt'
    df_join.to_csv(save_path, index=False)

    return save_path


def process_v_maria():
    """ normalize the data from universidad_v.csv and save in txt file
        return: fullpath of the file txt
     """
    # read the csv file
    df = pd.read_csv(ROOT + '/dags/files/universidad_v_m.csv')
    df_postal = get_df_cod_postal()
    df_postal['localidad'] = df_postal['localidad'].str.replace(' ', '_')
    df_join = pd.merge(df, df_postal, left_on='location', right_on='localidad', how='left').drop_duplicates()

    # normalize the data
    df_join['university'] = df_join['university'].str.lower().str.replace(' ', '').str.replace('-', '')
    df_join['career'] = df_join['career'].str.lower().str.replace(' ', '').str.replace('-', '')
    df_join['inscription_date'] = pd.to_datetime(df_join['inscription_date'], format= '%d-%b-%y').astype(str)
    df_join['first_name'] = df_join['name'].apply(get_first_name)
    df_join['last_name'] = df_join['name'].apply(get_last_name)
    df_join['gender'] = df_join.apply(choice_gender, axis=1)
    df_join['location'] = df_join['localidad'].str.lower().str.replace(' ', '').str.replace('-', '')
    df_join['postal_code'] = df_join['codigo_postal'].astype(str)
    # calculate age
    df_join['age'] = df_join['age'].apply(calculate_age_v)
    df_join['email'] = df_join['email'].str.lower().str.replace(' ', '').str.replace('-', '')
    df_join = df_join[['university', 'career', 'inscription_date', 'first_name', 'last_name', 'gender',
                       'age','postal_code','location','email']]


    save_path = ROOT + '/dags/files/universidad_v_m_normalized.txt'
    df_join.to_csv(save_path, index=False)

    return save_path
 

def main():
    process_flores()
    process_v_maria()


if __name__ == '__main__':
    main()

