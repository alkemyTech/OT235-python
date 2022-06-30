from pathlib import Path
from datetime import date

import pandas as pd

ROOT = str(Path(__name__).parent.resolve())


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
    first_name = row.lower().replace('-', '').replace('_', ' ')
    # replace words ending letter '.'
    for name in first_name.split(' '):
        if '.' in name:
            first_name = first_name.replace(name, '')
    # trim
    first_name = first_name.strip()
    return first_name.split(' ')[0]


def get_last_name(row):
    last_name = row.lower().replace('-', '').replace('_', ' ')
    # replace words ending with '.'
    for name in last_name.split(' '):
        if '.' in name:
            last_name = last_name.replace(name, '')
    last_name = last_name.strip()
    concat_name = ''
    for index, name in enumerate(last_name.split(' ')):
        if index > 0:
            concat_name += name + ' '
    last_name = concat_name
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

