import pandas as pd
from pathlib import Path


def process_data_univ() -> None:
    """Transform data from csv files where column information comes
        in the following order:
        - index
        - university name
        - career
        - inscription date
        - name
        - gender
        - birth date
        - location or postal code
        - email

        The processed data are saved in txt files.
    """

    # Definition of file paths
    DIR = Path(__file__).resolve().parent.parent
    UNIVERSIDAD = ['interam', 'pampa']
    FILE_PATH_READ_1 = str(DIR) + '/files/datos_universidad_interam.csv'
    FILE_PATH_READ_2 = str(DIR) + '/files/datos_universidad_pampa.csv'
    FILE_PATH = [FILE_PATH_READ_1, FILE_PATH_READ_2]
    FILE_CP = str(DIR) + '/cod_postales.csv'  

    for index, FILE in enumerate(FILE_PATH):
        with open(FILE, 'r') as file:
            df = pd.read_csv(file, encoding='UTF-8')

        # Columns rename
        col_names = df.columns.values
        df = df.rename(columns={col_names[1]: 'university',
                                col_names[2]: 'career',
                                col_names[3]: 'inscription_dates',
                                col_names[4]: 'name',
                                col_names[5]: 'gender',
                                col_names[6]: 'birth_date',
                                col_names[8]: 'email'
                                })
        if FILE == FILE_PATH[0]:
            df = df.rename(columns={col_names[7]: 'location'})
        elif FILE == FILE_PATH[1]:
            df = df.rename(columns={col_names[7]: 'postal_code'})

        df.drop(columns=col_names[0], inplace=True)

        # normalization of columns university, career and inscription dates
        # and email
        df.university = df.university.str.replace(
            '-', ' ').str.strip().str.lower()
        df.career = df.career.str.replace('-', ' ').str.strip().str.lower()
        df.email = df.email.str.strip().str.lower()

        df.inscription_dates = pd.to_datetime(df.inscription_dates).astype(str)

        # transformation of column names
        if FILE == FILE_PATH[0]:
            names = df.name.str.strip().str.lower().str.split('-', expand=True)
        elif FILE == FILE_PATH[1]:
            names = df.name.str.strip().str.lower().str.split(' ', expand=True)

        arr = ['md', 'md.', 'dr.', 'ms.', 'mrs.', 'mrs', 'mr.', 'mr', 'jr.',
               'phd', 'dds', 'dds.', 'dvm', 'dvm.', 'miss', 'i', 'ii', 'iii',
               'iv']
        names = names.applymap(lambda x:  '' if x in arr else x)
        names.fillna(' ', inplace=True)
        if FILE == FILE_PATH[0]:
            names['full_name'] = names[[0, 1, 2]].agg(' '.join, axis=1)
        elif FILE == FILE_PATH[1]:
            names['full_name'] = names[[0, 1, 2, 3]].agg(' '.join, axis=1)

        df[['last_name', 'first_name']] = names['full_name'].str.strip(
        ).str.lower().str.split(' ', expand=True)
        df.drop(columns='name', inplace=True)

        # transformation of column gender
        df.gender = df.gender.str.replace(
            'M', 'male').str.replace('F', 'female')

        # tranformation of birth date to age (birth date out of range
        # are replaced for a fixed date)
        df.birth_date = pd.to_datetime(df.birth_date, errors='coerce')
        df.birth_date.fillna(pd.to_datetime('20/Jan/20'), inplace=True)
        df['age'] = pd.to_datetime('today').year - df.birth_date.dt.year
        df.drop(columns='birth_date', inplace=True)

        # normalization of column location
        if FILE == FILE_PATH[0]:
            df.location = df.location.str.strip().str.lower().str.replace('-',
                                                                          ' ')
        # postal code file manipulation
        with open(FILE_CP, 'r') as file_cp:
            df_cp = pd.read_csv(file_cp)
        df_cp.localidad = df_cp.localidad.str.lower()
        df_cp.rename(columns={'localidad': 'location',
                              'codigo_postal': 'postal_code'}, inplace=True)

        # join between location and postal code
        if FILE == FILE_PATH[0]:
            df = df.join(df_cp.set_index('location'), on='location')
            df.postal_code = df.postal_code.astype(str).str.replace('.0', '')
        elif FILE == FILE_PATH[1]:
            df = df.join(df_cp.set_index('postal_code'), on='postal_code')

        # save txt files in files folder
        FILE_PATH_WRITE = f'{str(DIR)}/files/datos_universidad_{UNIVERSIDAD[index]}.txt'
        with open(FILE_PATH_WRITE, 'w') as f:
            df.to_csv(f, header=None, index=None,
                      sep=' ', line_terminator='\n')
