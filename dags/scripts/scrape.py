import os
import csv
import psycopg2

connection= psycopg2.connect(user= "alkymer2",
                                password= "Alkemy23",
                                host= "training-main.cghe7e6sfljt.us-east-1.rds.amazonaws.com",
                                port= "5432",
                                database= "training")
cursor= connection.cursor()


def read_query(path: str) -> str:
    """
    Read a query from a file and return it as a string.
    :param path: Path to the file
    :return: query as a string
    """
    query = open(path, 'rb').read().decode('utf-8')

    return query

def create_csv_uni_a():
    """
    Perform the SQL query and generate the CSV
    """
    root= os.path.abspath(os.getcwd())
    root_q= root+"/dags/querys/universidades_a.sql"

    cursor.execute(read_query(root_q))
    result_query= cursor.fetchall()

    root_f= root+"/dags/files/universidades_a.csv"

    with open(root_f, 'w', newline='') as file:
        writer = csv.writer(file, quoting=csv.QUOTE_ALL,delimiter=',')
        writer.writerows(result_query)

if __name__ == '__main__':
    create_csv_uni_a()