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
    
def querys_address():
    """
    Get the list of SQL queries along with their directories
    :return: address list of each of the SQL queries
    """
    querys= []
    root= os.path.abspath(os.getcwd())+"/dags/querys"
    list_querys= os.listdir(root)
    for script in list_querys:
        querys.append(root+"/"+script)

    return querys
    
def create_csv(querys):
    """
    Perform the SQL query and generate the CSV
    """
    #Create the file folder if it doesn't exist
    folder = os.path.abspath(os.getcwd())+"/dags/files"
    os.makedirs(folder, exist_ok=True)

    #Create the csv
    for script in querys:
        query= read_query(script)
        cursor.execute(query)
        result_query= cursor.fetchall()

        #Name db
        lista_query= query.split()
        name_db= lista_query[lista_query.index("FROM")+1]

        #Headers
        lista_header= query.split()
        header= []
        for post in range(len(lista_header)):
           if (lista_header[post]=='as'):
                header_c= lista_header[post+1].replace(',','')
                header.append(header_c)

        #Address for the csv
        csv_format= script.replace(".sql", ".csv")
        csv_script= csv_format.replace("/querys/", "/files/")

        with open(csv_script, 'w', newline='') as file:
            writer = csv.writer(file, quoting=csv.QUOTE_ALL,delimiter=',')
            writer.writerow(header)
            writer.writerows(result_query)

                
            
    cursor.close()


def extract_data():
    #Get query address
    querys= querys_address()

    #Create CSV for each query
    create_csv(querys)

def main():
    extract_data()

if __name__ == '__main__':
    main()