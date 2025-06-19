from pg8000.native import Connection 

def create_connection():
    user = "postgres"
    database = "destination_db"
    dbport = "5432"
    password = "secret"
    return Connection(
        database=database, user=user, password=password, port=dbport
    )