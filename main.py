import csv, sqlite3, logging
from sqlite3.dbapi2 import connect

logging.basicConfig(filename='state_log.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

def create_database(db_name):
    connection = sqlite3.connect(db_name)
    cursor = connection.cursor()

    sql = """
        CREATE TABLE StateInformation (
        stateNo TEXT,
        stateName TEXT,
        capital TEXT,
        population TEXT,
        primary key(stateNo)
        ) """

    cursor.execute(sql)
    logging.info("State Table Created")

    #Saves File
    connection.commit()
    #Closes Database
    connection.close()

def import_data(db_name):
    connection = sqlite3.connect(db_name)
    cursor = connection.cursor()

    for num, state in enumerate(state_information):
        stateNo = num
        cursor.execute("INSERT INTO StateInformation VALUES (?,?,?,?)", [stateNo, state_information[state]["state"], state_information[state]["capital"], state_information[state]["population"]])

    connection.commit()
    connection.close()
    logging.info("Database Import Completed")

database_name = "states.db"
state_information = {}

# Step One: Create Database
create_database(database_name)

# Step Two: Read CSV and Load Data to Nested-Dictionary
with open('States.csv', mode='r', encoding='utf-8-sig') as state_csv:
    reader = csv.reader(state_csv)
    next(reader)

    for row in reader:
        state_information[row[0]] = {"state":row[0], "capital":row[1], "population":row[2]}
    logging.info("Records Extracted")

#Step Three: Import Nested Dictionary to DB Table
import_data(database_name)