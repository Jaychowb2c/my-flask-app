import pyodbc  
import struct  
from azure import identity  
from typing import Union  
from fastapi import FastAPI  
from pydantic import BaseModel  
  
# Model for Person  
class Person(BaseModel):  
    first_name: str  
    last_name: Union[str, None] = None  
  
# Directly set the connection string  
connection_string = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:jaysqltest.database.windows.net,1433;Database=SQLfortest;Uid=forsqlserver;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;Authentication=ActiveDirectoryIntegrated"  
  
# Initialize FastAPI app  
app = FastAPI()  
  
@app.get("/")  
def root():  
    print("Root of Person API")  
    try:  
        conn = get_conn()  
        cursor = conn.cursor()  
        # Table should be created ahead of time in production app.  
        cursor.execute("""  
            CREATE TABLE Persons (  
                ID int NOT NULL PRIMARY KEY IDENTITY,  
                FirstName varchar(255),  
                LastName varchar(255)  
            );  
        """)  
        conn.commit()  
    except Exception as e:  
        # Table may already exist  
        print(e)  
    return "Person API"  
  
@app.get("/all")  
def get_persons():  
    rows = []  
    with get_conn() as conn:  
        cursor = conn.cursor()  
        cursor.execute("SELECT * FROM Persons")  
        for row in cursor.fetchall():  
            print(row.FirstName, row.LastName)  
            rows.append(f"{row.ID}, {row.FirstName}, {row.LastName}")  
    return rows  
  
@app.get("/person/{person_id}")  
def get_person(person_id: int):  
    with get_conn() as conn:  
        cursor = conn.cursor()  
        cursor.execute("SELECT * FROM Persons WHERE ID = ?", person_id)  
        row = cursor.fetchone()  
        return f"{row.ID}, {row.FirstName}, {row.LastName}"  
  
@app.post("/person")  
def create_person(item: Person):  
    with get_conn() as conn:  
        cursor = conn.cursor()  
        cursor.execute("INSERT INTO Persons (FirstName, LastName) VALUES (?, ?)", item.first_name, item.last_name)  
        conn.commit()  
    return item  
  
# Function to get a connection to the database  
def get_conn():  
    credential = identity.DefaultAzureCredential(exclude_interactive_browser_credential=False)  
    token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")  
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)  
    SQL_COPT_SS_ACCESS_TOKEN = 1256  # This connection option is defined by microsoft in msodbcsql.h  
    conn = pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})  
    return conn  