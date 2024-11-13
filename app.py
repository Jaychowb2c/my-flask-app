import pyodbc  
import struct  
import msal  
from typing import Union  
from fastapi import FastAPI, HTTPException  
from pydantic import BaseModel  
  
# Model for Person  
class Person(BaseModel):  
    first_name: str  
    last_name: Union[str, None] = None  
  
# Model for updating a Person  
class UpdatePerson(BaseModel):  
    first_name: Union[str, None] = None  
    last_name: Union[str, None] = None  
  
# Directly set the connection string  
connection_string = (  
    "Driver={ODBC Driver 18 for SQL Server};"  
    "Server=tcp:jaysqltest.database.windows.net,1433;"  
    "Database=SQLfortest;"  
    "Encrypt=yes;"  
    "TrustServerCertificate=no;"  
    "Connection Timeout=30;"  
    "Authentication=ActiveDirectoryMsi"  
)  
  
# Initialize FastAPI app  
app = FastAPI()  
  
@app.on_event("startup")  
def startup():  
    print("Starting up and creating the table if it doesn't exist.")  
    try:  
        conn = get_conn()  
        cursor = conn.cursor()  
        cursor.execute("""  
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Persons' and xtype='U')  
            CREATE TABLE Persons (  
                ID int NOT NULL PRIMARY KEY IDENTITY,  
                FirstName varchar(255),  
                LastName varchar(255)  
            );  
        """)  
        conn.commit()  
    except Exception as e:  
        print(f"Error during table creation: {e}")  
  
@app.get("/")  
def root():  
    return "Person API"  
  
@app.get("/persons")  
def get_persons():  
    rows = []  
    try:  
        with get_conn() as conn:  
            cursor = conn.cursor()  
            cursor.execute("SELECT * FROM Persons")  
            for row in cursor.fetchall():  
                rows.append({"ID": row.ID, "FirstName": row.FirstName, "LastName": row.LastName})  
    except Exception as e:  
        raise HTTPException(status_code=500, detail=str(e))  
    return rows  
  
@app.get("/person/{person_id}")  
def get_person(person_id: int):  
    try:  
        with get_conn() as conn:  
            cursor = conn.cursor()  
            cursor.execute("SELECT * FROM Persons WHERE ID = ?", person_id)  
            row = cursor.fetchone()  
            if row:  
                return {"ID": row.ID, "FirstName": row.FirstName, "LastName": row.LastName}  
            else:  
                raise HTTPException(status_code=404, detail="Person not found")  
    except Exception as e:  
        raise HTTPException(status_code=500, detail=str(e))  
  
@app.post("/person")  
def create_person(item: Person):  
    try:  
        with get_conn() as conn:  
            cursor = conn.cursor()  
            cursor.execute("INSERT INTO Persons (FirstName, LastName) VALUES (?, ?)", item.first_name, item.last_name)  
            conn.commit()  
    except Exception as e:  
        raise HTTPException(status_code=500, detail=str(e))  
    return item  
  
@app.put("/person/{person_id}")  
def update_person(person_id: int, item: UpdatePerson):  
    try:  
        with get_conn() as conn:  
            cursor = conn.cursor()  
            if item.first_name is not None:  
                cursor.execute("UPDATE Persons SET FirstName = ? WHERE ID = ?", item.first_name, person_id)  
            if item.last_name is not None:  
                cursor.execute("UPDATE Persons SET LastName = ? WHERE ID = ?", item.last_name, person_id)  
            conn.commit()  
            cursor.execute("SELECT * FROM Persons WHERE ID = ?", person_id)  
            row = cursor.fetchone()  
            if row:  
                return {"ID": row.ID, "FirstName": row.FirstName, "LastName": row.LastName}  
            else:  
                raise HTTPException(status_code=404, detail="Person not found")  
    except Exception as e:  
        raise HTTPException(status_code=500, detail=str(e))  
  
@app.delete("/person/{person_id}")  
def delete_person(person_id: int):  
    try:  
        with get_conn() as conn:  
            cursor = conn.cursor()  
            cursor.execute("DELETE FROM Persons WHERE ID = ?", person_id)  
            conn.commit()  
            if cursor.rowcount == 0:  
                raise HTTPException(status_code=404, detail="Person not found")  
            return {"detail": "Person deleted"}  
    except Exception as e:  
        raise HTTPException(status_code=500, detail=str(e))  
  
# Function to get a connection to the database using Managed Identity  
def get_conn():  
    client_id = 'YOUR_CLIENT_ID'  # Replace with your Managed Identity Client ID  
    managed_identity = msal.UserAssignedManagedIdentity(client_id=client_id)  
    global_app = msal.ManagedIdentityClient(managed_identity, http_client=requests.Session())  
    result = global_app.acquire_token_for_client(resource='https://database.windows.net/')  
      
    if "access_token" in result:  
        print("Token obtained!")  
        token_bytes = result['access_token'].encode("UTF-16-LE")  
        token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)  
        SQL_COPT_SS_ACCESS_TOKEN = 1256  # This connection option is defined by Microsoft in msodbcsql.h  
        conn = pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})  
        return conn  
    else:  
        raise HTTPException(status_code=500, detail="Failed to obtain access token")  
