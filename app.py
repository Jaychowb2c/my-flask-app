import os  
import pyodbc  
import struct  
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential  
from azure.keyvault.secrets import SecretClient  
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
        cursor.close()  
        conn.close()  
    except Exception as e:  
        print(f"Error during table creation: {e}")  
  
@app.get("/")  
def root():  
    return "Person API"  
  
@app.get("/persons")  
def get_persons():  
    rows = []  
    try:  
        conn = get_conn()  
        cursor = conn.cursor()  
        cursor.execute("SELECT * FROM Persons")  
        for row in cursor.fetchall():  
            rows.append({"ID": row.ID, "FirstName": row.FirstName, "LastName": row.LastName})  
        cursor.close()  
        conn.close()  
    except Exception as e:  
        raise HTTPException(status_code=500, detail=str(e))  
    return rows  
  
@app.get("/person/{person_id}")  
def get_person(person_id: int):  
    try:  
        conn = get_conn()  
        cursor = conn.cursor()  
        cursor.execute("SELECT * FROM Persons WHERE ID = ?", person_id)  
        row = cursor.fetchone()  
        cursor.close()  
        conn.close()  
        if row:  
            return {"ID": row.ID, "FirstName": row.FirstName, "LastName": row.LastName}  
        else:  
            raise HTTPException(status_code=404, detail="Person not found")  
    except Exception as e:  
        raise HTTPException(status_code=500, detail=str(e))  
  
@app.post("/person")  
def create_person(item: Person):  
    try:  
        conn = get_conn()  
        cursor = conn.cursor()  
        cursor.execute("INSERT INTO Persons (FirstName, LastName) VALUES (?, ?)", item.first_name, item.last_name)  
        conn.commit()  
        cursor.close()  
        conn.close()  
    except Exception as e:  
        raise HTTPException(status_code=500, detail=str(e))  
    return item  
  
@app.put("/person/{person_id}")  
def update_person(person_id: int, item: UpdatePerson):  
    try:  
        conn = get_conn()  
        cursor = conn.cursor()  
        if item.first_name is not None:  
            cursor.execute("UPDATE Persons SET FirstName = ? WHERE ID = ?", item.first_name, person_id)  
        if item.last_name is not None:  
            cursor.execute("UPDATE Persons SET LastName = ? WHERE ID = ?", item.last_name, person_id)  
        conn.commit()  
        cursor.execute("SELECT * FROM Persons WHERE ID = ?", person_id)  
        row = cursor.fetchone()  
        cursor.close()  
        conn.close()  
        if row:  
            return {"ID": row.ID, "FirstName": row.FirstName, "LastName": row.LastName}  
        else:  
            raise HTTPException(status_code=404, detail="Person not found")  
    except Exception as e:  
        raise HTTPException(status_code=500, detail=str(e))  
  
@app.delete("/person/{person_id}")  
def delete_person(person_id: int):  
    try:  
        conn = get_conn()  
        cursor = conn.cursor()  
        cursor.execute("DELETE FROM Persons WHERE ID = ?", person_id)  
        conn.commit()  
        rows_affected = cursor.rowcount  
        cursor.close()  
        conn.close()  
        if rows_affected == 0:  
            raise HTTPException(status_code=404, detail="Person not found")  
        return {"detail": "Person deleted"}  
    except Exception as e:  
        raise HTTPException(status_code=500, detail=str(e))  
  
# Function to get a connection to the database using a connection string from Azure Key Vault  
def get_conn():  
    # Retrieve environment variables  
    key_vault_url = os.getenv('AZURE_KEY_VAULT_URL')  
    secret_name = os.getenv('AZURE_SQL_CONNECTION_STRING_SECRET_NAME')  
    client_id = os.getenv('AZURE_SQL_USER')  
  
    if not key_vault_url or not secret_name or not client_id:  
        raise ValueError("Key Vault URL, Secret name, or Client ID is not set.")  
  
    # Retrieve the connection string from Azure Key Vault  
    credential = DefaultAzureCredential()  
    client = SecretClient(vault_url=key_vault_url, credential=credential)  
    secret = client.get_secret(secret_name)  
    connection_string = secret.value 
  
    # Get the access token using ManagedIdentityCredential  
    credential = ManagedIdentityCredential(client_id=client_id)  
    token = credential.get_token("https://database.windows.net/.default")  
    token_bytes = token.token.encode("utf-16-le")  
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)  
    SQL_COPT_SS_ACCESS_TOKEN = 1256  # This connection option is defined by Microsoft in msodbcsql.h  
  
    # Connect to the database using the access token  
    conn = pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})  
  
    return conn  
