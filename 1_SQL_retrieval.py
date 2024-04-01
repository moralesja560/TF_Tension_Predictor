import pandas as pd
import pyodbc
import sys,os


def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    base_path = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_path, relative_path)

#SQL start
# Conexion SQL
server = 'SAL-W12E-SQL\MSSQLMEX' 
database = 'scadadata' 
username = 'scadamex' 
password = 'scadamex'  

#area to retrieve the df from SQL
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

query = r"SELECT * FROM [CSW_Cambio_Rollos] WHERE FECHA like '%2022%' OR FECHA like '%2024%'"
df = pd.read_sql(query, cnxn)
print(df.head())
df.to_csv(resource_path(f"coils_t.csv"),index=False)


query = r"SELECT * FROM [CSW_HL1_Registro] WHERE FECHA like '%2022%' OR FECHA like '%2024%'"
df = pd.read_sql(query, cnxn)
print(df.head())
df.to_csv(resource_path(f"HL1_t.csv"),index=False)


query = r"SELECT * FROM [CSW_HL2_Registro] WHERE FECHA like '%2022%' OR FECHA like '%2024%'"
df = pd.read_sql(query, cnxn)
print(df.head())
df.to_csv(resource_path(f"HL2_t.csv"),index=False)

query = r"SELECT * FROM [CSW_HL3_Registro] WHERE FECHA like '%2022%' OR FECHA like '%2024%'"
df = pd.read_sql(query, cnxn)
print(df.head())
df.to_csv(resource_path(f"HL3_t.csv"),index=False)

