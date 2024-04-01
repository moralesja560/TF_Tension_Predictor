import pandas as pd 
import sys,os

def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    base_path = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_path, relative_path)


hl1 = pd.read_csv(resource_path("HL1_t.csv"),parse_dates=['FECHA_AMD'])
hl2 = pd.read_csv(resource_path("HL2_t.csv"),parse_dates=['FECHA_AMD'])
hl3 = pd.read_csv(resource_path("HL3_t.csv"),parse_dates=['FECHA_AMD'])

hl1 = hl1.assign(line=1)
hl2 = hl2.assign(line=2)
hl3 = hl3.assign(line=3)



hl_final = pd.concat([hl1,hl2,hl3])
print(hl_final.head())
print(hl_final.describe())

hl_final.to_csv(resource_path("hl_final_t.csv"),index=False)






