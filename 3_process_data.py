import pandas as pd
import sys,os
import datetime


def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    base_path = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_path, relative_path)


#final_data hour and date processing
final_data = pd.read_csv(resource_path("hl_final_t.csv"),parse_dates=['FECHA_AMD'])
final_data['HORA'] = final_data['HORA'].str[0:5] + ":00"
final_data['HORA'] = pd.to_datetime(final_data['HORA'], format='%H:%M:%S')
final_data['HORA'] = final_data['HORA'].dt.time

print(f"pre-shaved final data {len(final_data)}")
#row shaving to final data
final_data = final_data[final_data.HARDENING_TEMP > 500]
final_data = final_data[final_data.TEMPERING_TEMP > 300]
final_data = final_data[final_data.Quenching_Temp >500]
"""
final_data = final_data[final_data.HARDENING_FLUJO_1 >0]
final_data = final_data[final_data.HARDENING_FLUJO_2 >0]
final_data = final_data[final_data.HARDENING_FLUJO_3 >0]
final_data = final_data[final_data.HARDENING_FLUJO_4 >0]
final_data = final_data[final_data.HARDENING_FLUJO_5 >0]

final_data = final_data[final_data.TEMPERING_FLUJO_1 >0]
final_data = final_data[final_data.TEMPERING_FLUJO_2 >0]
final_data = final_data[final_data.TEMPERING_FLUJO_3 >0]
final_data = final_data[final_data.TEMPERING_FLUJO_4 >0]
"""
print(f"shaved final data {len(final_data)}")


#initial and final tension date processing
initial_tension = pd.read_csv(resource_path("initial_tension_t.csv"),parse_dates=['FECHA_AMD'])
final_tension = pd.read_csv(resource_path("final_tension_t.csv"),parse_dates=['FECHA_AMD'])

initial_tension['FECHA_AMD'] = pd.to_datetime(initial_tension['FECHA_AMD'],format='%Y-%M-%d')
initial_tension['HORA'] = pd.to_datetime(initial_tension['HORA'], format='%H:%M:%S')
initial_tension['HORA'] = initial_tension['HORA'].dt.time

final_tension['HORA'] = pd.to_datetime(final_tension['HORA'], format='%H:%M:%S')
final_tension['HORA'] = final_tension['HORA'].dt.time
final_tension['FECHA_AMD'] = pd.to_datetime(final_tension['FECHA_AMD'],format='%Y-%M-%d')




inner_join = final_data.merge(initial_tension, on=["line","FECHA_AMD","HORA"],how='right')

inner_join2 = final_data.merge(final_tension, on=["line","FECHA_AMD","HORA"],how='right')
#alternative inner_join
#final_tension = final_tension.rename(columns={'HORA': 'HORA_BU'})
#final_tension = final_tension.rename(columns={'actual_end_hour': 'HORA'})
#final_tension['HORA'] = pd.to_datetime(final_tension['HORA'], format='%H:%M:%S')
#final_tension['HORA'] = final_tension['HORA'].dt.time
#inner_join3 = final_data.merge(final_tension, on=["line","FECHA_AMD","HORA"],how='right')



print(f"inner join 1: {len(inner_join)} /// inner join 2: {len(inner_join2)}")

inner_join.to_csv(resource_path("dataset10_t.csv"),index=False)
inner_join2.to_csv(resource_path("dataset20_t.csv"),index=False)
#inner_join3.to_csv(resource_path("dataset30_t.csv"),index=False)



