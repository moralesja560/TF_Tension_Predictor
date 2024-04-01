import argparse
from queue import Queue
from threading import Thread
from datetime import datetime
from dotenv import load_dotenv
from urllib.request import Request, urlopen
import json
from urllib.parse import quote
import pyads
import sys
from datetime import datetime
import time
import pandas as pd
import os
import pyodbc

load_dotenv()
token_Tel = os.getenv('TOK_EN_BOT')
Jorge_Morales = os.getenv('JORGE_MORALES')
Label_Group = os.getenv('LABEL_SYSTEM')
# Conexion SQL
server = 'SAL-W12E-SQL\MSSQLMEX' 
database = 'scadadata' 
username = 'scadamex' 
password = 'scadamex'  

""" References to MyDocuments"""
def My_Documents(location):
	import ctypes.wintypes
		#####-----This section discovers My Documents default path --------
		#### loop the "location" variable to find many paths, including AppData and ProgramFiles
	CSIDL_PERSONAL = location       # My Documents
	SHGFP_TYPE_CURRENT = 0   # Get current, not default value
	buf= ctypes.create_unicode_buffer(ctypes.wintypes.MAX_PATH)
	ctypes.windll.shell32.SHGetFolderPathW(None, CSIDL_PERSONAL, None, SHGFP_TYPE_CURRENT, buf)
	#####-------- please use buf.value to store the data in a variable ------- #####
	#add the text filename at the end of the path
	temp_docs = buf.value
	return temp_docs


def send_message(user_id, text,token):
	if opt.noTele:
		return
	global json_respuesta
	url = f"https://api.telegram.org/{token}/sendMessage?chat_id={user_id}&text={text}"
	#resp = requests.get(url)
	#hacemos la petición
	try:
		respuesta  = urlopen(Request(url))
	except Exception as e:
		print(f"Ha ocurrido un error al enviar el mensaje: {e}")
	else:
		#recibimos la información
		cuerpo_respuesta = respuesta.read()
		# Procesamos la respuesta json
		json_respuesta = json.loads(cuerpo_respuesta.decode("utf-8"))
		print("mensaje enviado exitosamente")

def watchdog_t(main_queue,PLC_LT1_queue_i,PLC_LT2_queue_i):
	while True:
		stop_signal = input()
		if stop_signal == "T":
			main_queue.put((None,0))
			PLC_LT1_queue_i.put(None)
			PLC_LT2_queue_i.put(None)
			shutdown_queue.put(None)
		break


def write_log(serial,line):
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	#print("date and time =", dt_string)	
	mis_docs = My_Documents(5)
	pd_ruta = str(mis_docs)+ r"\registro_Tesla_tags.csv"
	pd_file_exists = os.path.exists(pd_ruta)

	#check if pandas DataFrame exists to load the stuff or to create with dummy data.
	if pd_file_exists:
		pd_log = pd.read_csv(pd_ruta)
	else:
		pd_log = pd.DataFrame(pd_dict)

	new_row = {'timestamp' : [dt_string], 'Serial' : [serial], 'Linea' : [line]}
	new_row_pd = pd.DataFrame(new_row)
	pd_concat = pd.concat([pd_log,new_row_pd])
	pd_concat.to_csv(pd_ruta,index=False)



#..............................................................................
"""Function to consume the results"""
def consumer(main_queue,ready_queue):
	#SQL start
	#area to retrieve the df from SQL
	try:
		cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
		query1_22 = r"SELECT [NUM_SERIE],[CARGA],[FECHA],[HORA] FROM LoadTester1_Tesla WHERE FECHA like '%2022%';"
		query2_22 = r"SELECT [NUM_SERIE],[CARGA],[FECHA],[HORA] FROM LoadTester2_Tesla WHERE FECHA like '%2022%';"

		query1_23 = r"SELECT [NUM_SERIE],[CARGA],[FECHA],[HORA] FROM LoadTester1_Tesla WHERE FECHA like '%2023%';"
		query2_23 = r"SELECT [NUM_SERIE],[CARGA],[FECHA],[HORA] FROM LoadTester2_Tesla WHERE FECHA like '%2023%';"

		query1_24 = r"SELECT [NUM_SERIE],[CARGA],[FECHA],[HORA] FROM LoadTester1_Tesla WHERE FECHA like '%2024%';"
		query2_24 = r"SELECT [NUM_SERIE],[CARGA],[FECHA],[HORA] FROM LoadTester2_Tesla WHERE FECHA like '%2024%';"
		
		df1_22 = pd.read_sql(query1_22, cnxn)
		df2_22 = pd.read_sql(query2_22, cnxn)
		
		df1_23 = pd.read_sql(query1_23, cnxn)
		df2_23 = pd.read_sql(query2_23, cnxn)
		
		df1_24 = pd.read_sql(query1_24, cnxn)
		df2_24 = pd.read_sql(query2_24, cnxn)
		
	except:
		cnxn,df1_22,df2_22,df1_23,df2_23,df1_24,df2_24= aux_consumer()

	df22 = pd.concat([df1_22,df2_22])
	df23 = pd.concat([df1_23,df2_23])
	df24 = pd.concat([df1_24,df2_24])
	
	print(f'++++++++++++++++++++++++++++++++++ Consumer: 2022: {len(df22):,} 2023: {len(df23):,} 2024: {len(df24):,} rows')
	serials_list = []
	serials_time = []
	serials_list_2 = []
	serials_time_2 = []
	serial_label = '0'
	line = 0
	#We're ready
	ready_queue.put('s')
	while True:
		# get a unit of work
		assert serial_label=='0', "Failure to reset var SQL"
		serial_label,line = main_queue.get(block=True)
		# check for stop
		if serial_label is None:
			print("Consumer Finished Successfully")
			break
		print(f'			SQL-{line} : {serial_label}')

		if len(serials_list)>600000:
			print(f"DF24 update: Current size {len(df24)}")
			try:
				df1_24 = pd.read_sql(query1_24, cnxn)
				df2_24 = pd.read_sql(query2_24, cnxn)
				df24 = pd.concat([df1_24,df2_24])
			except:
				cnxn,df1_24,df2_24= aux_consumer2()
			#delete the list
			print(f"DF24 update finished: Current size {len(df24)}")
			serials_list = []
			serials_time = []
			serials_list_2 = []
			serials_time_2 = []
		
		df22_found = df22[df22['NUM_SERIE'] == serial_label]
		df23_found = df23[df23['NUM_SERIE'] == serial_label]
		df24_found = df24[df24['NUM_SERIE'] == serial_label]

		if len(df22_found)>0 or len(df23_found)>0 or len(df24_found)>0:
			print(f"Etiqueta duplicada encontrada en SQL. {serial_label}")
			send_message(Label_Group,quote(f"Etiqueta Duplicada en SQL EN Linea {line}: {serial_label}"),token_Tel)
			if len(df22_found)>0:
				send_message(Label_Group,quote(f"Datos SQL22: \n{df22_found}"),token_Tel)
			elif len(df23_found)>0:
				send_message(Label_Group,quote(f"Datos SQL23: \n{df23_found}"),token_Tel)
			elif len(df24_found)>0:
				send_message(Label_Group,quote(f"Datos SQL24: \n{df24_found}"),token_Tel)
			if not opt.noAlarm:
				stop1_queue.put((serial_label,line))

		if serial_label in serials_list and line==1:	
			now = datetime.now()
			times = now.strftime("%d/%m/%y %H:%M:%S")
			send_message(Label_Group,quote(f"Dupli en Lista de Misma Linea: Linea: {line} - {serial_label} timestamp: {serials_time[serials_list.index(serial_label)]} \ntimestamp now {times} "),token_Tel)
			#send_message(Label_Group,quote(f"timestamp original: {serials_time[serials_list.index(serial_label)]} \ntimestamp now {times} "),token_Tel)
			print(f"Dupli en misma linea. {serial_label}")
			print(f"timestamp original: {serials_time[serials_list.index(serial_label)]} \ntimestamp now {times}")
			if not opt.noAlarm:
				stop1_queue.put((serial_label,line))
	
		if serial_label in serials_list_2 and line==2:
			now = datetime.now()
			times = now.strftime("%d/%m/%y %H:%M:%S")
			send_message(Label_Group,quote(f"Dupli en Lista de Misma Linea: Linea: {line} - {serial_label} timestamp: {serials_time_2[serials_list_2.index(serial_label)]} \ntimestamp now {times} "),token_Tel)
			#send_message(Label_Group,quote(f"timestamp original: {serials_time_2[serials_list_2.index(serial_label)]} \ntimestamp now {times} "),token_Tel)
			print(f"Dupli en misma linea. {serial_label}")
			print(f"timestamp original: {serials_time_2[serials_list_2.index(serial_label)]} \ntimestamp now {times}")
			if not opt.noAlarm:
				stop1_queue.put((serial_label,line))

		if serial_label in serials_list_2 and line==1:
			now = datetime.now()
			times = now.strftime("%d/%m/%y %H:%M:%S")
			send_message(Label_Group,quote(f"Dupli en Cross Check. Label está en LT{line} y originalmente salió en LT2. \n{serial_label} \ntimestamp: {serials_time_2[serials_list_2.index(serial_label)]} \ntimestamp now {times}"),token_Tel)
			#now = datetime.now()
			#times = now.strftime("%d/%m/%y %H:%M:%S")
			#send_message(Label_Group,quote(f"timestamp de LT2: {serials_time_2[serials_list_2.index(serial_label)]} \ntimestamp now {times} "),token_Tel)
			print(f"Dupli en cross check. {serial_label}")
			print(f"timestamp original: {serials_time_2[serials_list_2.index(serial_label)]} \ntimestamp now {times}")
			if not opt.noAlarm:
				stop1_queue.put((serial_label,line))
		
		if serial_label in serials_list and line==2:
			now = datetime.now()
			times = now.strftime("%d/%m/%y %H:%M:%S")	
			send_message(Label_Group,quote(f"Dupli en Cross Check. Label está en LT{line} y originalmente salió en LT1. \n{serial_label} \ntimestamp: {serials_time[serials_list.index(serial_label)]} \ntimestamp now {times}"),token_Tel)
			#now = datetime.now()
			#times = now.strftime("%d/%m/%y %H:%M:%S")
			#send_message(Label_Group,quote(f"timestamp de LT1: {serials_time[serials_list.index(serial_label)]} \ntimestamp now {times} "),token_Tel)
			print(f"Dupli en cross check. {serial_label}")
			print(f"timestamp original: {serials_time[serials_list.index(serial_label)]} \ntimestamp now {times}")
			if not opt.noAlarm:
				stop1_queue.put((serial_label,line))


		#After the process we store the label.
		if line == 1:
			serials_list.append(str(serial_label))
			now = datetime.now()
			dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
			serials_time.append(dt_string)
			serial_label = '0'
		if line == 2:
			serials_list_2.append(str(serial_label))
			now = datetime.now()
			dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
			serials_time_2.append(dt_string)
			serial_label = '0'
			

		main_queue.task_done()
		print(f"Processed. {main_queue.qsize()}//LT1:{len(serials_list)}//LT2{len(serials_list_2)}")

			
	# all done
	print('Consumer: Done')

def aux_consumer():
	while True:
		try:
			cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
			query1_22 = r"SELECT [NUM_SERIE],[CARGA],[FECHA], [HORA] FROM LoadTester1_Tesla WHERE FECHA like '%2022%';"
			query2_22 = r"SELECT [NUM_SERIE],[CARGA],[FECHA],[HORA] FROM LoadTester2_Tesla WHERE FECHA like '%2022%';"

			query1_23 = r"SELECT [NUM_SERIE],[CARGA],[FECHA], [HORA] FROM LoadTester1_Tesla WHERE FECHA like '%2023%';"
			query2_23 = r"SELECT [NUM_SERIE],[CARGA],[FECHA],[HORA] FROM LoadTester2_Tesla WHERE FECHA like '%2023%';"

			query1_24 = r"SELECT [NUM_SERIE],[CARGA],[FECHA],[HORA] FROM LoadTester1_Tesla WHERE FECHA like '%2024%';"
			query2_24 = r"SELECT [NUM_SERIE],[CARGA],[FECHA],[HORA] FROM LoadTester2_Tesla WHERE FECHA like '%2024%';"

			df1_22 = pd.read_sql(query1_22, cnxn)
			df2_22 = pd.read_sql(query2_22, cnxn)

			df1_23 = pd.read_sql(query1_23, cnxn)
			df2_23 = pd.read_sql(query2_23, cnxn)

			df1_24 = pd.read_sql(query1_24, cnxn)
			df2_24 = pd.read_sql(query2_24, cnxn)
		except:
			print(f"Auxiliary Consumer: Couldn't open")
			time.sleep(4)
			continue
		else:
			print("Success Consumer")
			return cnxn,df1_22,df2_22,df1_23,df2_23,df1_24,df2_24
		

def aux_consumer2():
	while True:
		try:
			cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
			query1_24 = r"SELECT [NUM_SERIE],[CARGA],[FECHA],[HORA] FROM LoadTester1_Tesla WHERE FECHA like '%2024%';"
			query2_24 = r"SELECT [NUM_SERIE],[CARGA],[FECHA],[HORA] FROM LoadTester2_Tesla WHERE FECHA like '%2024%';"
			df1_24 = pd.read_sql(query1_24, cnxn)
			df2_24 = pd.read_sql(query2_24, cnxn)
		except:
			print(f"Auxiliary Consumer: Couldn't open")
			time.sleep(4)
			continue
		else:
			print("Success Consumer")
			return cnxn,df1_24,df2_24
		
def PLC_comms(PLC_LT1_queue_i,PLC_LT1_queue_o,plc):
	print('++++++++++++++++++++++++++++++++++ PLC1: Running')
	prev_state_sc1 = False
	prev_state_sc2 = False
	sc1_LT1_data = '0'
	sc2_LT1_data = '0'
	sc1_LT1_ok = False
	sc2_LT1_ok = False
	lockout_sc1_LT1 = False
	lockout_sc2_LT1 = False

	try:
		plc.open()
		plc.set_timeout(2000)
		scanner1_LT1_h = plc.get_handle('.Scanner_Blockeinheit1_Extra_Data')
		

		sc1_LT1_ok_h  = plc.get_handle('PB_Labelerkennung.Scanner1_Automatic_Step')
		sc2_LT1_ok_h  = plc.get_handle('PB_Labelerkennung.Scanner2_Automatic_Step')
		
		scanner2_LT1_h = plc.get_handle('.Scanner_Blockeinheit2_Extra_Data')

		permission_LT1 = plc.get_handle('.TP_WS_Abtransport_Klasse2_Kamera_aktivieren')
		forward_pulse = plc.get_handle('.Conveyor_Forward_Pulse')
		
		error_572_1 = plc.get_handle('.TP_ERR_Stoerungen_ARRAY[572]')
		suspicious_ser_1 = plc.get_handle('.TP_IW_Suspicious_Label')
	except Exception as e:
			print(f"Starting error: {e}")
			time.sleep(5)
			plc,scanner1_LT1_h,sc1_LT1_ok_h,scanner2_LT1_h,sc2_LT1_ok_h,permission_LT1,error_572_1,suspicious_ser_1,forward_pulse = aux_PLC_comms()
	while True:
		# get a unit of work
		try:
			item = PLC_LT1_queue_i.get(block=False)
		except:
			item='0000'
			pass

		# check for stop
		if item is None:
			#PLC release and break
			plc.release_handle(scanner1_LT1_h)
			plc.release_handle(sc1_LT1_ok_h)
			plc.release_handle(scanner2_LT1_h)
			plc.release_handle(sc2_LT1_ok_h)
			plc.release_handle(permission_LT1)
			plc.release_handle(error_572_1)
			plc.release_handle(suspicious_ser_1)
			plc.release_handle(forward_pulse)	
			print(f"handles1 released")
			plc.close()
			PLC_LT1_queue_i.task_done()
			break

		#it's time to work.
		try:
			if item[:4] == "SMBD":
				print("Executing Line 1 Stop")
				send_message(Label_Group,quote(f"Executing Line 1 Stop "),token_Tel)
				plc.write_by_name("",True,plc_datatype=pyads.PLCTYPE_BOOL,handle=error_572_1)
				plc.write_by_name("",item,plc_datatype=pyads.PLCTYPE_STRING,handle=suspicious_ser_1)
				PLC_LT1_queue_i.task_done()


			scan_LT1_active = plc.read_by_name("", plc_datatype=pyads.PLCTYPE_BOOL,handle=permission_LT1)
			#scanners ok
			if scan_LT1_active == True:
				scanner1_step_LT1 = plc.read_by_name("", plc_datatype=pyads.PLCTYPE_UINT,handle=sc1_LT1_ok_h)
				scanner2_step_LT1 = plc.read_by_name("", plc_datatype=pyads.PLCTYPE_UINT,handle=sc2_LT1_ok_h)
				conveyor_forward_pulse = plc.read_by_name("", plc_datatype=pyads.PLCTYPE_BOOL,handle=forward_pulse)

				"""
				if sc1_LT1_ok == True and prev_state_sc1 == False:
					# read the value and store it
					sc1_LT1_data = plc.read_by_name("", plc_datatype=pyads.PLCTYPE_STRING,handle=scanner1_LT1_h)
					#print(f"leyendo 1 {sc1_LT1_data}")
					prev_state_sc1 = True

				if sc1_LT1_ok == False and prev_state_sc1 == True:
					prev_state_sc1 = False
					sc1_LT1_data = '0'

				if sc2_LT1_ok == True and prev_state_sc2 == False:
					# read the value and store it
					sc2_LT1_data = plc.read_by_name("", plc_datatype=pyads.PLCTYPE_STRING,handle=scanner2_LT1_h)
					#print(f"				leyendo 2 {sc2_LT1_data}")
					prev_state_sc2 = True

				if sc2_LT1_ok == False and prev_state_sc2 == True:
					prev_state_sc2 = False
					sc2_LT1_data = '0'
				"""
				if scanner1_step_LT1 == 50 and lockout_sc1_LT1 ==False:
					print("										LECTURA SC1 LT1")
					sc1_LT1_data = plc.read_by_name("", plc_datatype=pyads.PLCTYPE_STRING,handle=scanner1_LT1_h)
					#lock the process
					lockout_sc1_LT1 = True
				else:
					sc1_LT1_data ='0'

				if scanner2_step_LT1 == 50 and lockout_sc2_LT1 ==False:
					print("										LECTURA SC2 LT1")
					sc2_LT1_data = plc.read_by_name("", plc_datatype=pyads.PLCTYPE_STRING,handle=scanner2_LT1_h)
					#lock the process
					lockout_sc2_LT1 = True
				else:
					sc2_LT1_data ='0'

				if conveyor_forward_pulse and scanner1_step_LT1 == 0 and lockout_sc1_LT1:
					print("										UNLOCK SC1 LT1")
					lockout_sc1_LT1 = False
					
				if conveyor_forward_pulse and scanner2_step_LT1 == 0 and lockout_sc2_LT1:
					print("										UNLOCK SC2 LT1")
					lockout_sc2_LT1 = False
				
				if sc1_LT1_data != '0':
					PLC_LT1_queue_o.put(sc1_LT1_data)
				if sc2_LT1_data != '0':
					PLC_LT1_queue_o.put(sc2_LT1_data)
			
				sc1_LT1_data = '0'
				sc2_LT1_data = '0'

		except Exception as e:
			print(f"Could not update in PLC1: error {e}")
			sc1_LT1_data = '0'
			sc2_LT1_data = '0'
			PLC_LT1_queue_o.put(sc1_LT1_data)
			PLC_LT1_queue_o.put(sc2_LT1_data)
			plc,scanner1_LT1_h,sc1_LT1_ok_h,scanner2_LT1_h,sc2_LT1_ok_h,permission_LT1,error_572_1,suspicious_ser_1,forward_pulse = aux_PLC_comms()
			continue
				#print(f"PLC1 processed: Sent: {PLC_LT1_queue_o.qsize()} ")

def aux_PLC_comms():

	while True:
		try:
			plc=pyads.Connection('10.65.96.2.1.1', 801, '10.65.96.2')
			plc.open()
			scanner1_LT1_h = plc.get_handle('.Scanner_Blockeinheit1_Extra_Data')
			scanner2_LT1_h = plc.get_handle('.Scanner_Blockeinheit2_Extra_Data')
			forward_pulse = plc.get_handle('.Conveyor_Forward_Pulse')
			permission_LT1 = plc.get_handle('.TP_WS_Abtransport_Klasse2_Kamera_aktivieren')
			error_572_1 = plc.get_handle('.TP_ERR_Stoerungen_ARRAY[572]')
			suspicious_ser_1 = plc.get_handle('.TP_IW_Suspicious_Label')
			
			sc1_LT1_ok_h  = plc.get_handle('PB_Labelerkennung.Scanner1_Automatic_Step')
			sc2_LT1_ok_h  = plc.get_handle('PB_Labelerkennung.Scanner2_Automatic_Step')
		except:
			print(f"Auxiliary PLC_LT1: Couldn't open")
			time.sleep(4)
			continue
		else:
			plc.open()
			print("Success PLC_LT1")
			return plc,scanner1_LT1_h,sc1_LT1_ok_h,scanner2_LT1_h,sc2_LT1_ok_h,permission_LT1,error_572_1,suspicious_ser_1,forward_pulse


def PLC_comms2(PLC_LT2_queue_i,PLC_LT2_queue_o,plc2):
	print('++++++++++++++++++++++++++++++++++ PLC2: Running')
	prev_state_sc2_2 = False
	prev_state_sc1_2 = False
	sc1_LT2_data = '0'
	sc2_LT2_data = '0'
	sc1_LT2_ok = False
	sc2_LT2_ok = False
	lockout_sc1 = False
	lockout_sc2 = False
	
	try:
		plc2.open()
		plc2.set_timeout(2000)
		scanner1_LT2_h = plc2.get_handle('.Scanner_Blockeinheit1_Extra_Data')
		
		sc1_LT2_ok_h  = plc2.get_handle('PB_Final_Inspection.Scanner1_Automatic_Step')
		sc2_LT2_ok_h  = plc2.get_handle('PB_Final_Inspection.Scanner2_Automatic_Step')
		
		forward_pulse = plc2.get_handle('.Conveyor_Forward_Pulse')
		scanner2_LT2_h = plc2.get_handle('.Scanner_Blockeinheit2_Extra_Data')
		permission_LT2 = plc2.get_handle('.TP_WS_Etikettierer_Scanner_Aus')
		error_572 = plc2.get_handle('.TP_ERR_Stoerungen_ARRAY[572]')
		suspicious_ser = plc2.get_handle('.TP_IW_Suspicious_Label')
	except Exception as e:
			print(f"Starting error: {e}")
			time.sleep(5)
			plc2,scanner1_LT2_h,sc1_LT2_ok_h,scanner2_LT2_h,sc2_LT2_ok_h,permission_LT2,error_572,suspicious_ser,forward_pulse = aux_PLC_comms2()
	while True:
		# get a unit of work
		try:
			item2 = PLC_LT2_queue_i.get(block=False)
		except:
			item2='0000'
			pass

		# check for stop
		if item2 is None:
			#PLC release and break
			plc2.release_handle(scanner1_LT2_h)
			plc2.release_handle(sc1_LT2_ok_h)
			plc2.release_handle(scanner2_LT2_h)
			plc2.release_handle(sc2_LT2_ok_h)
			plc2.release_handle(permission_LT2)
			plc2.release_handle(error_572)
			plc2.release_handle(suspicious_ser)
			plc2.release_handle(forward_pulse)
			print(f"handles2 released")
			plc2.close()
			PLC_LT2_queue_i.task_done()
			break
		#it's time to work.
		try:
			if item2[:4] == "SMBD":
				print("Executing Line 2 Stop")
				send_message(Label_Group,quote(f"Executing Line 2 Stop "),token_Tel)
				plc2.write_by_name("",True,plc_datatype=pyads.PLCTYPE_BOOL,handle=error_572)
				plc2.write_by_name("",item2,plc_datatype=pyads.PLCTYPE_STRING,handle=suspicious_ser)
				PLC_LT2_queue_i.task_done()


			scan_LT2_active = plc2.read_by_name("", plc_datatype=pyads.PLCTYPE_BOOL,handle=permission_LT2)
			#scanners ok
			if scan_LT2_active == True:
				#Let's change this code to read only when the auto step is 50. And it can only unlock if the step is 0
				scanner1_step = plc2.read_by_name("", plc_datatype=pyads.PLCTYPE_UINT,handle=sc1_LT2_ok_h)
				scanner2_step = plc2.read_by_name("", plc_datatype=pyads.PLCTYPE_UINT,handle=sc2_LT2_ok_h)
				conveyor_forward_pulse = plc2.read_by_name("", plc_datatype=pyads.PLCTYPE_BOOL,handle=forward_pulse)
				"""
				if sc1_LT2_ok == True and prev_state_sc1_2 ==False:
					# read the value and store it
					sc1_LT2_data = plc2.read_by_name("", plc_datatype=pyads.PLCTYPE_STRING,handle=scanner1_LT2_h)
					prev_state_sc1_2 = True

				if sc1_LT2_ok == False and prev_state_sc1_2 == True:
					prev_state_sc1_2 = False
					sc1_LT2_data = '0'

				if sc2_LT2_ok == True and prev_state_sc2_2 ==False:
					# read the value and store it
					sc2_LT2_data = plc2.read_by_name("", plc_datatype=pyads.PLCTYPE_STRING,handle=scanner2_LT2_h)
					prev_state_sc2_2 = True
				if sc2_LT2_ok == False and prev_state_sc2_2 == True:
					prev_state_sc2_2 = False
					sc2_LT2_data = '0'
				"""
				if scanner1_step == 50 and lockout_sc1 ==False:
					print("																LECTURA SC1 LT2")
					sc1_LT2_data = plc2.read_by_name("", plc_datatype=pyads.PLCTYPE_STRING,handle=scanner1_LT2_h)
					#lock the process
					lockout_sc1 = True
				else:
					sc1_LT2_data ='0'

				if scanner2_step == 50 and lockout_sc2 ==False:
					print("																LECTURA SC2 LT2")
					sc2_LT2_data = plc2.read_by_name("", plc_datatype=pyads.PLCTYPE_STRING,handle=scanner2_LT2_h)
					#lock the process
					lockout_sc2 = True
				else:
					sc2_LT2_data ='0'

				if conveyor_forward_pulse and scanner1_step == 0 and lockout_sc1:
					print("																UNLOCK SC1 LT2")
					lockout_sc1 = False
					
				if conveyor_forward_pulse and scanner2_step == 0 and lockout_sc2:
					print("																UNLOCK SC2 LT2")
					lockout_sc2 = False
				
				if sc1_LT2_data != '0':
					PLC_LT2_queue_o.put(sc1_LT2_data)
				if sc2_LT2_data != '0':
					PLC_LT2_queue_o.put(sc2_LT2_data)
			
				sc1_LT2_data = '0'
				sc2_LT2_data = '0'


		except Exception as e:
			print(f"Could not update in PLC2: error {e}")
			sc1_LT2_data = '0'
			sc2_LT2_data = '0'
			PLC_LT2_queue_o.put(sc1_LT2_data)
			plc2,scanner1_LT2_h,sc1_LT2_ok_h,scanner2_LT2_h,sc2_LT2_ok_h,permission_LT2,error_572,suspicious_ser,forward_pulse = aux_PLC_comms2()
			continue
				#print(f"PLC2 processed: Sent: {PLC_LT1_queue_o.qsize()} ")

def aux_PLC_comms2():

	while True:
		try:
			plc2=pyads.Connection('10.65.96.129.1.1', 801, '10.65.96.129')
			plc2.open()
			scanner1_LT2_h = plc2.get_handle('.Scanner_Blockeinheit1_Extra_Data')
			sc1_LT2_ok_h  = plc2.get_handle('PB_Final_Inspection.Scanner1_Automatic_Step')
			scanner2_LT2_h = plc2.get_handle('.Scanner_Blockeinheit2_Extra_Data')
			sc2_LT2_ok_h  = plc2.get_handle('PB_Final_Inspection.Scanner2_Automatic_Step')
			permission_LT2 = plc2.get_handle('.TP_WS_Etikettierer_Scanner_Aus')
			error_572 = plc2.get_handle('.TP_ERR_Stoerungen_ARRAY[572]')
			suspicious_ser = plc2.get_handle('.TP_IW_Suspicious_Label')
			forward_pulse = plc2.get_handle('.Conveyor_Forward_Pulse')
		except:
			print(f"Auxiliary PLC_LT2: Couldn't open")
			time.sleep(4)
			continue
		else:
			plc.open()
			print("Success PLC_LT2")
			return plc2,scanner1_LT2_h,sc1_LT2_ok_h,scanner2_LT2_h,sc2_LT2_ok_h,permission_LT2,error_572,suspicious_ser,forward_pulse

def process_coordinator():
	scanner_LT1 = '0'
	scanner_LT2 = '0'
	serial_dup = '0'
	linea = 0
	n=0
	stop_testing = False

	while True:

		time.sleep(0.5)
		now = datetime.now()
		times = now.strftime("%d/%m/%y %H:%M:%S")
		#check the PLC queues
		try:
			item = shutdown_queue.get(block=False)
		except:
			pass
		else:
			if item == None:
				print("Closing thread")
				shutdown_queue.task_done()
				break
		assert scanner_LT1=='0', "Failure to reset var LT1"
		try:
			scanner_LT1 = PLC_LT1_queue_o.get(block=False)
		except:
			#print("PLC1 no reportó")
			pass
		else:
			print(f"LT1: dato {scanner_LT1} - {times}")
			PLC_LT1_queue_o.task_done()

		assert scanner_LT2=='0', "Failure to reset var LT2"
		try:
			scanner_LT2 = PLC_LT2_queue_o.get(block=False)
		except:
			#print("PLC2 no reportó")
			pass
		else:
			print(f"             LT2: dato {scanner_LT2}-{times}")
			PLC_LT2_queue_o.task_done()

		if "SMBD" in scanner_LT1:
			main_queue.put((str(scanner_LT1),1))
			write_log(scanner_LT1,1)
		if "SMBD" in scanner_LT2:
			main_queue.put((str(scanner_LT2),2))
			write_log(scanner_LT2,2)
		
		scanner_LT1 = '0'
		scanner_LT2 = '0'



		if opt.noPLC:
			continue

		try:
			serial_dup,linea = stop1_queue.get(block=False)
		except:
			pass
		else:
			if len(serial_dup)>4 and linea > 0:
				#We execute the line stop
				if linea == 2:
					PLC_LT2_queue_i.put(serial_dup)
				elif linea ==1:
					PLC_LT1_queue_i.put(serial_dup)
				stop1_queue.task_done()

			


				
		


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--noPLC', action='store_true', help='Use the counter with no PLC')
	parser.add_argument('--noTele', action='store_true', help='Disable Telegram messaging')
	parser.add_argument('--testing', nargs='+', type=str, default='None', help='Continuous test of labels')
	parser.add_argument('--noAlarm', action='store_true', help='Disable PLC Stop')
	
	opt = parser.parse_args()

	#start consumer and PLC queues
	pd_dict = {'timestamp' : [0], 'Serial' : [0], 'Linea' : [0]}


	
	#Q1 for PLC_LT1 command
	PLC_LT1_queue_i = Queue()
	#Q2 is for PLC_LT2 command
	PLC_LT2_queue_i = Queue()
	
	#Q1 for PLC_LT1 command
	PLC_LT1_queue_o = Queue()
	#Q2 is for PLC_LT2 command
	PLC_LT2_queue_o = Queue()

	#Queue to collect the data we need to check for duplicates.
	main_queue = Queue()
	ready_queue = Queue()
	
	#Queue to shutdown main process
	shutdown_queue = Queue()
	
	#Queue to stop the lines.
	stop1_queue = Queue()

	if not opt.noAlarm:
		send_message(Label_Group,quote(f"Alarm Enabled"),token_Tel)
		# When you write the argument noAlarm, you don't want alarm
		# the NOT before means that if you write the arugment, this if is false.
	if opt.noAlarm:
		send_message(Label_Group,quote(f"Alarm Disabled"),token_Tel)
	
	if opt.noTele:
		send_message(Label_Group,quote(f"Telegram Disabled"),token_Tel)
	if not opt.noTele:
		send_message(Label_Group,quote(f"Telegram Enabled"),token_Tel)


	#Start consumer thread
	consumer = Thread(target=consumer, args=(main_queue,ready_queue),daemon=True)
	consumer.start()

	#Start monitor thread
	monitor_thread = Thread(target=watchdog_t, args=(main_queue,PLC_LT1_queue_i,PLC_LT2_queue_i),daemon=True)
	monitor_thread.start()

	print("Waiting for Consumer")
	ready_2_go = ready_queue.get(block=True)
	ready_queue.task_done()
	print("Ready to start PLC and process coordinator")

	try:
		pyads.open_port()
		ams_net_id = pyads.get_local_address().netid
		print(ams_net_id)
		pyads.close_port()
		plc=pyads.Connection('10.65.96.2.1.1', 801, '10.65.96.2')
		plc.set_timeout(2000)
		PLC_thread = Thread(name="hilo_PLC",target=PLC_comms, args=(PLC_LT1_queue_i,PLC_LT1_queue_o,plc),daemon=True)
	except:
		print("PLC LT1 couldn't be open. Try establishing it first using System Manager")
		sys.exit()
	else:
		PLC_thread.start()
	try:
		pyads.open_port()
		ams_net_id = pyads.get_local_address().netid
		print(ams_net_id)
		pyads.close_port()
		plc2=pyads.Connection('10.65.96.129.1.1', 801, '10.65.96.129')
		plc2.set_timeout(2000)
		PLC_thread2 = Thread(name="hilo_PLC",target=PLC_comms2, args=(PLC_LT2_queue_i,PLC_LT2_queue_o,plc2),daemon=True)
	except:
		print("PLC LT2 couldn't be open. Try establishing it first using System Manager")
		sys.exit()
	else:
		PLC_thread2.start()

	process_coordinator()
	consumer.join()
	monitor_thread.join()
	PLC_thread.join()
	PLC_thread2.join()
	

