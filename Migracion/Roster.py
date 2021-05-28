# Imports
from google_drive_downloader import GoogleDriveDownloader as gdd
import pandas as pd
from gspread_pandas import Spread, conf
from ast import literal_eval

# Funciones
def feudos(i):
    bandera = 0
    if i['City'] == 'Buenos Aires':
        for j in caba:
            if i['Area'] in j['Area']:
                val = j['Feudo']
                bandera = 1
            if bandera == 0:
                val = 'Sin Asignar'
    else:
        for j in resto:
            if i['City'] in j['City']:
                val = j['Feudo']
                bandera = 1
        if bandera == 0:
            val = 'Sin Asignar'
    return val

def reinos(i):
    if i['Feudo'] == 'CAP Z1' or i['Feudo'] == 'CAP Z2' or i['Feudo'] == 'CAP Z3':
        val = 'CABA + ZN'
    else:
        bandera = 0
        for j in resto:
            if i['Feudo'] == j['Feudo']:
                val = j['Reino']
                bandera = 1
        if bandera == 0:
            val = 'Sin Asignar'
    return val

def a_listas_caba(i):
    dicc = {}
    for j in cols_caba:
        if j == 'Area':
            dicc[j] = literal_eval(i[j])
        else:
            dicc[j] = i[j]
    caba.append(dicc)
    
def a_listas_resto(i):
    dicc = {}
    for j in cols_resto:
        if j == 'City':
            dicc[j] = literal_eval(i[j])
        else:
            dicc[j] = i[j]
    resto.append(dicc)

# Credenciales
cred = conf.get_config('C:\\Users\\micaela.fuchs\\Anaconda', 'PedidosYa-6e661fd93faf.json')

# Roster CABA
sheet_id = '1JNywQTVzEQKRwqrJRkpzjiXx5Ly-FldtBMfeSYHuL7w'
wks_name = 'Roster CABA'
sheet = Spread(sheet_id, wks_name, config=cred)
reporte_caba = sheet.sheet_to_df(index=0,header_rows=1)

# Roster Resto
sheet_id = '1JNywQTVzEQKRwqrJRkpzjiXx5Ly-FldtBMfeSYHuL7w'
wks_name = 'Roster Resto'
sheet = Spread(sheet_id, wks_name, config=cred)
reporte_resto = sheet.sheet_to_df(index=0,header_rows=1)

# Creo las columnas
cols_caba = reporte_caba.columns
cols_resto = reporte_resto.columns

# Creo las listas
caba = []
resto = []
reporte_caba.apply(a_listas_caba,axis=1)
reporte_resto.apply(a_listas_resto,axis=1)