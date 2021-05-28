# Imports
from google_drive_downloader import GoogleDriveDownloader as gdd
import pandas as pd
from df2gspread import df2gspread as d2g
from oauth2client.service_account import ServiceAccountCredentials
import gspread
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
scope = ['https://spreadsheets.google.com/feeds'] 
credentials = ServiceAccountCredentials.from_json_keyfile_name('PedidosYa-6e661fd93faf.json', scope) 
gc = gspread.authorize(credentials)

# Roster CABA
reporte_caba = '1JNywQTVzEQKRwqrJRkpzjiXx5Ly-FldtBMfeSYHuL7w'
reporte = gc.open_by_key(reporte_caba)
worksheet_reporte = reporte.worksheet('Roster CABA')
table_reporte = worksheet_reporte.get_all_values()
headers_reporte = table_reporte.pop(0)
reporte_caba = pd.DataFrame(table_reporte, columns=headers_reporte)

# Roster Resto
reporte_resto = '1JNywQTVzEQKRwqrJRkpzjiXx5Ly-FldtBMfeSYHuL7w'
reporte = gc.open_by_key(reporte_resto)
worksheet_reporte = reporte.worksheet('Roster Resto')
table_reporte = worksheet_reporte.get_all_values()
headers_reporte = table_reporte.pop(0)
reporte_resto = pd.DataFrame(table_reporte, columns=headers_reporte)

# Creo las columnas
cols_caba = reporte_caba.columns
cols_resto = reporte_resto.columns

# Creo las listas
caba = []
resto = []
reporte_caba.apply(a_listas_caba,axis=1)
reporte_resto.apply(a_listas_resto,axis=1)