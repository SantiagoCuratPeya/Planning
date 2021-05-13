### IMPORTS

import pandas as pd
import numpy as np
import pandas_gbq
import datetime
from dateutil.relativedelta import relativedelta
import calendar
import time
from ast import literal_eval
from gspread_pandas import Spread, conf
import a as a

print('Arranca Argentina')

### CREDENCIALES

cred = conf.get_config('C:\\Users\\santiago.curat\\Pandas\\PEYA', 'PedidosYa-8b8c4d19f61c.json')

### GOOGLE SHEETS

# Managers
sheet_id = '1JNywQTVzEQKRwqrJRkpzjiXx5Ly-FldtBMfeSYHuL7w'
wks_name = 'Managers'
sheet = Spread(sheet_id, wks_name, config=cred)
managers = sheet.sheet_to_df(index=0,header_rows=1)

# Reporte KAM
sheet_id = '1JNywQTVzEQKRwqrJRkpzjiXx5Ly-FldtBMfeSYHuL7w'
wks_name = 'KAM'
sheet = Spread(sheet_id, wks_name, config=cred)
reporte_kams = sheet.sheet_to_df(index=0,header_rows=1)

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

# Roster
sheet_id = '1JNywQTVzEQKRwqrJRkpzjiXx5Ly-FldtBMfeSYHuL7w'
wks_name = 'Restaurant'
sheet = Spread(sheet_id, wks_name, config=cred)
roster = sheet.sheet_to_df(index=0,header_rows=1)

# RCP
sheet_id = '1JNywQTVzEQKRwqrJRkpzjiXx5Ly-FldtBMfeSYHuL7w'
wks_name = 'RCP'
sheet = Spread(sheet_id, wks_name, config=cred)
rcp = sheet.sheet_to_df(index=0,header_rows=1)

# AM Growth
sheet_id = '1JNywQTVzEQKRwqrJRkpzjiXx5Ly-FldtBMfeSYHuL7w'
wks_name = 'AM Growth'
sheet = Spread(sheet_id, wks_name, config=cred)
am_growth = sheet.sheet_to_df(index=0,header_rows=1)

# Sales Growth
sheet_id = '1JNywQTVzEQKRwqrJRkpzjiXx5Ly-FldtBMfeSYHuL7w'
wks_name = 'Sales Growth'
sheet = Spread(sheet_id, wks_name, config=cred)
sl_growth = sheet.sheet_to_df(index=0,header_rows=1)

# Inbound
sheet_id = '1FANWqEVrOiYb_GdNQt3vTb1y9MeBTb6NDXu1e3j148s'
wks_name = 'Roster'
sheet = Spread(sheet_id, wks_name, config=cred)
inbound = sheet.sheet_to_df(index=0,header_rows=1)

#### TRABAJO INBOUND

# Hago lista de los chicos de Inbound
inbounds = [x.upper() for x in inbound[inbound.columns[0]].to_list()] + ['JESUS BAEZ MENDOZA']
inb_camp = str([int(x) for x in inbound[inbound.columns[1]].to_list() if x != '']).replace('[','').replace(']','')

#### TABAJO GROWTH

# Hago lista de AM, Sales y Ciudades de Growth
city_growth = str(am_growth['Ciudad'].to_list()).replace('[','').replace(']','')
am_growth = am_growth['Account'].to_list()
sl_growth = sl_growth['Sales'].to_list()
# Paso todo a mayusculas
am_growth = [x.upper() for x in am_growth]
sl_growth = [x.upper() for x in sl_growth]

#### TRABAJO LOS MANAGERS

manager_caba = managers[managers['Region'] == 'CABA + ZN']['Manager'].values[0]
manager_gba = managers[managers['Region'] == 'ZS + ZO + LP + MDQ']['Manager'].values[0]
manager_noa = managers[managers['Region'] == 'NOA + CBA']['Manager'].values[0]
manager_nea = managers[managers['Region'] == 'NEA + SFE']['Manager'].values[0]
manager_pat = managers[managers['Region'] == 'CENTRO + PAT']['Manager'].values[0]

#### TRABAJO LOS ROSTER DE CIUDADES

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

# Creo las columnas
cols_caba = reporte_caba.columns
cols_resto = reporte_resto.columns

# Creo las listas
caba = []
resto = []
reporte_caba.apply(a_listas_caba,axis=1)
reporte_resto.apply(a_listas_resto,axis=1)

#### TRABAJO LOS KAM

# Franchises KAM
franchises_kam = [x for x in reporte_kams[reporte_kams.columns[0]].to_list() if x is not '']
# KAMs
kams = [x.upper() for x in reporte_kams[reporte_kams.columns[1]].to_list() if x is not '']

#### TRABAJO EL ROSTER

# Roster
roster[roster.columns[0]] = roster[roster.columns[0]].str.upper()
roster.columns = ['Account_Owner','Zona','Cargo','Lider','Manager']

#### TRABAJO RCP

# Hago lista de RCP
rcp = [x.upper() for x in rcp[rcp.columns[0]]]

### CONSTANTES

# Fechas
today = datetime.date.today()
if today.day == 1:
    ftm = str(today - relativedelta(days=1))
    itm = str((today - relativedelta(days=1)).replace(day=1))
else:
    itm = str(today.replace(day=1))
    ftm = str(today + relativedelta(months=1) - relativedelta(days=(today + relativedelta(months=1)).day))

if today.day == 1:
    tm = str(today - relativedelta(days=1))
    lm = str((today - relativedelta(months=2)).replace(day=1))
else:
    tm = str(today + relativedelta(months=1) - relativedelta(days=(today + relativedelta(months=1)).day))
    lm = str((today - relativedelta(months=1)).replace(day=1))

if today.day == 1:
    tm_zombie = today - relativedelta(days=1)
    limite = tm_zombie.replace(day=15)
else:
    tm_zombie = today + relativedelta(months=1) - relativedelta(days=(today + relativedelta(months=1)).day)
    limite = tm_zombie.replace(day=15)

if today.day == 1:
    flm = str(today - relativedelta(days=1) - relativedelta(months=1))
    ilm = str((today - relativedelta(days=1) - relativedelta(months=1)).replace(day=1))
else:
    ilm = str(today.replace(day=1) - relativedelta(months=1))
    flm = str(today - relativedelta(days=today.day))

# Campañas
min_regiones = 3
min_amba = 1

# Low Orders
min_orders = 30

### FUNCIONES

#### FUNCIONES DEL ROSTER

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

#### FUNCIONES DE DATOS PEYA Y AM

def contar_campaigns(i):
    if i['Manager'] == manager_caba or i['Manager'] == manager_gba:
        if i['Confirmed_Campaign TM'] >= min_amba:
            val = 1
        else:
            val = 0
    else:
        if i['Confirmed_Campaign TM'] >= min_regiones:
            val = 1
        else:
            val = 0
    return val

#### FUNCIONES CRUDOS AM

# Valoraciones RR
viernes = 1.4
sabado = 1.4
domingo = 1.4
# Fechas
hoy = datetime.date.today()
if hoy.day == 1:
    hoy = hoy - relativedelta(days=1)
else:
    hoy = hoy
# Armo el RR
calendario = calendar.monthcalendar(hoy.year,hoy.month)
dias = {0:0,1:0,2:0,3:0,4:0,5:0,6:0}
transcurridos = {0:0,1:0,2:0,3:0,4:0,5:0,6:0}
mes = [dias,transcurridos]
hoy = hoy.day
for i in range(7):
    for j in calendario:
        if j[i] != 0:
            dias[i] += 1
        if j[i] < hoy and j[i] != 0:
            transcurridos[i] +=1
final_rr = [0,0]
cont = 0
for j in mes:
    for i in j:
        if i < 4:
            final_rr[cont] += j[i]
        elif i == 4:
            final_rr[cont] += j[i] * viernes
        elif i == 5:
            final_rr[cont] += j[i] * sabado
        else:
            final_rr[cont] += j[i] * domingo
    cont += 1
rr = 0
if final_rr[1] == 0:
    rr = 0.01
else:
    rr = final_rr[1] / final_rr[0]

#### FUNCIONES ZOMBIES

def clasificacion(i):
    if i['Fecha_Cierre'] != '-':
        cierre = datetime.datetime.strptime(i['Fecha_Cierre'],'%Y-%m-%d').date()
        if cierre > limite:
            val = 'New Online'
        else:
            val = 'Activa'
    else:
        if i['Dias_Online LM'] > 0 and i['Confirmed LM'] == 0:
            val = 'RCP Next Month'
        else:
            val = 'Activa'
    return val

####################################################################################################################################################
#
# LOG
#
####################################################################################################################################################

log = pd.DataFrame(columns=['Archivo',str(datetime.date.today())])

### FUNCION CARGA

def carga(url, sheet, df, archivo, log):
    try:
        wks_name = sheet
        sheet_id = url
        sheet = Spread(sheet_id, wks_name, config=cred)
        sheet.df_to_sheet(df, index=False, sheet=wks_name, replace=True)
        log = log.append({'Archivo': archivo, str(datetime.date.today()): 'Correcto'}, ignore_index=True)
        return log
    except:
        log = log.append({'Archivo': archivo, str(datetime.date.today()): 'Error'}, ignore_index=True)
        return log

####################################################################################################################################################
#
# PARTNERS PEYA
#
####################################################################################################################################################

print('###############')
print('Arranca Partners PEYA')

### QUERIES

# 29MB
q_partners = '''WITH pm_table AS(
      SELECT pm.restaurant_id AS Id,
             CASE WHEN pm.has_confirmed_orders THEN 'Si' ELSE 'No' END AS Confirmed_Orders_TM,
             CASE WHEN pm.is_online THEN 'Si' ELSE 'No' END AS Online_TM,
             CASE WHEN pm.is_online AND pm.has_confirmed_orders = FALSE THEN 'Si' ELSE 'No' END AS Zombie,
             CASE WHEN pm.is_new_online THEN 'Si' ELSE 'No' END AS New_Online,
             CASE WHEN pm.is_churned THEN 'Si' ELSE 'No' END AS Churn
      FROM `peya-bi-tools-pro.il_core.fact_partners_monthly` AS pm
      WHERE DATE(pm.full_date) BETWEEN DATE('{0}') AND DATE('{1}'))
SELECT p.salesforce_id AS Grid,
       p.partner_id AS Id,
       p.partner_name AS Name,
       IFNULL(p.franchise.franchise_name,'-') AS Franchise,
       p.city.name AS City,
       a.area_name AS Area,
       CASE WHEN p.is_online THEN 'Si' ELSE 'No' END AS Online,
       CASE WHEN p.accepts_and_supports_vouchers THEN 'Si' ELSE 'No' END AS Accepts_Vouchers,
       CASE WHEN p.has_online_payment THEN 'Si' ELSE 'No' END AS Has_PO,
       CASE WHEN p.is_logistic THEN 'Si' ELSE 'No' END AS Logistic,
       CASE WHEN p.is_concept THEN 'Si' ELSE 'No' END AS Concept,
       IFNULL(p.business_type.business_type_name,'-') AS Business,
       IFNULL(p.main_cousine_category_name,'-') AS Main_Cuisine,
       IFNULL(CAST(p.billingInfo.partner_commission AS STRING),'-') AS Commission,
       IFNULL(CAST(p.first_date_online AS STRING),'-') AS First_Date_Online,
       IFNULL(pm.Confirmed_Orders_TM,'No') AS Confirmed_Orders_TM,
       IFNULL(pm.Online_TM,'No') AS Online_TM,
       IFNULL(pm.Zombie,'No') AS Zombie,
       IFNULL(pm.New_Online,'No') AS New_Online,
       IFNULL(pm.Churn,'No') AS Churn,
       IFNULL(sfu.Name,'-') AS Account_Owner,
       IFNULL(p.partner_status,'-') AS BO_Status,
       IFNULL(sfa.account_status,'-') AS SF_Status,
       IFNULL(CAST(p.qty_products_portal AS STRING),'-') AS Qty_Products,
       IFNULL(CAST(p.qty_picts_portal AS STRING),'-') AS Qty_Photos
FROM `peya-bi-tools-pro.il_core.dim_partner` AS p
LEFT JOIN pm_table AS pm ON p.partner_id = pm.Id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_area` AS a ON p.address.area_id = a.area_id
LEFT JOIN `peya-bi-tools-pro.il_salesforce.dim_salesforce_account` AS sfa ON p.salesforce_id = sfa.account_grid
LEFT JOIN `peya-data-origins-pro.cl_salesforce.user` AS sfu ON sfa.owner_id = sfu.Id
WHERE p.country_id = 3
      AND p.salesforce_id IS NOT NULL'''.format(itm,ftm)

# Descargo la data
hue_partners = pd.io.gbq.read_gbq(q_partners, project_id='peya-argentina', dialect='standard')

# Copio la base
partners = hue_partners.copy()

### TRABAJO

# Coloco el Feudo y Reino
partners['Feudo'] = partners.apply(feudos,axis=1)
partners['Reino'] = partners.apply(reinos,axis=1)

# Marco las KA
partners['Account_Owner'] = partners['Account_Owner'].str.upper()
partners['KAM'] = partners.apply(lambda x: 'Si' if x['Franchise'] in franchises_kam or x['Account_Owner'] in kams else 'No',axis=1)

# Ordeno las columnas
cols = ['Grid','Id','Name','Franchise','City','Area','Feudo','Reino','KAM','Concept','Online','Accepts_Vouchers','Has_PO',
        'Logistic','Business','Main_Cuisine','Commission','First_Date_Online','New_Online','Account_Owner','SF_Status',
        'BO_Status','Churn','Zombie','Confirmed_Orders_TM','Online_TM','Qty_Products','Qty_Photos']
partners = partners[cols]

# Ordeno segun Id
partners.sort_values(by=['Id'],inplace=True)

print('Corrio Partners -',datetime.date.today())

####################################################################################################################################################
#
# DATOS PEYA Y AM
#
####################################################################################################################################################

print('###############')
print('Arranca Datos PEYA y AM')

### QUERIES

# 2.8GB
q_orders = '''SELECT o.restaurant.id AS Id,
       SUBSTR(CAST(o.registered_date AS STRING),1,7) AS Month,
       COUNT(DISTINCT o.order_id) AS Total,
       COUNT(DISTINCT CASE WHEN o.order_status = 'CONFIRMED' THEN o.order_id ELSE NULL END) AS Confirmed,
       COUNT(DISTINCT CASE WHEN o.order_status = 'CONFIRMED' AND o.with_logistics THEN o.order_id ELSE NULL END) AS Logistic,
       COUNT(DISTINCT CASE WHEN o.order_status IN ('PENDING','REJECTED','CANCELLED') AND o.fail_rate_owner = 'Restaurant' THEN o.order_id ELSE NULL END) AS VFR_Num,
       SUM(CASE WHEN o.order_status = 'CONFIRMED' THEN o.total_amount + o.shipping_amount ELSE 0 END) AS GMV,
       SUM(CASE WHEN o.order_status = 'CONFIRMED' THEN o.total_amount ELSE 0 END) AS GFV,
       SUM(CASE WHEN o.order_status = 'CONFIRMED' THEN o.commission_amount ELSE 0 END) AS Revenue,
       SUM(CASE WHEN o.order_status = 'CONFIRMED' THEN o.shipping_amount_no_discount ELSE 0 END) AS DF_No_Discount,
       SUM(CASE WHEN o.with_logistics AND o.order_status = 'CONFIRMED' THEN o.commission_amount ELSE 0 END) AS Log_Revenue,
       SUM(CASE WHEN o.order_status = 'CONFIRMED' THEN CASE WHEN o.with_logistics THEN o.total_amount + o.discount_paid_by_company + o.shipping_amount - o.shipping_amount_no_discount ELSE o.total_amount + o.shipping_amount + o.discount_paid_by_company END ELSE 0 END) AS Income,
       COUNT(DISTINCT CASE WHEN o.order_status = 'CONFIRMED' THEN o.user.id ELSE NULL END) AS Active_Users,
       COUNT(DISTINCT CASE WHEN o.order_status = 'CONFIRMED' AND o.has_voucher_discount > 0 THEN o.order_id ELSE NULL END) AS Confirmed_Voucher,
       COUNT(DISTINCT CASE WHEN o.order_status = 'CONFIRMED' AND o.is_online_payment > 0 THEN o.order_id ELSE NULL END) AS Confirmed_OP
FROM `peya-bi-tools-pro.il_core.fact_orders` AS o
WHERE o.country_id = 3
      AND o.registered_date BETWEEN DATE('{0}') AND DATE('{1}')
GROUP BY 1,2'''.format(lm,tm)

# 1.5GB
q_campaigns = '''SELECT o.restaurant.id AS Id,
       SUBSTR(CAST(o.registered_date AS STRING),1,7) AS Month,
       COUNT(DISTINCT o.order_id) AS Confirmed_Campaign
FROM `peya-bi-tools-pro.il_core.fact_orders` AS o,
UNNEST (details) AS od
LEFT JOIN `peya-bi-tools-pro.il_core.dim_subsidized_product` AS sp ON od.product.product_id = sp.product_id AND od.is_subsidized
LEFT JOIN `peya-bi-tools-pro.il_growth.dim_subsidized_campaign` AS sc ON sp.subsidized_product_campaing_id = sc.subsidized_campaign_id
WHERE o.registered_date BETWEEN DATE('{0}') AND DATE('{1}')
      AND od.is_subsidized
      AND o.order_status = 'CONFIRMED'
      AND o.country_id = 3
      AND sc.subsidized_campaign_id IS NOT NULL
GROUP BY 1,2'''.format(lm,tm)

# 740MB
q_vl = '''SELECT o.restaurant.id AS Id,
       SUBSTR(CAST(o.registered_date AS STRING),1,7) AS Month,
       IFNULL(COUNT(DISTINCT CASE WHEN lo.is_vendor_late_10 = 1 THEN o.order_id ELSE NULL END),0) AS VL_Numerador,
       IFNULL(COUNT(DISTINCT CASE WHEN lo.is_vendor_late_nn = 1 THEN o.order_id ELSE NULL END),0) AS VL_Denominador
FROM `peya-bi-tools-pro.il_logistics.fact_logistic_orders` AS lo
LEFT JOIN `peya-bi-tools-pro.il_core.fact_orders` AS o ON lo.peya_order_id = o.order_id
WHERE lo.created_date_local BETWEEN DATE('{0}') AND DATE('{1}')
      AND o.registered_date BETWEEN DATE('{0}') AND DATE('{1}')
      AND o.country_id = 3
GROUP BY 1,2'''.format(lm,tm)

# 15.2MB
q_partners = '''SELECT p.partner_id AS Id,
       CASE WHEN p.is_online THEN 'Si' ELSE 'No' END AS Online,
       IFNULL(sfu.Name,'-') AS Account_Owner
FROM `peya-bi-tools-pro.il_core.dim_partner` AS p
LEFT JOIN `peya-bi-tools-pro.il_salesforce.dim_salesforce_account` AS sfa ON p.salesforce_id = sfa.account_grid
LEFT JOIN `peya-data-origins-pro.cl_salesforce.user` AS sfu ON sfa.owner_id = sfu.Id
WHERE p.country_id = 3
      AND p.salesforce_id IS NOT NULL'''

# 290MB
q_online = '''SELECT ph.restaurant_id AS Id,
       SUBSTR(CAST(ph.yyyymmdd AS STRING),1,7) AS Month,
       COUNT(DISTINCT ph.date_id) AS Dias_Online
FROM `peya-bi-tools-pro.il_core.dim_historical_partners` AS ph
LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS p ON ph.restaurant_id = p.partner_id
WHERE p.country_id = 3
      AND p.salesforce_id IS NOT NULL
      AND ph.yyyymmdd BETWEEN DATE('{0}') AND DATE('{1}')
      AND ph.is_online
GROUP BY 1,2'''.format(lm,tm)

# 32MB
q_cw = '''SELECT p.partner_id AS Id,
       DATE(cw.closed_date) AS Fecha_Cierre
FROM `peya-bi-tools-pro.il_core.fact_sales_executives_closed_won_v2` AS cw
LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS p ON cw.grid = p.salesforce_id
LEFT JOIN `peya-data-origins-pro.cl_salesforce.opportunity` AS sfo ON cw.oportunity_unique_id = sfo.Id
WHERE DATE(cw.closed_date) BETWEEN DATE('{0}') AND DATE('{1}')
      AND p.country_id = 3
      AND sfo.Business_Type IN ('New Business','Win Back','Franchise Extension','New Bussiness')'''.format(lm,tm)

# 560MB
q_cvr = '''SELECT sbr.restaurant_id AS Id,
       SUBSTR(CAST(sbr.date AS STRING),1,7) AS Month,
       SUM(sbr.sessions) AS Sessions,
       SUM(sbr.unique_orders) AS Transactions
FROM `peya-bi-tools-pro.il_sessions.fact_sessions_by_restaurant` AS sbr
LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS p ON sbr.restaurant_id = p.partner_id
WHERE sbr.date BETWEEN DATE('{0}') AND DATE('{1}')
      AND p.country_id = 3
GROUP BY 1,2'''.format(lm,tm)

# 375MB
q_fr = '''SELECT ph.restaurant_id AS Id,
       SUBSTR(CAST(ph.yyyymmdd AS STRING),1,7) AS Month,
       SUM(CASE WHEN ph.is_online THEN IFNULL(ph.closed_times,0) ELSE NULL END) AS Closed_Time,
       SUM(CASE WHEN ph.is_online THEN IFNULL(ph.schedule_open_time,0) ELSE NULL END) AS Scheduled_Time
FROM `peya-bi-tools-pro.il_core.dim_historical_partners` AS ph
LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS p ON ph.restaurant_id = p.partner_id
WHERE ph.yyyymmdd BETWEEN DATE('{0}') AND DATE('{1}')
      AND ph.is_online
      AND p.salesforce_id IS NOT NULL 
      AND p.country_id = 3
GROUP BY 1,2'''.format(lm,tm)

# Descargo la data
hue_partners = pd.io.gbq.read_gbq(q_partners, project_id='peya-argentina', dialect='standard')
hue_orders = pd.io.gbq.read_gbq(q_orders, project_id='peya-argentina', dialect='standard')
hue_campaigns = pd.io.gbq.read_gbq(q_campaigns, project_id='peya-argentina', dialect='standard')
hue_vl = pd.io.gbq.read_gbq(q_vl, project_id='peya-argentina', dialect='standard')
hue_online = pd.io.gbq.read_gbq(q_online, project_id='peya-argentina', dialect='standard')
hue_cw = pd.io.gbq.read_gbq(q_cw, project_id='peya-argentina', dialect='standard')
hue_cvr = pd.io.gbq.read_gbq(q_cvr, project_id='peya-argentina', dialect='standard')
hue_fr = pd.io.gbq.read_gbq(q_fr, project_id='peya-argentina', dialect='standard')

# Copio las bases
partners_datos = hue_partners.copy()
orders = hue_orders.copy()
campaigns = hue_campaigns.copy()
vl = hue_vl.copy()
online = hue_online.copy()
cw = hue_cw.copy()
cvr = hue_cvr.copy()
fr = hue_fr.copy()

### TRABAJO

#### TRABAJO DATOS

# Comparaciones
tmf = tm[:-3]
lmf = lm[:-3]

# Trabajo Orders
val = [i for i in orders.columns if i not in ['Id','Month']]
orders[val] = orders[val].astype(float)
orders_pt = pd.DataFrame(orders.pivot_table(index=['Id'],columns=['Month'],values=val,aggfunc='sum',fill_value=0).to_records())
# Cambio nombre a columnas
cols = [i.replace("'","").replace('(','').replace(')','').replace(',','').replace(tmf,'TM').replace(lmf,'LM') for i in orders_pt.columns]
orders_pt.columns = cols

# Trabajo Campaigns
val = [i for i in campaigns.columns if i not in ['Id','Month']]
campaigns[val] = campaigns[val].astype(float)
campaigns_pt = pd.DataFrame(campaigns.pivot_table(index=['Id'],columns=['Month'],values=val,aggfunc='sum',fill_value=0).to_records())
# Cambio nombre a columnas
cols = [i.replace("'","").replace('(','').replace(')','').replace(',','').replace(tmf,'TM').replace(lmf,'LM') for i in campaigns_pt.columns]
campaigns_pt.columns = cols

# Trabajo VL
val = [i for i in vl.columns if i not in ['Id','Month']]
vl[val] = vl[val].astype(float)
vl_pt = pd.DataFrame(vl.pivot_table(index=['Id'],columns=['Month'],values=val,aggfunc='sum',fill_value=0).to_records())
# Cambio nombre a columnas
cols = [i.replace("'","").replace('(','').replace(')','').replace(',','').replace(tmf,'TM').replace(lmf,'LM') for i in vl_pt.columns]
vl_pt.columns = cols

# Trabajo Online
val = [i for i in online.columns if i not in ['Id','Month']]
online[val] = online[val].astype(float)
online_pt = pd.DataFrame(online.pivot_table(index=['Id'],columns=['Month'],values=val,aggfunc='sum',fill_value=0).to_records())
# Cambio nombre a columnas
cols = [i.replace("'","").replace('(','').replace(')','').replace(',','').replace(tmf,'TM').replace(lmf,'LM') for i in online_pt.columns]
online_pt.columns = cols

# Trabajo CVR
val = [i for i in cvr.columns if i not in ['Id','Month']]
cvr[val] = cvr[val].astype(float)
cvr_pt = pd.DataFrame(cvr.pivot_table(index=['Id'],columns=['Month'],values=val,aggfunc='sum',fill_value=0).to_records())
# Cambio nombre a columnas
cols = [i.replace("'","").replace('(','').replace(')','').replace(',','').replace(tmf,'TM').replace(lmf,'LM') for i in cvr_pt.columns]
cvr_pt.columns = cols

# Trabajo FR
val = [i for i in fr.columns if i not in ['Id','Month']]
fr[val] = fr[val].astype(float)
fr_pt = pd.DataFrame(fr.pivot_table(index=['Id'],columns=['Month'],values=val,aggfunc='sum',fill_value=0).to_records())
# Cambio nombre a columnas
cols = [i.replace("'","").replace('(','').replace(')','').replace(',','').replace(tmf,'TM').replace(lmf,'LM') for i in fr_pt.columns]
fr_pt.columns = cols

# Cambio las fechas a Str
cw['Fecha_Cierre'] = cw['Fecha_Cierre'].astype(str)

# Coloco los Ids como INT
partners_datos['Id'] = partners_datos['Id'].astype(int)
orders_pt['Id'] = orders_pt['Id'].astype(int)
campaigns_pt['Id'] = campaigns_pt['Id'].astype(int)
vl_pt['Id'] = vl_pt['Id'].astype(int)
online_pt['Id'] = online_pt['Id'].astype(int)
cvr_pt['Id'] = cvr_pt['Id'].astype(int)
fr_pt['Id'] = fr_pt['Id'].astype(int)
cw['Id'] = cw['Id'].astype(int)
# Unifico datos
final = partners_datos.merge(orders_pt,on=['Id'],how='left')
final = final.merge(campaigns_pt,on=['Id'],how='left')
final = final.merge(vl_pt,on=['Id'],how='left')
final = final.merge(online_pt,on=['Id'],how='left')
final = final.merge(cvr_pt,on=['Id'],how='left')
final = final.merge(fr_pt,on=['Id'],how='left')
final.replace([np.nan,np.inf,-np.inf],0,inplace=True)
# Unifico las CW
final = final.merge(cw,on=['Id'],how='left')
final.replace([np.nan,np.inf,-np.inf],'-',inplace=True)

# Ordeno segun Id
final.sort_values(by=['Id'],inplace=True)
# Paso los Owners a Mayuscula
final['Account_Owner'] = final['Account_Owner'].str.upper()
# Declaro Stats
stats = final.copy()
# Declaro Objetivos
objetivos_am = final.copy()

#### TRABAJO OBJETIVOS AM

# Trabajo los Objetivos
objetivos_am['Online Today'] = objetivos_am['Online'].apply(lambda x: 1 if x == 'Si' else 0)
objetivos_am['Online Month'] = objetivos_am['Dias_Online TM'].apply(lambda x: 1 if x > 0 else 0)
objetivos_am['Low Orders'] = objetivos_am.apply(lambda x: 1 if x['Online'] == 'Si' and 0 <= x['Confirmed TM'] <= min_orders else 0,axis=1)
# Hago Merge con el Roster
objetivos_am = objetivos_am.merge(roster,on=['Account_Owner'],how='left')
# Marco los faltantes como "No Restaurant"
objetivos_am.replace([np.nan,np.inf,-np.inf],'No Restaurant',inplace=True)
objetivos_am = objetivos_am[(objetivos_am['Manager'] != 'No Restaurant')&(objetivos_am['Cargo'] == 'Accounts')].copy()
# Trabajo las Campañas
objetivos_am['Cuenta c/Campaña'] = objetivos_am.apply(contar_campaigns,axis=1)

# Creo PT de AM
values = ['Confirmed TM','Logistic TM','VFR_Num TM','Total TM','VL_Numerador TM','VL_Denominador TM','GMV TM',
          'Revenue TM','Log_Revenue TM','Online Today','Online Month','Cuenta c/Campaña','Confirmed_Campaign TM','Low Orders']
objetivos_am = objetivos_am.pivot_table(index=['Account_Owner','Lider','Zona','Manager'],values=values,aggfunc='sum',fill_value=0).reset_index()

# Creo los Objetivos
objetivos_am['%OD'] = objetivos_am['Logistic TM'] / objetivos_am['Confirmed TM']
objetivos_am['%VL10'] = objetivos_am['VL_Numerador TM'] / objetivos_am['VL_Denominador TM']
objetivos_am['VFR'] = objetivos_am['VFR_Num TM'] / objetivos_am['Total TM']
objetivos_am['Take In Log'] = objetivos_am['Log_Revenue TM'] / objetivos_am['Logistic TM']
objetivos_am['%Adhesion Campañas'] = objetivos_am['Cuenta c/Campaña'] / objetivos_am['Online Today']
objetivos_am['%Ordenes Campañas'] = objetivos_am['Confirmed_Campaign TM'] / objetivos_am['Confirmed TM']
objetivos_am['%Low Orders'] = objetivos_am['Low Orders'] / objetivos_am['Online Today']
objetivos_am.replace([np.nan,np.inf,-np.inf],0,inplace=True)
# Hago un sort
objetivos_am.sort_values(by=['Manager','Lider','Zona'],inplace=True)

# Ordeno las columnas
cols = ['Account_Owner','Lider','Zona','Manager','Confirmed TM','Confirmed_Campaign TM','Cuenta c/Campaña','GMV TM','Log_Revenue TM',
        'Logistic TM','Online Month','Online Today','Revenue TM','Total TM','VFR_Num TM','VL_Denominador TM','VL_Numerador TM','%OD',
        '%VL10','VFR','Take In Log','%Adhesion Campañas','%Ordenes Campañas','Low Orders','%Low Orders']
objetivos_am = objetivos_am[cols].copy()

### CARGA

log = carga('1-CxJ2lMGZ8Nhr1oqdEjK4b2R-XjDxaafmIL3KvVezXw','Objetivos AM',objetivos_am,'Datos PEYA y AM',log)

print('Corrio Datos PEYA y AM -', datetime.date.today())

####################################################################################################################################################
#
# KAM
#
####################################################################################################################################################

print('###############')
print('Arranca KAM')

### TRABAJO

# Filtro las columnas a usar
cols = ['Account_Owner','Grid','Id','Franchise','Name','City','Area','Main_Cuisine','Logistic','Online','Online_TM']
# Preparo KAMs TM y LM
kams_tm = partners[partners['KAM'] == 'Si'][cols].copy()
kams_lm = partners[partners['KAM'] == 'Si'][cols].copy()
# Cambio nombre a las columnas para evitar errores con Logistic y las órdenes Logísticas
cols = ['Account_Owner','Grid','Id','Franchise','Name','City','Area','Main_Cuisine','Is Logistic','Online','Online_TM']
kams_tm.columns = cols
kams_lm.columns = cols

# Separo las columnas según TM y LM
cols = ['Active_Users','Confirmed_Voucher','Confirmed','Logistic','Confirmed_OP','VFR_Num','Total',
        'DF_No_Discount','Sessions','Transactions','GMV','Revenue','VL_Denominador','VL_Numerador']
cols_tm = ['Id']+[i+' TM' for i in cols]
cols_lm = ['Id']+[i+' LM' for i in cols]

# Separo las Stats TM y LM
stats_tm = stats[cols_tm].copy()
stats_lm = stats[cols_lm].copy()
# Doy formato a las columnas
stats_tm = stats_tm.astype(float)
stats_lm = stats_lm.astype(float)

# Doy formato a las columnas
kams_tm['Id'] = kams_tm['Id'].astype(float)
kams_lm['Id'] = kams_lm['Id'].astype(float)
# Uno las tablas
kams_tm = kams_tm.merge(stats_tm,on=['Id'],how='left')
kams_lm = kams_lm.merge(stats_lm,on=['Id'],how='left')

# Borro los TM y LM de las columnas
cols_tm = [i.replace(' TM','') for i in kams_tm.columns]
cols_lm = [i.replace(' LM','') for i in kams_lm.columns]
# Reemplazo los nombres de las columnas
kams_tm.columns = cols_tm
kams_lm.columns = cols_lm

# Creo las columnas faltantes
kams_tm['VFR'] = kams_tm['VFR_Num'] / kams_tm['Total']
kams_lm['VFR'] = kams_lm['VFR_Num'] / kams_lm['Total']
kams_tm['CVR'] = kams_tm['Transactions'] / kams_tm['Sessions']
kams_lm['CVR'] = kams_lm['Transactions'] / kams_lm['Sessions']
kams_tm.replace([np.nan,np.inf,-np.inf],0,inplace=True)
kams_lm.replace([np.nan,np.inf,-np.inf],0,inplace=True)
# Cambio el CVR sin Sesiones a '-'
kams_tm['CVR'] = kams_tm['CVR'].apply(lambda x: '-' if x == 0 else x)
kams_lm['CVR'] = kams_lm['CVR'].apply(lambda x: '-' if x == 0 else x)

### CARGA

log = carga('1K_H2gVUHQ0NA9eoCjrAxwgigl_HBJz-PUcbGEdrSMuY','TM',kams_tm,'KAM TM',log)

time.sleep(60)

log = carga('1K_H2gVUHQ0NA9eoCjrAxwgigl_HBJz-PUcbGEdrSMuY','LM',kams_lm,'KAM LM',log)

print('Corrio KAM -', datetime.date.today())

####################################################################################################################################################
#
# CAMPAÑAS PEYA
#
####################################################################################################################################################

print('###############')
print('Arranca Campañas PEYA')

### QUERIES

# 840MB
q_cam = '''SELECT o.restaurant.id AS Id,
       sc.subsidized_campaign_name AS Campaign_Name,
       COUNT(DISTINCT o.order_id) AS Confirmed
FROM `peya-bi-tools-pro.il_core.fact_orders` AS o,
UNNEST (details) AS od
LEFT JOIN `peya-bi-tools-pro.il_core.dim_subsidized_product` AS sp ON od.product.product_id = sp.product_id AND od.is_subsidized
LEFT JOIN `peya-bi-tools-pro.il_growth.dim_subsidized_campaign` AS sc ON sp.subsidized_product_campaing_id = sc.subsidized_campaign_id
WHERE o.registered_date BETWEEN DATE('{0}') AND DATE('{1}')
      AND od.is_subsidized
      AND o.order_status = 'CONFIRMED'
      AND o.country_id = 3
      AND sc.subsidized_campaign_id IS NOT NULL
GROUP BY 1,2'''.format(itm,ftm)

# Descargo la data
hue_cam = pd.io.gbq.read_gbq(q_cam, project_id='peya-argentina', dialect='standard')

# Copio las bases
cam = hue_cam.copy()

### TRABAJO

#### TRABAJO CAMPAÑAS

# Doy formato a las columnas
val = [i for i in cam.columns if i not in ['Id','Campaign_Name']]
cam[val] = cam[val].astype(float)

# Ordeno las columnas segun cantidad de ordenes
campaigns = cam.pivot_table(index=['Campaign_Name'],values=['Confirmed'],aggfunc='sum',fill_value=0).reset_index()
campaigns.sort_values(by=['Confirmed'],ascending=False,inplace=True)
campaigns_cols = campaigns['Campaign_Name'].to_list()

# Creo la PT de campañas
campaigns = pd.DataFrame(cam.pivot_table(index=['Id'],columns=['Campaign_Name'],values=val,aggfunc='sum',fill_value=0).to_records())
# Cambio nombre a columnas
cols = [i.replace("'","").replace('(','').replace(')','').replace(', ','').replace('Confirmed','') for i in campaigns.columns]
campaigns.columns = cols

# Preparo el merge de las tablas
campaigns['Id'] = campaigns['Id'].astype(int)
partners['Id'] = partners['Id'].astype(int)
# Uno las tablas
cols = ['Id','Grid','Account_Owner','Name','Franchise','Business','City','Area','Feudo','Reino','KAM','Concept','Online']
final_cam = campaigns.merge(partners[cols],on=['Id'],how='left')
# Elimino las columnas sin Grid
final_cam.dropna(subset=['Grid'],inplace=True)

# Ordeno las columnas
cols = cols + campaigns_cols
final_cam = final_cam[cols].copy()

#### TRABAJO ACCOUNTS

# Creo la PT de Accounts
accounts = final_cam.pivot_table(index=['Account_Owner'],values=campaigns_cols,aggfunc='sum',fill_value=0).reset_index()
# Ordeno las columnas
accounts = accounts[['Account_Owner'] + campaigns_cols].copy()

# Me quedo con las columnas de Objetivos que me sirven
cols_obj = ['Account_Owner','Lider','Zona','Manager','Cuenta c/Campaña','%Adhesion Campañas','Confirmed TM','Confirmed_Campaign TM','%Ordenes Campañas']
objetivos_cam = objetivos_am[cols_obj].copy()
# Uno las tablas
final_acc = objetivos_cam.merge(accounts,on=['Account_Owner'],how='left')
final_acc.replace([np.nan,np.inf,-np.inf],0,inplace=True)

### CARGA

log = carga('1PnxQ42g6YDmUPh28ldmU_cD_iFUPyK8u0wrhcNf36p0','Crudo Campañas',final_cam,'Crudo Campañas',log)

time.sleep(60)

log = carga('1PnxQ42g6YDmUPh28ldmU_cD_iFUPyK8u0wrhcNf36p0','Accounts',final_acc,'Crudo Campañas Accounts',log)

print('Corrio Campañas PEYA -',datetime.date.today())

####################################################################################################################################################
#
# CRUDOS AM
#
####################################################################################################################################################

print('###############')
print('Arranca Crudo AM')

### TRABAJO

# Filtro las columnas de stats
stats_crudos = stats.drop(['Online','Account_Owner','Fecha_Cierre'],axis=1).copy()
# Doy formato a las columnas
cols = [i for i in stats_crudos.columns if i not in ['Id']]
stats_crudos[cols] = stats_crudos[cols].astype(float)

# Doy formato a las columnas para unir las tablas
partners['Id'] = partners['Id'].astype(int)
stats_crudos['Id'] = stats_crudos['Id'].astype(int)
# Uno las tablas
final = partners.merge(stats_crudos,on=['Id'],how='left')

# Selecciono las columnas finales
cols = ['Grid','Id','Name','Franchise','City','Area','Feudo','Reino','KAM','Concept','Online','Accepts_Vouchers','Has_PO','Logistic',
        'Business','Main_Cuisine','Commission','First_Date_Online','New_Online','Account_Owner','SF_Status','BO_Status','Churn',
        'Zombie','Online_TM','Active_Users TM','Active_Users LM','Confirmed TM','Confirmed LM','Logistic TM','Logistic LM',
        'Confirmed_OP TM','Confirmed_OP LM','Dias_Online TM','Dias_Online LM','GMV TM','GMV LM','VFR_Num TM','VFR_Num LM','Total TM',
        'Total LM','Revenue TM','Revenue LM','VL_Numerador TM','VL_Numerador LM','VL_Denominador TM','VL_Denominador LM',
        'Sessions TM','Sessions LM','Transactions TM','Transactions LM','Confirmed_Campaign TM','Confirmed_Campaign LM','Closed_Time TM',
        'Closed_Time LM','Scheduled_Time TM','Scheduled_Time LM']
final = final[cols].copy()

# Calculo los campos necesarios
final['Orders W/RR'] = final['Confirmed TM'] / rr
final['ΔOrders W/RR'] = (final['Orders W/RR'] - final['Confirmed LM']) / final['Confirmed LM']
final['%OD TM'] = final['Logistic TM'] / final['Confirmed TM']
final['%OD LM'] = final['Logistic LM'] / final['Confirmed LM']
final['Δ%OD'] = (final['%OD TM'] - final['%OD LM']) / final['%OD LM']
final['VFR TM'] = final['VFR_Num TM'] / final['Total TM']
final['VFR LM'] = final['VFR_Num LM'] / final['Total LM']
final['ΔVFR'] = (final['VFR TM'] - final['VFR LM']) /final['VFR LM']
final['Avg Basket Size TM'] = final['GMV TM'] / final['Confirmed TM']
final['Avg Basket Size LM'] = final['GMV LM'] / final['Confirmed LM']
final['ΔAvg Basket Size'] = (final['Avg Basket Size TM'] - final['Avg Basket Size LM']) /final['Avg Basket Size LM']
final['GMV W/RR'] = final['GMV TM'] / rr
final['ΔGMV W/RR'] = (final['GMV W/RR'] - final['GMV LM']) / final['GMV LM']
final['Revenue W/RR'] = final['Revenue TM'] / rr
final['ΔRevenue W/RR'] = (final['Revenue W/RR'] - final['Revenue LM']) / final['Revenue LM']
final['Take-In TM'] = final['Revenue TM'] / final['Confirmed TM']
final['Take-In LM'] = final['Revenue LM'] / final['Confirmed LM']
final['ΔTake-In'] = (final['Take-In TM'] - final['Take-In LM']) / final['Take-In LM']
final['VL10 TM'] = final['VL_Numerador TM'] / final['VL_Denominador TM']
final['VL10 LM'] = final['VL_Numerador LM'] / final['VL_Denominador LM']
final['ΔVL10'] = (final['VL10 TM'] - final['VL10 LM']) / final['VL10 LM']
final['CVR TM'] = final['Transactions TM'] / final['Sessions TM']
final['CVR LM'] = final['Transactions LM'] / final['Sessions LM']
final['ΔCVR'] = (final['CVR TM'] - final['CVR LM']) / final['CVR LM']
final['%Campaign Orders TM'] = final['Confirmed_Campaign TM'] / final['Confirmed TM']
final['%Campaign Orders LM'] = final['Confirmed_Campaign LM'] / final['Confirmed LM']
final['Δ%Campaign Orders'] = (final['%Campaign Orders TM'] - final['%Campaign Orders LM']) / final['%Campaign Orders LM']
final['Churn NM'] = final.apply(lambda x: 'Si' if x['Dias_Online TM'] > 0 and x['Online'] == 'No' else 'No',axis=1)
final['%Open Time TM'] = (final['Scheduled_Time TM'] - final['Closed_Time TM']) / final['Scheduled_Time TM']
final['%Open Time LM'] = (final['Scheduled_Time LM'] - final['Closed_Time LM']) / final['Scheduled_Time LM']
final['Δ%Open Time'] = (final['%Open Time TM'] - final['%Open Time LM']) / final['%Open Time LM']
final.replace([np.nan,np.inf,-np.inf],0,inplace=True)

# Redondeo todo a 4 decimales
final = final.copy().round(4)

# Hago Merge con el Roster
final = final.merge(roster,on=['Account_Owner'],how='left')
# Marco los faltantes como "No Restaurant"
final.replace([np.nan,np.inf,-np.inf],'No Restaurant',inplace=True)

# Columnas finales
cols = ['Grid','Account_Owner','Online','Online_TM','Name','Franchise','City','Area','Feudo','Reino','Business','KAM','Concept',
        'Logistic','Accepts_Vouchers','Has_PO','Main_Cuisine','Commission','Zombie','Churn','Churn NM','SF_Status','BO_Status',
        'Active_Users TM','Active_Users LM','Confirmed TM','Confirmed LM','Orders W/RR','ΔOrders W/RR','Logistic TM','Logistic LM',
        '%OD TM','%OD LM','Δ%OD','VFR_Num TM','VFR_Num LM','VFR TM','VFR LM','ΔVFR','Avg Basket Size TM','Avg Basket Size LM',
        'ΔAvg Basket Size','GMV TM','GMV LM','GMV W/RR','ΔGMV W/RR','Revenue TM','Revenue LM','Revenue W/RR','ΔRevenue W/RR',
        'Take-In TM','Take-In LM','ΔTake-In','VL_Numerador TM','VL_Numerador LM','VL_Denominador TM','VL_Denominador LM',
        'VL10 TM','VL10 LM','ΔVL10','Sessions TM','Sessions LM','CVR TM','CVR LM','ΔCVR','Confirmed_Campaign TM','Confirmed_Campaign LM',
        '%Campaign Orders TM','%Campaign Orders LM','Δ%Campaign Orders','%Open Time TM','%Open Time LM','Δ%Open Time','Zona',
        'Cargo','Lider','Manager']
final = final[cols].copy()

# Separo las Regiones
crudo_caba = final[((final['Reino'] == 'CABA + ZN')&(final['Online'] == 'Si'))|((final['Cargo'] == 'Accounts')&(final['Manager'] == manager_caba))].copy()
crudo_gba = final[((final['Reino'] == 'ZS + ZO + LP + MDQ')&(final['Online'] == 'Si'))|((final['Cargo'] == 'Accounts')&(final['Manager'] == manager_gba))].copy()
crudo_noa = final[((final['Reino'] == 'NOA + CBA')&(final['Online'] == 'Si'))|((final['Cargo'] == 'Accounts')&(final['Manager'] == manager_noa))].copy()
crudo_nea = final[((final['Reino'] == 'NEA + SFE')&(final['Online'] == 'Si'))|((final['Cargo'] == 'Accounts')&(final['Manager'] == manager_nea))].copy()
crudo_pat = final[((final['Reino'] == 'CENTRO + PAT')&(final['Online'] == 'Si'))|((final['Cargo'] == 'Accounts')&(final['Manager'] == manager_pat))].copy()

### CARGA

log = carga('1td-XAs05rs0Z33sEkx3efVBYsxo4uvhLHrdWVb6sJOM','Crudo BQ',crudo_gba,'Crudo GBA',log)

time.sleep(60)

log = carga('1wpTcOVRQSFg63DXB1ZjBEYBS6mTBSu2KYQmIVrtf5wk','Crudo BQ',crudo_caba,'Crudo CABA',log)

time.sleep(60)

log = carga('1rMXgevezhvZs4XV_AvaS3SzIvPM-6gSICQtWMTwbgUY','Crudo BQ',crudo_noa,'Crudo NOA',log)

time.sleep(60)

log = carga('1tojAd2UUHmwAf5LvPZAUCNK3BsoWeBcRyCgXttTM2UI','Crudo BQ',crudo_nea,'Crudo NEA',log)

time.sleep(60)

log = carga('17mZ94hiqrZpA8x-_5oI_1PMMCh5O_UXHLX96fmTLQXA','Crudo BQ',crudo_pat,'Crudo PAT',log)

print('Corrio Crudo AM -',datetime.date.today())

####################################################################################################################################################
#
# ZOMBIES
#
####################################################################################################################################################

print('###############')
print('Arranca Zombies')

### TRABAJO

#### TRABAJO ZOMBIES

# Doy formato a las columnas de Stats
cols = [i for i in stats.columns if i not in ['Id','Fecha_Cierre','Online','Account_Owner']]
stats_zombies = stats.copy()
stats_zombies[cols] = stats_zombies[cols].astype(float)

# Filtro las columnas a usar
cols_partners = ['Id','Grid','Online','Account_Owner','Area','Feudo','Reino','KAM','Business','Concept']
cols_stats = ['Id','Fecha_Cierre','Dias_Online TM','Dias_Online LM','Confirmed TM','Confirmed LM']
partners_zombies = partners[cols_partners].copy()
stats_zombies = stats_zombies[cols_stats].copy()

# Doy formato a las columnas
partners_zombies['Id'] = partners_zombies['Id'].astype(int)
stats_zombies['Id'] = stats_zombies['Id'].astype(int)
# Hago el merge
final = partners_zombies.merge(stats_zombies,how='left')
final.replace([np.nan,np.inf,-np.inf],0,inplace=True)

# Coloco la Clasificacion
final['Clasificacion'] = final.apply(clasificacion,axis=1)
# Marco los Zombies
final['Zombie Empresa'] = final.apply(lambda x: 'Si' if x['Confirmed TM'] == 0 and x['Dias_Online TM'] > 0 else 'No',axis=1)
final['Zombie Account'] = final.apply(lambda x: 'Si' if x['Zombie Empresa'] == 'Si' and x['Online'] == 'Si' else 'No',axis=1)

# Ordeno las columnas
cols = ['Id','Grid','Online','Fecha_Cierre','Dias_Online LM','Dias_Online TM','Confirmed LM','Confirmed TM','Clasificacion',
        'Zombie Empresa','Zombie Account']
base_zombies = final[cols].copy()
# Ordeno segun Id
base_zombies.sort_values(by=['Id'],inplace=True)

#### TRABAJO OBJETIVO ZOMBIES

# Marco las RCP
final['Account_Owner'] = final['Account_Owner'].str.upper()
final['RCP'] = final['Account_Owner'].apply(lambda x: 'Si' if x in rcp else 'No')

# Coloco el Business Final
final['Business Final'] = final['Business'].apply(lambda x: 'Restaurant' if x == 'Restaurant' else 'NV')
# Creo una columna de base para contas las cuentas
final['Carteras'] = final['Dias_Online TM'].apply(lambda x: 1 if x > 0 else 0)

# Hago una PT para las Carteras
carteras_cm = pd.pivot_table(final,index=['Reino','Feudo','KAM','Concept','RCP','Business Final','Clasificacion'],values=['Carteras'],aggfunc='sum',fill_value=0).reset_index()
carteras_am = pd.pivot_table(final,index=['Account_Owner','Clasificacion'],values=['Carteras'],aggfunc='sum',fill_value=0).reset_index()

# Separo los Zombies
zombies = final[final['Zombie Empresa'] == 'Si'].copy()
# Ordeno las columnas
cols = ['Grid','Id','Area','Feudo','Reino','KAM','Business','Account_Owner','Concept','Online','Fecha_Cierre',
        'Dias_Online LM','Dias_Online TM','Confirmed LM','Confirmed TM','Clasificacion','Zombie Empresa','Zombie Account',
        'RCP','Carteras','Business Final']
zombies = zombies[cols].copy()

### CARGA

log = carga('11PbmF7vo_A1wfcSIY00iwy6lU04LjyWLjZ4SPebyqbA','Zombies PEYA',base_zombies,'Zombies PEYA',log)

time.sleep(60)

log = carga('1CHBlR8cFKRy-ycRdf_NAcTvGhA9TgfaKytJSJENM5xk','Cuentas RM-CM',carteras_cm,'Cuentas RM-CM Zombies',log)

time.sleep(60)

log = carga('1CHBlR8cFKRy-ycRdf_NAcTvGhA9TgfaKytJSJENM5xk','Cuentas Accounts',carteras_am,'Cuentas AM Zombies',log)

time.sleep(60)

log = carga('1CHBlR8cFKRy-ycRdf_NAcTvGhA9TgfaKytJSJENM5xk','Zombies',zombies,'Zombies Pais',log)

print('Corrio Zombies -',datetime.date.today())

####################################################################################################################################################
#
# TARGETS HEADS
#
####################################################################################################################################################

print('###############')
print('Arranca Targets Heads')

### QUERIES

# 862MB
q_nps = '''SELECT c.city_name AS City,
       a.area_name AS Area,
       o.business_type.business_type_name AS Business,
       1.0 * ((COUNT(DISTINCT CASE WHEN (q.nps_group = 'Promoter') THEN CASE WHEN q.response_id IS NULL THEN '0' ELSE q.response_id END ELSE NULL END)) - (COUNT(DISTINCT CASE WHEN (q.nps_group = 'Detractor') THEN CASE WHEN q.response_id IS NULL THEN '0' ELSE q.response_id END ELSE NULL END))) AS NPS_AO_Numerador,
       1.0 * (CASE WHEN (COUNT(DISTINCT CASE WHEN q.response_id IS NULL THEN '0' ELSE q.response_id END)) = 0 THEN 1 ELSE (COUNT(DISTINCT CASE WHEN q.response_id IS NULL THEN '0' ELSE q.response_id END)) END) AS NPS_AO_Denominador
FROM `peya-bi-tools-pro.il_nps.fact_qualtrics` AS q
LEFT JOIN `peya-bi-tools-pro.il_core.fact_orders` AS o ON q.order_id = o.order_id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_area` AS a ON o.address.area.id = a.area_id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_city` AS c ON o.city.city_id = c.city_id
WHERE o.country_id = 3
      AND o.registered_date BETWEEN DATE('{0}') AND DATE('{1}')
GROUP BY 1,2,3'''.format(itm,ftm)

# 1.1GB
q_orders = '''SELECT c.city_name AS City,
       a.area_name AS Area,
       o.business_type.business_type_name AS Business,
       COUNT(DISTINCT o.order_id) AS Total,
       COUNT(DISTINCT CASE WHEN o.order_status = 'CONFIRMED' THEN o.order_id ELSE NULL END) AS Confirmed,
       COUNT(DISTINCT CASE WHEN o.order_status = 'CONFIRMED' AND o.with_logistics THEN o.order_id ELSE NULL END) AS Logistic,
       COUNT(DISTINCT CASE WHEN o.order_status IN ('PENDING','REJECTED','CANCELLED') THEN o.order_id ELSE NULL END) AS FR_Num,
       COUNT(DISTINCT CASE WHEN o.order_status IN ('PENDING','REJECTED','CANCELLED') AND o.fail_rate_owner = 'Restaurant' THEN o.order_id ELSE NULL END) AS VFR_Num,
       SUM(CASE WHEN o.order_status = 'CONFIRMED' THEN o.commission_amount ELSE 0 END) AS Revenue,
       SUM(CASE WHEN o.with_logistics AND o.order_status = 'CONFIRMED' THEN o.commission_amount ELSE 0 END) AS Log_Revenue,
       SUM(CASE WHEN o.with_logistics AND o.order_status = 'CONFIRMED' THEN o.shipping_amount_no_discount ELSE 0 END) AS DF_Log
FROM `peya-bi-tools-pro.il_core.fact_orders` AS o
LEFT JOIN `peya-bi-tools-pro.il_core.dim_area` AS a ON o.address.area.id = a.area_id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_city` AS c ON o.city.city_id = c.city_id
WHERE o.country_id = 3
      AND o.registered_date BETWEEN DATE('{0}') AND DATE('{1}')
GROUP BY 1,2,3'''.format(itm,ftm)

# 1.1GB
q_vl = '''SELECT c.city_name AS City,
       a.area_name AS Area,
       o.business_type.business_type_name AS Business,
       IFNULL(COUNT(DISTINCT CASE WHEN lo.is_vendor_late_10 = 1 THEN o.order_id ELSE NULL END),0) AS VL_Numerador,
       IFNULL(COUNT(DISTINCT CASE WHEN lo.is_vendor_late_nn = 1 THEN o.order_id ELSE NULL END),0) AS VL_Denominador,
       SUM(lo.timings.actual_delivery_time*1.00/60) AS DT_Numerador,
       COUNT(DISTINCT lo.peya_order_id) AS DT_Denominador
FROM `peya-bi-tools-pro.il_logistics.fact_logistic_orders` AS lo
LEFT JOIN `peya-bi-tools-pro.il_core.fact_orders` AS o ON lo.peya_order_id = o.order_id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_area` AS a ON o.address.area.id = a.area_id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_city` AS c ON o.city.city_id = c.city_id
WHERE lo.created_date_local BETWEEN DATE('{0}') AND DATE('{1}')
      AND o.registered_date BETWEEN DATE('{0}') AND DATE('{1}')
      AND o.country_id = 3
GROUP BY 1,2,3'''.format(itm,ftm)

# 8.1MB
q_online = '''SELECT p.city.name AS City,
       a.area_name AS Area,
       p.business_type.business_type_name AS Business,
       COUNT(DISTINCT CASE WHEN pm.is_online THEN p.partner_id ELSE NULL END) AS Online,
       COUNT(DISTINCT CASE WHEN pm.is_churned THEN p.partner_id ELSE NULL END) AS Churns,
       COUNT(DISTINCT CASE WHEN pm.is_online AND pm.has_confirmed_orders = FALSE THEN p.partner_id ELSE NULL END) AS Zombies
FROM `peya-bi-tools-pro.il_core.fact_partners_monthly` AS pm
LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS p ON pm.restaurant_id = p.partner_id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_area` AS a ON p.address.area_id = a.area_id
WHERE p.country_id = 3
      AND DATE(pm.full_date) BETWEEN DATE('{0}') AND DATE('{1}')
GROUP BY 1,2,3'''.format(itm,ftm)

# 24MB
q_zc = '''WITH zc_table AS(
      SELECT p.partner_id AS Id,
            IFNULL(p.salesforce_id,'-') AS Grid,
            p.partner_name AS Name,
            IFNULL(p.franchise.franchise_name,'-') AS Franchise,
            p.business_type.business_type_name AS Business,
            p.city.name AS City,
            a.area_name AS Area,
            CASE WHEN p.is_online THEN 'Si' ELSE 'No' END AS Online,
            IFNULL(sfu.Name,'-') AS Account_Owner,
            CASE WHEN pm.is_churned THEN 'Churn'
                 WHEN pm.is_online AND pm.has_confirmed_orders = FALSE THEN 'Zombie'
                 ELSE '-' END AS Zombie_Churn
      FROM `peya-bi-tools-pro.il_core.fact_partners_monthly` AS pm
      LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS p ON pm.restaurant_id = p.partner_id
      LEFT JOIN `peya-bi-tools-pro.il_core.dim_area` AS a ON p.address.area_id = a.area_id
      LEFT JOIN `peya-bi-tools-pro.il_salesforce.dim_salesforce_account` AS sfa ON p.salesforce_id = sfa.account_grid
      LEFT JOIN `peya-data-origins-pro.cl_salesforce.user` AS sfu ON sfa.owner_id = sfu.Id
      WHERE p.country_id = 3
            AND DATE(pm.full_date) BETWEEN DATE('{0}') AND DATE('{1}'))
SELECT zc.*
FROM zc_table AS zc
WHERE zc.Zombie_Churn IN ('Zombie','Churn')'''.format(itm,ftm)

# 50.3MB
q_cw = '''SELECT p.salesforce_id AS Grid,
       sfo.Business_Type AS Tipo,
       sfc.Name__c AS Nombre,
       cw.account_owner AS Owner,
       MAX(sfc.Commission) AS Comision
FROM `peya-bi-tools-pro.il_core.fact_sales_executives_closed_won_v2` AS cw
LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS p ON cw.grid = p.salesforce_id
LEFT JOIN `peya-data-origins-pro.cl_salesforce.opportunity` AS sfo ON cw.oportunity_unique_id = sfo.Id
LEFT JOIN `peya-data-origins-pro.cl_salesforce.contract` AS sfc ON sfo.Id = sfc.Id_Opportunity
WHERE DATE(cw.closed_date) BETWEEN DATE('{0}') AND DATE('{1}')
      AND p.country_id = 3
      AND sfo.Business_Type IN ('New Business','Win Back','Franchise Extension','New Bussiness')
GROUP BY 1,2,3,4'''.format(itm,ftm)

# Descargo la data
hue_nps = pd.io.gbq.read_gbq(q_nps, project_id='peya-argentina', dialect='standard')
hue_orders = pd.io.gbq.read_gbq(q_orders, project_id='peya-argentina', dialect='standard')
hue_vl = pd.io.gbq.read_gbq(q_vl, project_id='peya-argentina', dialect='standard')
hue_online = pd.io.gbq.read_gbq(q_online, project_id='peya-argentina', dialect='standard')
hue_zc = pd.io.gbq.read_gbq(q_zc, project_id='peya-argentina', dialect='standard')
hue_cw = pd.io.gbq.read_gbq(q_cw, project_id='peya-argentina', dialect='standard')

# Copio las bases
nps = hue_nps.copy()
orders = hue_orders.copy()
vl = hue_vl.copy()
online = hue_online.copy()
zc = hue_zc.copy()
cw = hue_cw.copy()

### TRABAJO

#### TRABAJO LOS TARGETS

# Trabajo NPS
val = [i for i in nps.columns if i not in ['City','Area','Business']]
nps[val] = nps[val].astype(float)
# Trabajo Orders
val = [i for i in orders.columns if i not in ['City','Area','Business']]
orders[val] = orders[val].astype(float)
# Trabajo VL
val = [i for i in vl.columns if i not in ['City','Area','Business']]
vl[val] = vl[val].astype(float)
# Trabajo Online
val = [i for i in online.columns if i not in ['City','Area','Business']]
online[val] = online[val].astype(float)

# Uno las tablas
targets_heads = orders.merge(nps,on=['City','Area','Business'],how='outer')
targets_heads = targets_heads.merge(vl,on=['City','Area','Business'],how='outer')
targets_heads = targets_heads.merge(online,on=['City','Area','Business'],how='outer')
targets_heads.replace([np.nan,np.inf,-np.inf],0,inplace=True)

# Coloco el Feudo y Reino
targets_heads['Feudo'] = targets_heads.apply(feudos,axis=1)
targets_heads['Reino'] = targets_heads.apply(reinos,axis=1)

# Divido según Vertical
targets_heads['Restaurants/NV'] = targets_heads['Business'].apply(lambda x: x if x in ['Restaurant','Coffee'] else 'NV')

# Hago la PT Final
val = [i for i in targets_heads.columns if i not in ['City','Area','Business','Feudo','Reino','Restaurants/NV']]
targets_heads = targets_heads.pivot_table(index=['Reino','Feudo','Restaurants/NV'],values=val,fill_value=0,aggfunc='sum').reset_index()

#### TRABAJO LOS CW

# Agrego extras a KAM
kams = kams + ['LUCRECIA BORIA']

# Filtro columnas a usar de Partners
cols = ['Grid','Name','Franchise','City','Area','Feudo','Reino','KAM','Concept','Logistic','Business','Main_Cuisine']
partners_heads = partners[cols].copy()

# Doy formato a las columnas
cw['Grid'] = cw['Grid'].astype(str)
partners_heads['Grid'] = partners_heads['Grid'].astype(str)
# Uno las tablas
cw = cw.merge(partners_heads,on=['Grid'],how='left')
# Coloco los Owner KAM
cw['Owner'] = cw['Owner'].str.upper()
cw['Owner KAM'] = cw['Owner'].apply(lambda x: 'Si' if x in kams else 'No')

#### TRABAJO LOS ZOMBIES Y CHURNS

# Coloco Feudo y Reino
zc['Feudo'] = zc.apply(feudos,axis=1)
zc['Reino'] = zc.apply(reinos,axis=1)
# Divido según Vertical
zc['Restaurants/NV'] = zc['Business'].apply(lambda x: 'Restaurant' if x == 'Restaurant' else 'NV')

### CARGA

log = carga('1QYt3ZpOI67KVuls9-Zpf9auwtHjXB4o5vV59mG4kgKY','Targets BQ',targets_heads,'Targets Heads',log)

time.sleep(60)

log = carga('1s-_-_t65ybi7_iUfPSNiJY-aSZVAGdPloESuUJZ0Q0U','CW BQ',cw,'Contratos',log)

time.sleep(60)

log = carga('1k9_iZy_xG7EEsKIF9SNzEnyA7bl0BQxp1n1O2aViTrw','Zombies BQ',zc[zc['Zombie_Churn'] == 'Zombie'],'Gestion Zombies',log)

time.sleep(60)

log = carga('1k9_iZy_xG7EEsKIF9SNzEnyA7bl0BQxp1n1O2aViTrw','Churns BQ',zc[zc['Zombie_Churn'] == 'Churn'],'Gestion Churns',log)

print('Corrio Targets Heads -',datetime.date.today())

####################################################################################################################################################
#
# GROWTH
#
####################################################################################################################################################

print('###############')
print('Arranca Growth')

### QUERIES

# 200MB
q_fr = '''WITH open_time AS(
    SELECT ph.restaurant_id AS id,
           SUM(CASE WHEN ph.is_online THEN IFNULL(ph.closed_times,0) ELSE NULL END) AS closed_time,
           SUM(CASE WHEN ph.is_online THEN IFNULL(ph.schedule_open_time,0) ELSE NULL END) AS scheduled_time
    FROM `peya-bi-tools-pro.il_core.dim_historical_partners` AS ph
    WHERE ph.yyyymmdd BETWEEN DATE('{0}') AND DATE('{1}')
          AND ph.is_online
    GROUP BY 1)
SELECT p.partner_id AS Id,
       IFNULL(ot.closed_time,0) AS Closed_Time,
       IFNULL(ot.scheduled_time,0) AS Scheduled_Time
FROM `peya-bi-tools-pro.il_core.dim_partner` AS p
LEFT JOIN open_time AS ot ON p.partner_id = ot.id
WHERE p.country_id = 3
      AND p.salesforce_id IS NOT NULL'''.format(itm,ftm)

# 57MB
q_cw = '''SELECT p.salesforce_id AS Grid,
       sfo.Business_Type AS Tipo,
       sfc.Name__c AS Nombre,
       cw.account_owner AS Owner,
       DATE(cw.closed_date) AS Fecha_Cierre,
       MAX(sfc.Commission) AS Comision,
FROM `peya-bi-tools-pro.il_core.fact_sales_executives_closed_won_v2` AS cw
LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS p ON cw.grid = p.salesforce_id
LEFT JOIN `peya-data-origins-pro.cl_salesforce.opportunity` AS sfo ON cw.oportunity_unique_id = sfo.Id
LEFT JOIN `peya-data-origins-pro.cl_salesforce.contract` AS sfc ON sfo.Id = sfc.Id_Opportunity
WHERE DATE(cw.closed_date) BETWEEN DATE('{0}') AND DATE('{1}')
      AND p.country_id = 3
      AND sfo.Business_Type IN ('New Business','Win Back','Franchise Extension','New Bussiness')
GROUP BY 1,2,3,4,5'''.format(itm,ftm)

# 714MB
q_orders = '''SELECT c.city_name AS City,
       COUNT(DISTINCT o.order_id) AS Total,
       COUNT(DISTINCT CASE WHEN o.order_status = 'CONFIRMED' THEN o.order_id ELSE NULL END) AS Confirmed,
       COUNT(DISTINCT CASE WHEN o.order_status = 'CONFIRMED' AND o.with_logistics THEN o.order_id ELSE NULL END) AS Logistic,
       COUNT(DISTINCT CASE WHEN o.order_status IN ('REJECTED','PENDING','CANCELLED') AND o.fail_rate_owner = 'Restaurant' THEN o.order_id ELSE NULL END) AS Rejected_Vendor
FROM `peya-bi-tools-pro.il_core.fact_orders` AS o
LEFT JOIN `peya-bi-tools-pro.il_core.dim_city` AS c ON o.city.city_id = c.city_id
WHERE o.registered_date BETWEEN DATE('{0}') AND DATE('{1}')
      AND o.country_id = 3
      AND o.address.area.city.name IN ({2})
GROUP BY 1'''.format(itm,ftm,city_growth)

# 6MB
q_online = '''SELECT p.city.name AS City,
       COUNT(DISTINCT CASE WHEN pm.is_online THEN pm.restaurant_id ELSE NULL END) AS Online,
       COUNT(DISTINCT CASE WHEN pm.is_online AND pm.has_confirmed_orders = FALSE THEN pm.restaurant_id ELSE NULL END) AS Zombies,
       COUNT(DISTINCT CASE WHEN pm.is_churned THEN pm.restaurant_id ELSE NULL END) AS Churns
FROM `peya-bi-tools-pro.il_core.fact_partners_monthly` AS pm
LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS p ON pm.restaurant_id = p.partner_id
WHERE DATE(pm.full_date) BETWEEN DATE('{0}') AND DATE('{1}')
      AND p.country_id = 3
      AND p.city.name IN ({2})
GROUP BY 1'''.format(itm,ftm,city_growth)

# 970MB
q_campaigns = '''SELECT c.city_name AS City,
       COUNT(DISTINCT o.order_id) AS Confirmed_Mktg
FROM `peya-bi-tools-pro.il_core.fact_orders` AS o,
UNNEST (details) AS od
LEFT JOIN `peya-bi-tools-pro.il_core.dim_subsidized_product` AS sp ON od.product.product_id = sp.product_id AND od.is_subsidized
LEFT JOIN `peya-bi-tools-pro.il_growth.dim_subsidized_campaign` AS sc ON sp.subsidized_product_campaing_id = sc.subsidized_campaign_id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_city` AS c ON o.city.city_id = c.city_id
WHERE o.registered_date BETWEEN DATE('{0}') AND DATE('{1}')
      AND od.is_subsidized
      AND o.order_status = 'CONFIRMED'
      AND o.country_id = 3
      AND sc.subsidized_campaign_id IS NOT NULL
      AND o.address.area.city.name IN ({2})
GROUP BY 1'''.format(itm,ftm,city_growth)

# Descargo la data
hue_crudo = pd.io.gbq.read_gbq(q_fr, project_id='peya-argentina', dialect='standard')
hue_cw = pd.io.gbq.read_gbq(q_cw, project_id='peya-argentina', dialect='standard')
hue_orders = pd.io.gbq.read_gbq(q_orders, project_id='peya-argentina', dialect='standard')
hue_online = pd.io.gbq.read_gbq(q_online, project_id='peya-argentina', dialect='standard')
hue_campaigns = pd.io.gbq.read_gbq(q_campaigns, project_id='peya-argentina', dialect='standard')

# Copio las bases
fr_growth = hue_crudo.copy()
cw = hue_cw.copy()
orders = hue_orders.copy()
online = hue_online.copy()
campaigns = hue_campaigns.copy()

### TRABAJO

#### TRABAJO AM

# Doy formato a las columnas para el Merge
partners['Id'] = partners['Id'].astype(int)
stats['Id'] = stats['Id'].astype(int)
fr_growth['Id'] = fr_growth['Id'].astype(int)
# Elimino Online y Owner de Stats
stats_growth = stats.drop(columns=['Online','Account_Owner']).copy()
# Hago el Merge
growth = partners.merge(stats_growth,on=['Id'],how='left')
growth = growth.merge(fr_growth,on=['Id'],how='left')

# Clasifico en Restaurant y NV
growth['Restaurant/NV'] = growth['Business'].apply(lambda x: 'Restaurant' if x == 'Restaurant' else 'NV')
# Doy formato a las columnas necesarios
cols = ['Scheduled_Time','Closed_Time','VFR_Num TM','Total TM']
growth[cols] = growth[cols].astype(float)
# Creo las columnas faltantes
growth['%Open_Time'] = (growth['Scheduled_Time'] - growth['Closed_Time']) / growth['Scheduled_Time']
growth['VFR'] = growth['VFR_Num TM'] / growth['Total TM']
growth.replace([np.nan,np.inf,-np.inf],0,inplace=True)

# Me quedo con las columnas que necesito
cols = ['Id','Grid','Name','Fecha_Cierre','Account_Owner','Business','Restaurant/NV','Main_Cuisine','City','Area','Feudo','Reino','Online',
        'Online_TM','Zombie','Total TM','VFR_Num TM','VFR','Confirmed_Campaign TM','Closed_Time','Scheduled_Time','%Open_Time',
        'Qty_Products','Qty_Photos']
growth = growth[cols].copy()

# Filtro las columnas de los AM de Growth
growth = growth[growth['Account_Owner'].isin(am_growth)].copy()

#### TRABAJO SALES

# Mando el Propietario a Mayuscula
cw['Owner'] = cw['Owner'].apply(lambda x: x.upper())
# Me quedo con las CW de Growth
growth_cw = cw[cw['Owner'].isin(sl_growth)].copy()
# Cambio las fechas a Str
growth_cw['Fecha_Cierre'] = growth_cw['Fecha_Cierre'].astype(str)

# Selecciono las columnas necesarias
cols = ['Id','Grid','Name','City','Area','Business']
# Doy los formatos necesarios
partners['Grid'] = partners['Grid'].astype(str)
growth_cw['Grid'] = growth_cw['Grid'].astype(str)
# Hago el merge
growth_cw = growth_cw.merge(partners[cols],on=['Grid'],how='left')
# Ordeno las columnas
cols = ['Grid','Id','Name','City','Area','Business','Tipo','Nombre','Owner','Fecha_Cierre','Comision']
growth_cw = growth_cw[cols]

#### TRABAJO CM GROWTH

# Hago un Merge de las tablas
cm_growth = orders.merge(campaigns,on=['City'],how='outer')
cm_growth = cm_growth.merge(online,on=['City'],how='outer')
cm_growth.replace([np.nan,np.inf,-np.inf],0,inplace=True)

#Creo las columnas faltantes
cm_growth['%Campaigns'] = cm_growth['Confirmed_Mktg'] / cm_growth['Confirmed']
cm_growth['%OD'] = cm_growth['Logistic'] / cm_growth['Confirmed']
cm_growth['VFR'] = cm_growth['Rejected_Vendor'] / cm_growth['Total']
cm_growth['%Churns'] = cm_growth['Churns'] / cm_growth['Online']
cm_growth['%Cartera Activa'] = (cm_growth['Online'] - cm_growth['Zombies']) / cm_growth['Online']
cm_growth.replace([np.nan,np.inf,-np.inf],0,inplace=True)

### CARGA

log = carga('1z2MN12Xwvytz4vk3jiFmvSAOti42e2oFHS5HiM0VOJY','Crudo',growth,'Crudo Growth AM',log)

time.sleep(30)

log = carga('1z2MN12Xwvytz4vk3jiFmvSAOti42e2oFHS5HiM0VOJY','Crudo Sales',growth_cw,'Crudo Growth Sales',log)

time.sleep(30)

log = carga('1z2MN12Xwvytz4vk3jiFmvSAOti42e2oFHS5HiM0VOJY','Crudo CM',cm_growth,'Crudo Growth CM',log)

print('Corrio Growth -',datetime.date.today())

####################################################################################################################################################
#
# ZOMBIES Y CHURNS SALES
#
####################################################################################################################################################

print('###############')
print('Arranca ZC Sales')

### QUERIES

# 40MB
q_cw = '''SELECT p.partner_id AS Id,
       cw.account_owner AS Owner
FROM `peya-bi-tools-pro.il_core.fact_sales_executives_closed_won_v2` AS cw
LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS p ON cw.grid = p.salesforce_id
LEFT JOIN `peya-data-origins-pro.cl_salesforce.opportunity` AS sfo ON cw.oportunity_unique_id = sfo.Id
WHERE DATE(cw.closed_date) BETWEEN DATE('{0}') AND DATE('{1}')
      AND p.country_id = 3
      AND sfo.Business_Type IN ('New Business','Win Back','Franchise Extension','New Bussiness')'''.format(ilm,flm)

# Descargo la data
hue_cw = pd.io.gbq.read_gbq(q_cw, project_id='peya-argentina', dialect='standard')

# Copio las bases
cw = hue_cw.copy()

### TRABAJO

# Doy formato a las columnas
cw['Id'] = cw['Id'].astype(int)
stats['Id'] = stats['Id'].astype(int)
partners['Id'] = partners['Id'].astype(int)
# Hago el Merge
zc_sales = cw.merge(stats[['Id','Sessions TM','Sessions LM','Confirmed TM','Confirmed LM','Dias_Online TM','Dias_Online LM']],on=['Id'],how='left')
zc_sales = zc_sales.merge(partners[['Id','Grid','Name','Online','Churn','SF_Status','BO_Status']],on=['Id'],how='left')
zc_sales.replace([np.nan,np.inf,-np.inf],0,inplace=True)

# Doy formato a las columnas
cols = ['Sessions TM','Sessions LM','Confirmed TM','Confirmed LM','Dias_Online TM','Dias_Online LM']
zc_sales[cols] = zc_sales[cols].astype(float)
# Creo las columnas faltantes
zc_sales['Sessions'] = zc_sales['Sessions TM'] + zc_sales['Sessions LM']
zc_sales['Orders'] = zc_sales['Confirmed TM'] + zc_sales['Confirmed LM']
zc_sales['Dias_Online'] = zc_sales['Dias_Online TM'] + zc_sales['Dias_Online LM']

# Me quedo con las columnas necesarias
cols = ['Sessions TM','Sessions LM','Confirmed TM','Confirmed LM','Dias_Online TM','Dias_Online LM']
zc_sales.drop(columns=cols,inplace=True)
# Ordeno las columnas
cols = ['Id','Grid','Name','Owner','Online','Churn','SF_Status','BO_Status','Sessions','Orders','Dias_Online']
zc_sales = zc_sales[cols].copy()

# Manejo la logica
zc_sales['Zombie'] = zc_sales.apply(lambda x: 'Si' if x['Orders'] == 0 and x['Sessions'] >= 20 and x['Dias_Online'] > 0 else 'No',axis=1)
zc_sales['Churn RT'] = zc_sales['SF_Status'].apply(lambda x: 'Si' if x == 'Terminated' else 'No')

### CARGA

log = carga('11RbtyaGviE1Tm9VibNPK_AlB4oJUVZM_Sw15ly2dzN4','CW LM',zc_sales,'ZC Sales',log)

print('Corrio ZC Sales -',datetime.date.today())

####################################################################################################################################################
#
# MIGRACIONES INBOUND
#
####################################################################################################################################################

print('###############')
print('Arranca Migraciones Inbound')

### QUERIES

# 705MB
q_cam = '''SELECT DISTINCT p.partner_id AS Id,
       sc.subsidized_campaign_name AS Campaign
FROM `peya-bi-tools-pro.il_core.dim_subsidized_product` AS sp
INNER JOIN `peya-bi-tools-pro.il_growth.dim_subsidized_campaign` AS sc ON sp.subsidized_product_campaing_id = sc.subsidized_campaign_id
INNER JOIN `peya-bi-tools-pro.il_core.dim_partner_product` AS pp ON sp.product_id = pp.product_id
INNER JOIN `peya-bi-tools-pro.il_core.dim_partner` AS p ON pp.partner_id = p.partner_id
WHERE p.country_id = 3
      AND sp.is_deleted = FALSE 
      AND pp.unavailable = FALSE 
      AND pp.enabled
      AND pp.is_deleted = FALSE
      AND pp.section_is_deleted = FALSE
      AND sc.subsidized_campaign_id IN ({0})
      AND (DATE(sp.end_date) >= CURRENT_DATE() OR DATE(sp.end_date) IS NULL)
      AND p.salesforce_id IS NOT NULL
ORDER BY 1,2'''.format(inb_camp)

# 10MB
q_origen = '''SELECT p.partner_id AS Id,
       IFNULL(sfa.account_source,'-') AS Origen
FROM`peya-bi-tools-pro.il_core.dim_partner` AS p
LEFT JOIN `peya-bi-tools-pro.il_salesforce.dim_salesforce_account` AS sfa ON p.salesforce_id = sfa.account_grid
WHERE p.country_id = 3
      AND p.salesforce_id IS NOT NULL'''

# Descargo la data
hue_cam = pd.io.gbq.read_gbq(q_cam, project_id='peya-argentina', dialect='standard')
hue_origen = pd.io.gbq.read_gbq(q_origen, project_id='peya-argentina', dialect='standard')

# Copio las bases
cam_inb = hue_cam.copy()
origen_inb = hue_origen.copy()

### TRABAJO

# Hago una pivot table con las Campañas
mig_inb = pd.DataFrame(cam_inb.pivot_table(index=['Id'],columns=['Campaign'],aggfunc='size',fill_value=0).to_records())
# Cambio los 1 y 0 por Si y No
for i in mig_inb.columns[1:]:
    mig_inb[i] = mig_inb[i].apply(lambda x: 'Si' if x > 0 else 'No')

# Doy formato a las columnas
partners['Id'] = partners['Id'].astype(int)
mig_inb['Id'] = mig_inb['Id'].astype(int)
origen_inb['Id'] = origen_inb['Id'].astype(int)
# Hago un Merge con Partners
mig_inb = partners.merge(mig_inb,on=['Id'],how='left')
mig_inb = mig_inb.merge(origen_inb,on=['Id'],how='left')
mig_inb.replace([np.nan,np.inf,-np.inf],'No',inplace=True)

# Filtro los Inbound
mig_inb = mig_inb[mig_inb['Account_Owner'].isin(inbounds)].copy()

### CARGA

log = carga('1FANWqEVrOiYb_GdNQt3vTb1y9MeBTb6NDXu1e3j148s','Crudo',mig_inb,'Migracion Inbound',log)

print('Corrio Migracion Inbound -',datetime.date.today())

####################################################################################################################################################
#
# CITIES 2.0
#
####################################################################################################################################################

print('###############')
print('Arranca Cities 2.0')

### QUERIES

# 3.6GB
q_orders = '''SELECT SUBSTR(CAST(o.registered_date AS STRING),1,7) AS Month,
       c.city_name AS City,
       a.area_name AS Area,
       o.business_type.business_type_name AS Business,
       COUNT(DISTINCT(o.order_id)) AS Total,
       COUNT(DISTINCT(CASE WHEN o.order_status = 'CONFIRMED' THEN o.order_id ELSE NULL END)) AS Confirmed,
       COUNT(DISTINCT(CASE WHEN (o.order_status = 'CONFIRMED'  AND o.with_logistics = TRUE) THEN o.order_id ELSE NULL END)) AS Logistic,
       COUNT(DISTINCT(CASE WHEN (o.order_status = 'CONFIRMED' AND o.has_voucher_discount = 1) THEN o.order_id ELSE NULL END)) AS Vouchers_Orders,
       COUNT(DISTINCT(CASE WHEN (o.order_status = 'CONFIRMED' AND o.is_online_payment = 1) THEN o.order_id ELSE NULL END)) AS OP_Orders,
       COUNT(DISTINCT(CASE WHEN (o.order_status = 'REJECTED' AND o.fail_rate_owner = 'Restaurant') THEN o.order_id ELSE NULL END)) AS Rejected_Partner,
       COUNT(DISTINCT(CASE WHEN (o.order_status = 'REJECTED' AND o.fail_rate_owner = 'PedidosYa') THEN o.order_id ELSE NULL END)) AS Rejected_PEYA,
       COUNT(DISTINCT(CASE WHEN o.order_status = 'REJECTED' THEN o.order_id ELSE NULL END)) AS Rejected,
       COUNT(DISTINCT(CASE WHEN (o.order_status = 'PENDING' AND o.fail_rate_owner = 'Restaurant') THEN o.order_id ELSE NULL END)) AS Pending_Partner,
       COUNT(DISTINCT(CASE WHEN (o.order_status = 'PENDING' AND o.fail_rate_owner = 'PedidosYa') THEN o.order_id ELSE NULL END)) AS Pending_PEYA,
       COUNT(DISTINCT(CASE WHEN o.order_status = 'PENDING' THEN o.order_id ELSE NULL END)) AS Pending,
       COUNT(DISTINCT(CASE WHEN (o.order_status = 'CANCELLED' AND o.fail_rate_owner = 'Restaurant') THEN o.order_id ELSE NULL END)) AS Cancelled_Partner,
       COUNT(DISTINCT(CASE WHEN (o.order_status = 'CANCELLED' AND o.fail_rate_owner = 'PedidosYa') THEN o.order_id ELSE NULL END)) AS Cancelled_PEYA,
       COUNT(DISTINCT(CASE WHEN o.order_status = 'CANCELLED' THEN o.order_id ELSE NULL END)) AS Cancelled,
       ROUND(SUM(CASE WHEN o.order_status = 'CONFIRMED' AND o.with_logistics THEN (o.total_amount + o.shipping_amount) ELSE NULL END),3) AS GMV_Log,
       ROUND(SUM(CASE WHEN o.order_status = 'CONFIRMED' AND o.with_logistics = FALSE THEN (o.total_amount + o.shipping_amount) ELSE NULL END),3) AS GMV_Mktp,
       ROUND(SUM(CASE WHEN o.order_status = 'CONFIRMED' AND o.with_logistics THEN o.commission_amount ELSE NULL END),3) AS Revenue_Log,
       ROUND(SUM(CASE WHEN o.order_status = 'CONFIRMED' AND o.with_logistics = FALSE THEN o.commission_amount ELSE NULL END),3) AS Revenue_Mktp,
       ROUND(SUM(CASE WHEN o.order_status = 'CONFIRMED' AND o.with_logistics THEN o.shipping_amount_no_discount ELSE NULL END),3) AS DF_Log,
       ROUND(SUM(CASE WHEN o.order_status = 'CONFIRMED' AND o.with_logistics = FALSE THEN o.shipping_amount_no_discount ELSE NULL END),3) AS DF_Mktp,
       COUNT(DISTINCT(CASE WHEN o.order_status = 'CONFIRMED' AND o.with_logistics THEN o.order_id ELSE NULL END)) AS Log_Orders_Economics,
       COUNT(DISTINCT(CASE WHEN o.order_status = 'CONFIRMED' AND o.with_logistics = FALSE THEN o.order_id ELSE NULL END)) AS Mktp_Orders_Economics,
       ROUND(SUM(CASE WHEN o.order_status = 'CONFIRMED' AND o.is_online_payment = 1 THEN (o.total_amount + o.shipping_amount) ELSE NULL END),3) AS GMV_OP,
       ROUND(SUM(CASE WHEN o.order_status = 'CONFIRMED' AND o.is_online_payment = 1 THEN o.commission_amount ELSE NULL END),3) AS Revenue_OP,
       SUM(CASE WHEN o.order_status = 'CONFIRMED' AND o.with_logistics THEN o.total_amount + o.discount_paid_by_company + o.shipping_amount - o.shipping_amount_no_discount ELSE NULL END) AS Log_Income,
       SUM(CASE WHEN o.order_status = 'CONFIRMED' AND o.with_logistics = FALSE THEN o.total_amount + o.shipping_amount + o.discount_paid_by_company ELSE NULL END) AS Mktp_Income,
       COUNT(DISTINCT o.user.id) AS Total_Users,
       COUNT(DISTINCT CASE WHEN o.order_status = 'CONFIRMED' THEN o.user.id ELSE NULL END) AS Active_Users
FROM `peya-bi-tools-pro.il_core.fact_orders` as o
LEFT JOIN `peya-bi-tools-pro.il_core.dim_area` AS a ON o.address.area.id = a.area_id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_city` AS c ON o.city.city_id = c.city_id
WHERE o.country_id = 3 
      AND o.registered_date BETWEEN DATE('{0}') AND DATE('{1}')
GROUP BY 1,2,3,4'''.format(lm,tm)

# 55MB
q_online = '''SELECT SUBSTR(CAST(DATE(pm.full_date) AS STRING),1,7) AS Month,
       p.city.name AS City,
       a.area_name AS Area,
       p.business_type.business_type_name AS Business,
       COUNT(DISTINCT(CASE WHEN pm.is_online THEN p.partner_id ELSE NULL END)) AS Online,
       COUNT(DISTINCT(CASE WHEN pm.is_online AND pm.is_logistic THEN p.partner_id ELSE NULL END)) AS OD,
       COUNT(DISTINCT(CASE WHEN pm.is_online AND pm.has_online_payment THEN p.partner_id ELSE NULL END)) AS OP,
       COUNT(DISTINCT(CASE WHEN pm.is_online AND pm.accepts_vouchers THEN p.partner_id ELSE NULL END)) AS Partners_Vouchers,
       SUM(CASE WHEN pm.has_confirmed_orders THEN pm.total_gold_vip + pm.total_featured_product + pm.total_gold_vip_category + pm.total_vip ELSE 0 END) AS NCR,
       SUM(CASE WHEN pm.has_confirmed_orders THEN pm.joker_fee ELSE 0 END) AS Joker
FROM `peya-bi-tools-pro.il_core.fact_partners_monthly` AS pm
LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS p ON pm.restaurant_id = p.partner_id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_area` AS a ON p.address.area_id = a.area_id
WHERE DATE(pm.full_date) BETWEEN DATE('{0}') AND DATE('{1}')
      AND p.country_id = 3
GROUP BY 3,4,2,1'''.format(lm,tm)

# 430MB
q_sesiones = '''SELECT SUBSTR(CAST(sbr.date AS STRING),1,7) AS Month,
       p.city.name AS City,
       a.area_name AS Area,
       p.business_type.business_type_name AS Business,
       SUM(sbr.sessions) AS Sessions
FROM `peya-bi-tools-pro.il_sessions.fact_sessions_by_restaurant` AS sbr
LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` as p ON sbr.restaurant_id = p.partner_id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_area` AS a ON p.address.area_id = a.area_id
WHERE p.country_id = 3 
      AND sbr.date BETWEEN DATE('{0}') AND DATE('{1}')
GROUP BY 2,3,4,1'''.format(lm,tm)

# 2.2GB
q_op = '''SELECT SUBSTR(CAST(o.registered_date AS STRING),1,7) AS Month,
       c.city_name AS City,
       a.area_name AS Area,
       o.business_type.business_type_name AS Business,
       IFNULL(COUNT(DISTINCT CASE WHEN lo.is_vendor_late_10 = 1 THEN o.order_id ELSE NULL END),0) AS VL10_Numerador,
       IFNULL(COUNT(DISTINCT CASE WHEN lo.is_vendor_late_nn = 1 THEN o.order_id ELSE NULL END),0) AS VL_Denominador
FROM `peya-bi-tools-pro.il_logistics.fact_logistic_orders` AS lo
LEFT JOIN `peya-bi-tools-pro.il_core.fact_orders` AS o ON lo.peya_order_id = o.order_id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_area` AS a ON o.address.area.id = a.area_id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_city` AS c ON o.city.city_id = c.city_id
WHERE lo.created_date_local BETWEEN DATE('{0}') AND DATE('{1}')
      AND o.registered_date BETWEEN DATE('{0}') AND DATE('{1}')
      AND o.country_id = 3
GROUP BY 1,2,3,4'''.format(lm,tm)

# 2.1GB
q_acq = '''SELECT SUBSTR(CAST(o.registered_date AS STRING),1,7) AS Month,
       c.city_name AS City,
       a.area_name AS Area,
       o.business_type.business_type_name AS Business,
       COUNT(DISTINCT CASE WHEN obc.nro_order_confirmed = 1 THEN obc.user_id ELSE NULL END) AS Acquisitions
FROM `peya-bi-tools-pro.il_core.fact_peya_orders_by_customers` AS obc
LEFT JOIN `peya-bi-tools-pro.il_core.fact_orders` AS o ON obc.order_id = o.order_id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_area` AS a ON o.address.area.id = a.area_id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_city` AS c ON o.city.city_id = c.city_id
WHERE o.registered_date BETWEEN DATE('{0}') AND DATE('{1}')
      AND DATE(obc.registered_date) BETWEEN DATE('{0}') AND DATE('{1}')
GROUP BY 3,4,2,1'''.format(lm,tm)

# Descargo la data
hue_online = pd.io.gbq.read_gbq(q_online, project_id='peya-argentina', dialect='standard')
hue_orders = pd.io.gbq.read_gbq(q_orders, project_id='peya-argentina', dialect='standard')
hue_acq = pd.io.gbq.read_gbq(q_acq, project_id='peya-argentina', dialect='standard')
hue_sesiones = pd.io.gbq.read_gbq(q_sesiones, project_id='peya-argentina', dialect='standard')
hue_op = pd.io.gbq.read_gbq(q_op, project_id='peya-argentina', dialect='standard')

#Copia las bases
orders = hue_orders.copy()
online = hue_online.copy()
sesiones = hue_sesiones.copy()
op = hue_op.copy()
acq = hue_acq.copy()

### TRABAJO

# Doy el mismo formato a todos los Months
orders['Month'] = pd.to_datetime(orders['Month'], format='%Y-%m').dt.strftime('%Y-%m')
online['Month'] = pd.to_datetime(online['Month'], format='%Y-%m').dt.strftime('%Y-%m')
sesiones['Month'] = pd.to_datetime(sesiones['Month'], format='%Y-%m').dt.strftime('%Y-%m')
op['Month'] = pd.to_datetime(op['Month'], format='%Y-%m').dt.strftime('%Y-%m')
acq['Month'] = pd.to_datetime(acq['Month'], format='%Y-%m').dt.strftime('%Y-%m')

# Hago el merge
cols = ['Month','City','Area','Business']
cities = orders.merge(online,on=cols,how='outer')
cities = cities.merge(sesiones,on=cols,how='outer')
cities = cities.merge(op,on=cols,how='outer')
cities = cities.merge(acq,on=cols,how='outer')
cities = cities.replace([np.inf, -np.inf, np.nan], 0).copy()

#Coloco los Feudos y Reinos
cities['Feudo'] = cities.apply(feudos,axis=1)
cities['Reino'] = cities.apply(reinos,axis=1)
cities['Vertical'] = cities['Business'].apply(lambda x: x if x == 'Restaurant' else 'NV')

# Doy formato necesario a las columnas
cols = [i for i in cities.columns if i not in ['Month','City','Area','Business','Feudo','Reino','Vertical']]
cities[cols] = cities[cols].astype(float)

# Formo las columnas faltantes
cities['GMV'] = cities['GMV_Log'] + cities['GMV_Mktp']
cities['Revenue'] = cities['Revenue_Log'] + cities['Revenue_Mktp']
cities['Total_Rejected_Partner'] = cities['Rejected_Partner'] + cities['Pending_Partner'] + cities['Cancelled_Partner']
cities['Total_Rejected_PEYA'] = cities['Rejected_PEYA'] + cities['Pending_PEYA'] + cities['Cancelled_PEYA']
cities['Total_Rejected'] = cities['Rejected'] + cities['Pending'] + cities['Cancelled']
cities = cities.replace([np.inf, -np.inf, np.nan], 0).copy()

# Ordeno las columnas
cols = ['Month','City','Area','Business','Confirmed','Logistic','Vouchers_Orders','OP_Orders','Online','OD','OP','Partners_Vouchers',
        'Sessions','Rejected_Partner','Rejected_PEYA','Rejected','Pending_Partner','Pending_PEYA','Pending','Cancelled_Partner',
        'Cancelled_PEYA','Cancelled','Total','GMV_Log','GMV_Mktp','Revenue_Log','Revenue_Mktp','DF_Log','DF_Mktp','Log_Orders_Economics',
        'Mktp_Orders_Economics','GMV_OP','Revenue_OP','Log_Income','Mktp_Income','Total_Users','Active_Users','Acquisitions',
        'VL10_Numerador','VL_Denominador','NCR','Joker','Feudo','Reino','Vertical','GMV','Revenue','Total_Rejected_Partner',
        'Total_Rejected_PEYA','Total_Rejected']
cities = cities[cols].copy()

### CARGA

log = carga('1aiwWbTP2l_2aVNop5vGCdea9SYvlRCX50vzKQ9yicQI','Crudo',cities,'Cities 2.0',log)

print('Corrio Cities 2.0 -',datetime.date.today())

####################################################################################################################################################
#
# CARGA FINAL DE PARTNERS Y STATS
#
####################################################################################################################################################

print('###############')
print('Arranca Carga de Partners')

time.sleep(60)

### CARGA PARTNERS

log = carga('1HmAvHYbJJa3JyRTgRJGKXataQB_TvFwNzyo8a6qXd2o','Partners PEYA',partners,'Partners',log)

time.sleep(120)

print('Corrio Carga de Partners -',datetime.date.today())

### CARGA STATS

print('###############')
print('Arranca Carga de Stats')

log = carga('1PO5kGLgz5Xn_WNCQQB0OPcKCl7KeS7kMrkJaGxafqmY','Stats PEYA',stats,'Stats',log)

time.sleep(30)

print('Corrio Carga de Stats -',datetime.date.today())

print('###############')
print('Corrio Correctamente Argentina -',datetime.date.today())
print('###############')

### LOG

log = carga('1eNHyzpSiSOiITJIgZQQy2-cIkG0aMXrXFU272dkryn0','Log',log,'Log',log)

print('Corrio el Logeo -',datetime.date.today())