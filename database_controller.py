import json
import urllib3
import psycopg2
import logging
import datetime
import sys
import configparser

date_preset = None
date_conversion = None
#si viene un argumento de fecha debemos cambiar el parametro de filtro
if (len(sys.argv)>1):
    date_preset = sys.argv[1]
    date_conversion = {'today':0,'last_3d':3, 'last_7d':7, 'last_14d':14,'last_28d':28, 'last_30d':30, 'last_90d':90}

config = configparser.ConfigParser()
config.read_file(open('/home/josesilvero/Develop/test-marketing/defaults.cfg'))
log_file = config.get('common','log_file')
db_name = config.get('common', 'db_name')
db_user = config.get('common', 'db_user')
db_pass = config.get('common', 'db_pass')
db_host = config.get('common', 'db_host')
db_port = config.get('common', 'db_port')
db_schema = config.get('common','db_schema')
crossroads_key = config.get('api', 'crossroads_key')
acces_token = config.get('api', 'acces_token')
facebook_accounts = config.get('api','facebook_accounts').split("\n")

logging.basicConfig(level=logging.INFO, filename="campaigns_py_log.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")
try:
    conn = psycopg2.connect(database=db_name, user=db_user, password=db_pass, host=db_host, port=db_port)
except:
    logging.ERROR("Error al intentar conectar a la base de datos.")

today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
yesterday_format = yesterday.strftime("%Y-%m-%d")

if (date_preset == None):    
    start_date_format = yesterday_format
    filter = 'yesterday'
else:
    filter = date_preset
    try:
        last_xdays = date_conversion[date_preset]
        start_date = today - datetime.timedelta(days=last_xdays)
        start_date_format = start_date.strftime("%Y-%m-%d")
    except:
        logging.error(date_preset + ' is not a valid parameter for date_preset')

http = urllib3.PoolManager()

url_crossroads = "https://crossroads.domainactive.com/api/v2/get-campaigns-info?key="+crossroads_key+"&start-date="+start_date_format+"&end-date="+yesterday_format

crossroads_campaigns = "https://crossroads.domainactive.com/api/v2/get-campaigns?key="+crossroads_key

def get_facebook_data(account):
    url_facebook = "https://graph.facebook.com/v19.0/act_"+account+"/insights?fields=spend,impressions,clicks,objective,campaign_name,campaign_id,actions&level=campaign&date_preset="+filter+"&access_token="+acces_token+"&filtering=[{'field':'campaign.effective_status','operator':'IN','value':['ACTIVE','PAUSED','CAMPAIGN_PAUSED']}]&limit=200"

    logging.info(f"Intentando descargar datos de facebook...")
    next_link = url_facebook
    while (next_link != None):
        cur = conn.cursor()
        try:
            response = http.request('GET', next_link)
            data = json.loads(response.data.decode('utf-8'))

            # del JSON de respuesta nos interesa solo la data 
            key_list = list(data.keys())
            first_key = key_list[0]
            if ('paging' in key_list):
                paging = data['paging'].keys()
                # mientras siga habiendo una siguiente pagina debemos procesar mas registros
                if ('next' in paging):
                    next_link = data['paging']['next']
                else:
                    next_link = None
            else:
                next_link = None

            for i in data[first_key]:
                var4 = var5 = var6 = var9 = None

                var1 = i['campaign_id']
                var2 = i['campaign_name']
                var3 = i['spend']
                if ('clicks' in i):
                    var4 = i['clicks']
                if ('impressions' in i):
                    var5 = i['impressions']
                if ('actions' in i):
                    actions = i['actions']
                    for act in actions:
                        if (act['action_type'] == 'purchase'):
                            var6 = act['value']
                var7 = 'facebook'
                var8 = i['date_start']
                if ('date_stop' in i):
                    var9 = i['date_stop']
                var10 = account
                cur.execute("""
                    INSERT INTO """+db_schema+""".campaign (campaign_id, campaign_name, spend, clicks, impressions, purchase_value, data_source, date_start, date_stop, account_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (campaign_id) DO UPDATE SET
                    (campaign_name, spend, clicks, impressions, purchase_value, date_start, date_stop, account_id) = (EXCLUDED.campaign_name, EXCLUDED.spend, EXCLUDED.clicks, EXCLUDED.impressions, EXCLUDED.purchase_value, EXCLUDED.date_start, EXCLUDED.date_stop, EXCLUDED.account_id); 
                    """,
                (var1, var2, var3, var4, var5, var6,var7, var8, var9, var10))
                conn.commit()
            cur.close()
        except:
            logging.error("Error al intentar insertar los datos",exc_info=True)
            logging.error(response.status)
            logging.error(response.data.decode('utf-8'))

def get_domains():
    cur = conn.cursor()
    try:
        response = http.request('GET', crossroads_campaigns)
        data = json.loads(response.data.decode('utf-8'))
        key_list = list(data.keys())
        first_key = key_list[0]

        for i in data[first_key]:
            var1 = i['id']
            var2 = i['status']
            var3 = i['revenue_domain_name']

            cur.execute("""
                INSERT INTO """+db_schema+""".domain (campaign_id, status, revenue_domain_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (campaign_id) DO UPDATE SET
                (status, revenue_domain_name) = (EXCLUDED.status, EXCLUDED.revenue_domain_name); 
                """,
            (var1, var2, var3))
            conn.commit()
        cur.close()
    except:
       logging.error("Error al intentar insertar los datos",exc_info=True)
       logging.error(response.status)
       logging.error(response.data.decode('utf-8'))

def get_crossroads_data():
    cur = conn.cursor()
    logging.info(f"Intentado descargar datos de crossroads...")
    try:
        response = http.request('GET', url_crossroads)
        data = json.loads(response.data.decode('utf-8'))
       
        # del JSON de respuesta nos interesa solo la data 
        key_list = list(data.keys())
        first_key = key_list[0]

        for i in data[first_key]:
            var1 = i['campaign_id']
            var2 = i['campaign__name']
            var3 = i['revenue']
            var4 = i['lander_visitors']
            var5 = i['revenue_events']
            var6 = 'crossroads'
            var7 = i['date']
            var8 = i['campaign__created_at']
            var9 = i['rpc']
            
            cur.execute("""
                INSERT INTO """+db_schema+""".campaign (campaign_id, campaign_name, revenue, lander_visitors, revenue_events, data_source, date, campaign__created_at, rpc)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (campaign_id) DO UPDATE SET
                (revenue, lander_visitors, revenue_events, date, rpc) = (EXCLUDED.revenue, EXCLUDED.lander_visitors, EXCLUDED.revenue_events, EXCLUDED.date, EXCLUDED.rpc); 
                """,
            (var1, var2, var3, var4, var5, var6, var7, var8, var9))
            conn.commit()
        cur.close()
    except:
        logging.error("Error al intentar insertar los datos",exc_info=True)
        logging.error(response.status)
        logging.error(response.data.decode('utf-8'))

get_crossroads_data()
get_domains()
logging.info(f"Descarga de crossroads finalizada.")
for account in facebook_accounts:
    get_facebook_data(account)
    logging.info(f"Descarga de facebook finalizada.")