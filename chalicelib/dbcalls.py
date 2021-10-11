import vbdb.db as db
import pandas as pd
import sqlalchemy as sql_db

from chalicelib.util import (get_db_conn_string, combine_sites_for_company,
get_sqs_url, send_sqs_message,
round_date,combine_sites,generate_time_df,get_ticker_id,get_db_conn,to_date,dollar_to_float)


from chalicelib.util import (get_db_conn_string, combine_sites_for_company,
get_sqs_url, send_sqs_message,
round_date,combine_sites,generate_time_df,get_ticker_id,get_db_conn,to_date,dollar_to_float)

def get_df(website_ids):
    website_str = str(website_ids).replace(",","") if len(website_ids) == 1 else str(website_ids)
    query = f"Select * from alexa_api_weekly where website in "+ website_str + " ;"
    alexa_df = pd.read_sql(query, con=get_db_conn_string())
    return alexa_df

# _BIG_REVENUE = None
_ALEXA_SITES = None 
_ENGINE = None
_VALID_SITES = None

def get_valid_sites():
    global _VALID_SITES
    if _VALID_SITES is None:
        _VALID_SITES = {x[0]:x[1] for x in pd.read_sql("select * from alexa_completeness_table",con=get_db_conn_string()).itertuples(index=False)}
    return _VALID_SITES
def remove_invalid_sites(web_id_list):
    def is_valid(web_id):
        if web_id not in get_valid_sites():
            return False
        return get_valid_sites()[web_id]
    return [w for w in web_id_list if is_valid(w)]
def get_engine():
    global _ENGINE
    if _ENGINE is None:
        _ENGINE =  sql_db.create_engine(get_db_conn_string())
    return _ENGINE

def get_alexa_sites():
    global _ALEXA_SITES
    if _ALEXA_SITES is None:
        _ALEXA_SITES= [site for site in list(get_db_conn().session.query(db.Website).order_by(db.Website.id).filter(db.Website.alexa_api_run ==True).all())]
    return _ALEXA_SITES
domain_webid= {site.domain : site.id for site in get_alexa_sites() }
domain_company_id =  {site.domain : site.company_id for site in get_alexa_sites() }

webid_domain= {site.id : site.domain for site in get_alexa_sites() }

companyid_webid = {}
for site in get_alexa_sites():
    companyid_webid.setdefault(site.company_id,[]).append(site.id)
_ID_TICKER = None
def get_id_ticker():
    global _ID_TICKER
    if _ID_TICKER is None:
        _ID_TICKER  = {c.id: c.ticker for c in get_db_conn().session.query(db.Company).all()}
    return _ID_TICKER
id_ticker = get_id_ticker()

_WEB_ID_TO_CID = None
_CID_TO_TICKER = None

def get_web_id_to_cid():
    global _WEB_ID_TO_CID
    if _WEB_ID_TO_CID is None:
        _WEB_ID_TO_CID  = {s.id : s.company_id for s in get_db_conn().session.query(db.Website).all()}
    return _WEB_ID_TO_CID
def get_cid_to_ticker():
    global _CID_TO_TICKER
    if _CID_TO_TICKER is None:
        _CID_TO_TICKER = {c.id: c.ticker for c in get_db_conn().session.query(db.Company).all()}
    return _CID_TO_TICKER
def web_id_to_ticker(webid):
    try:
        return get_cid_to_ticker()[get_web_id_to_cid()[webid]]
    except:
        return None