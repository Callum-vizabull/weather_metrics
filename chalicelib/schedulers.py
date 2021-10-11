from chalice import AuthResponse, Chalice

from chalicelib.util import (
    get_queues,
    get_signal_queue
    )
from chalicelib.dbcalls import (
    companyid_webid,
    webid_domain,
    get_db_conn,
    send_sqs_message,
    web_id_to_ticker,
    get_db_conn_string,
    )
import pandas as pd
import vbdb.db as db





def update_scheduler_root(event):
    sqs_queue_url_domain, sqs_queue_url_ticker, sqs_queue_url_revenue = get_queues()
    def domain_and_ticker_calls(ticker):
        try:
            web_ids = companyid_webid[[ c.id for c in get_db_conn().session.query(db.Company).all() if c.ticker ==ticker][0]]
            domains = [webid_domain[web_id] for web_id in web_ids]
            send_sqs_message(sqs_queue_url_ticker, [{"ticker":ticker}])
            send_sqs_message(sqs_queue_url_revenue, [{"ticker":ticker}])
            for domain in domains:
                send_sqs_message(sqs_queue_url_domain, [{"domain":domain}])
        except Exception as e:
            print(e)
            print(ticker)
    query = "Select * from alexa_api_weekly ;"
    web_ids =  list(pd.read_sql(query, con=get_db_conn_string()).website.unique())
    ticker_list = [web_id_to_ticker(webid) for webid in web_ids]
    ticker_list = [t for t in ticker_list if t is not None]
    ticker_list = list(set(ticker_list))

    tickers = ticker_list
    for ticker in tickers:
        domain_and_ticker_calls(ticker)

    return {"tickers":ticker_list}    


def update_all_signals_root(event):
    sqs_queue_signals = get_signal_queue()
    print("queues")
    def ticker_calls(ticker):
        try:
            send_sqs_message(sqs_queue_signals, [{"ticker":ticker}])
        except:
            print("Failed: "+ticker)

    query = "Select * from company_level_metrics ;"

    tickers = list(pd.read_sql(query, con=get_db_conn_string()).ticker.unique())
    print(tickers)
    for ticker in tickers:
        ticker_calls(ticker)

def update_alexa_completeness_root(event):
    import datetime
    
    last_13 = [datetime.date.today() - i* datetime.timedelta(days=30) for i in range(1,14)]
    last_13 = [datetime.date(x.year,x.month,1) for x in last_13]
    query = f"Select * from alexa_api_weekly where date > '"+(last_13[-1]-datetime.timedelta(days=5)).strftime("%Y-%m-%d")+"';"
    df = pd.read_sql(query, con=get_db_conn_string()).sort_values("date")
    df["month"] = df.date.apply(lambda x: datetime.date(x.year,x.month,1))
    df_counts = df[df.month.isin(last_13)].groupby(["month","website"],as_index=False).count()
    df_valid_months = df_counts[df_counts.date>25].groupby("website",as_index=False).count()
    df_valid_months["valid"]= df_valid_months.date == 13
    df_valid_months[["website","valid"]].to_sql("alexa_completeness_table",con=get_db_conn_string(),if_exists='replace',index=False)