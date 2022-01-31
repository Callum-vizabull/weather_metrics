import json
import os
conf_path = "/home/ubuntu/environment/weather_metrics/.chalice/config.json"
f = open(conf_path)
conf_json = json.load(f)
conf_json["stages"]["dev"]["environment_variables"]
for k,v in conf_json["stages"]["dev"]["environment_variables"].items():
    os.environ[k]=v
    
import app

from chalice.test import Client
from chalicelib import util
from chalicelib import test


def test_index_route():
    with Client(app.app) as client:
        response = client.http.get('/')
        assert response.status_code == 200
        assert response.json_body == {'hello': 'world'}
def test_run_all_signs():
    with Client(app.app) as client:
        lambda_event = {
            "version": "0",
            "account": "123456789012",
            "region": "us-west-2",
            "detail": {},
            "detail-type": "Scheduled Event",
            "source": "aws.events",
            "time": "1970-01-01T00:00:00Z",
            "id": "event-id",
            "resources": [
              "arn:aws:events:us-west-2:123456789012:rule/my-schedule"
            ]
        }
        response = client.lambda_.invoke('sign_metrics',
                                         lambda_event)
                                         
def test_sign_ticker():
    with Client(app.app) as client:
        sqs_event = {'Records': [{
            'attributes': {
                'ApproximateFirstReceiveTimestamp': '1530576251596',
                'ApproximateReceiveCount': '1',
                'SenderId': 'sender-id',
                'SentTimestamp': '1530576251595'
            },
            'awsRegion': 'us-west-2',
            'body': json.dumps([{"ticker":"AAP"}], cls=util.DecimalEncoder),
            'eventSource': 'aws:sqs',
            'eventSourceARN': 'arn:aws:sqs:us-west-2:12345:queue-name',
            'md5OfBody': '754ac2f7a12df38320e0c5eafd060145',
            'messageAttributes': {},
            'messageId': 'message-id',
            'receiptHandle': 'receipt-handle'
        }]}        
        
        # event = client.event.generate_sns_event(ticker='AAP')
        response = client.lambda_.invoke('apply_sign', sqs_event)
        # assert len(response) > 0, "response empty"
def test_weather_route():
    with Client(app.app) as client:
        response = client.http.get('/weather_qtd/AAP/2021-01-01/2021-02-01')
        
        assert response.status_code == 200
def test_yoy_signed_for_backtesting():
    metrics = ['cold_count', 'heat_count',
           'rain_and_sleet', 'rain_count', 'sleet', 'sleet_count', 'snow',
           'snow_count', 'temp_24h_mean', 'weekend_cold_count',
           'weekend_heat_count', 'weekend_rain_count', 'weekend_sleet_count',
           'weekend_snow_count']
    values_df, input_df = test.get_signed_yoy_weather_for_backtesting("AAP")
    assert len(values_df) ==1
    assert len(input_df) > 0
    for m in metrics:
        assert m in values_df.columns
        assert m in input_df.columns
def test_backtesting_without_write():
    columns = ['final_value', 'final_wr', 'num_pos', 'num_neg', 'score_pos',
       'score_neg', 'formula', 'recent_correct', 'update_date', 'ticker',
       'cold_count', 'heat_count', 'rain_and_sleet', 'rain_count', 'sleet',
       'sleet_count', 'snow', 'snow_count', 'temp_24h_mean',
       'weekend_cold_count', 'weekend_heat_count', 'weekend_rain_count',
       'weekend_sleet_count', 'weekend_snow_count', 'Qtr_wr', 'cold_count_wr',
       'heat_count_wr', 'rain_and_sleet_wr', 'rain_count_wr', 'sleet_wr',
       'sleet_count_wr', 'snow_wr', 'snow_count_wr', 'temp_24h_mean_wr',
       'weekend_cold_count_wr', 'weekend_heat_count_wr',
       'weekend_rain_count_wr', 'weekend_sleet_count_wr',
       'weekend_snow_count_wr', 'date_wr', 'date']
    df =  test.save_backtesting_on_ticker("HOG","2021-01-01",debug=False,test=True)
    assert len(df) == 1
    assert (df.columns == columns).all()
def test_consolidate_weather_by_period():
    df = test.consolidate_weather_by_period("quarter",lookback=180)
    assert len(df) > 0
