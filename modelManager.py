from requests.auth import HTTPBasicAuth
import requests

# ML Modelling
from scipy.stats import norm
import statsmodels.api as sm

# Dataset stuff
import pandas as pd 
from datetime import datetime, timedelta
import itertools

# Scheduling 
import schedule
import time


# This is the python model manager


# We will make our own macros
MONTAG = 0
DIENSTAG = 1
MITTWOCH = 2
DONNERSTAG = 3
FREITAG = 4
SAMSTAG = 5
SONNTAG = 6

MITTLE_NACHT = 24


import sys

# Model download
import socket 
import os

# Logging
import logging

# retrieve all records from elastic search
query_body = {
  
    "query": {
        "match_all": {}
    }



}

dummy_json = {
	"object" : {
		"battery": 0
	},

    "model_updated" : True 
	
}

# Global variables fuck this
is_model_updated = False 
new_weekly_data_retrieved = False 
real_ts = pd.DataFrame(columns=['t','count'])


# Consumer JWT Token
jwt_token = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1NzU5MDIwODgsImlzcyI6ImlvdHBsYXRmb3JtIiwic3ViIjoiMSJ9.d5EFKNCawtfUAtp3QOWGFgrhZyIGGJFsrp59erHNiTq37gJ8DmZ-9p_yy5rBir5VJeAJvGb-dn2J9_w5G8hmMu9RKC_v4gr7I8BJYHXgzhlQt2TfwzH8DBKB15hXlkkJBo0tayN38SZGzXkaC1clJMbczlafxWYPp_jdPNT_yGiOSpdAfGxsmFspws5_2qedb5jTmWd0fxNE2_-wO1qfrKT_1E-_oa-ASY1EWntwWX5CFSWNDYwN9cDsYhjXNeW42nzzjQuNuoaPAGDL37IEa54lUTI7ynMh_U6mkPUJaSG0jWwhZz70muGrb0Df8gyguGkQ4Q1KWP-brVhyJ_CN06QH037yrbw7HqpnkjM6y-8cCzIoWk5oxHeI4XIQGEr4jVbBNc9Rp7-9b08Sb7D90WKEa-5jjnLOVouHn4wCBM8xnA03a-pWYmGQijD3VhvpTBTmmYQmMMOJNWeXsCUpqaBnU2bmlngKRnaj4c507W3qdvQhvsUcOxNvjp2kzNjhCO3fdeWKQoIVz47SM5XmNHbiAIzooAAwDHpITkc_yQufBXpwDgz0d42ODanbkajczuCxK89sZ8Pk8KyioJ8i5ddd4MvdJ2xJoJtU7CoC1kszNjih6QhKJGz_V1iJxtCo_pqwxH8jca7XtLZ2lbSR_0Vf_oY_RESXKusMzslHDy8'

# local_http_server = 'http://localhost:9000'
edge_server = 'http://edge.caps.in.tum.de:9512'
local_http_server = 'http://129.187.212.21:9000'
worker_ip = '10.195.2.229'
master_ip = '10.195.0.10'

designated_port = 9000
ssl_port = 12345

def connect_to_http(ip_addr):
    response = requests.post(ip_addr, json=dummy_json)
    response.close()
    print("Post request sent")
    return 1







class model_manager():

    def __init__(self):
        self.model = None 
        self.is_model_updated = False 
        self.res = None

    def build_model(self, generated_ts, name = "SARIMAX"):
        self.model = sm.tsa.statespace.SARIMAX(generated_ts['count'].astype(float), trend='c', order=(1,1,(1,0,0,1)))

    def re_fit(self, new_ts):

        # Rebuild the model with the new dataset
        self.build_model(generated_ts = new_ts)
        assert(self.model is not None)
        
        # Save model and make it read for transfer
        self.res = self.model.fit()
        self.is_model_updated = True
    
    def save_model(self,name = "current_model", with_timestamp = False):

        if with_timestamp == True: 
            # Current time
            now = datetime.now()
            current_timestamp = int(datetime.timestamp(now) * 1000)

            self.res.save(name + "_" + str(current_timestamp) + ".pkl")
        
        else: 
            self.res.save("current_model.pkl")

    def load_model(self, model_name = "current_model.pkl"):
        loaded_model = self.model.load(model_name)


    
    def secure_model_transfer(self, port = 12345):
        """[Secure connection to provide http server on edge device with model download. Model manager acts as server]
        Keyword Arguments:
            port {int} -- Port that server will connect to (default: {12345})
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("",port))

        s.listen(5)
        c, addr = s.accept()
        print("Connection accepted")

       
        # SP SP Stack overflow
        f = open("./current_model.pkl","rb")
        size = os.path.getsize("./current_model.pkl")
        size_bytes = f.read(size)

        c.sendall(size_bytes)
        f.close()
        print("Sent model")

        self.is_model_updated = False
        time.sleep(10) 

        s.close()
    
    def run_infinite(self, server_ip):
        """[Run model manager infinitely]
        
        Arguments:
            server_ip {string} -- [IP address of server to communicate with]
        """

        while True: 
            print("Model Manager is running")
            # If model is updated, send post request
            if is_model_updated:
                self.post_to_http(ip_addr=server_ip)
                # print("Initiating model transfer")
                # self.secure_model_transfer()
        
    
    def post_to_http(self, ip_addr, json_message):
        """ [Method to post request to specific ip]
        
        Arguments:
            ip_addr {string} -- [IP address to send the post request to]
        
        Returns:
            [int] -- [Successful exit code]
        """
        response = requests.post(ip_addr, json=json_message)
        response.close()
        print("Post request sent")
        return 1

def parse_data(json_data):
    """Json Data to parse (helper method)
    
    Arguments:
        json_data {Json Object} -- [Json object for us to parse]
    """
    # print("json_data: {}".format(json_data))
    # This should have an array of the data that we requested 
    # Size will vary depending on the specifications
    total_hits = json_data['hits']['total']
    dataset = json_data['hits']['hits']

    return dataset, total_hits




def build_pandas_db(dataset):
    """ Create a pandas dataframe
    
    Arguments:
        dataset {list} -- List of JSON objects to build data frame from
    
    Returns:
        [Pandas dataframe] -- [The dataset that we will use for the training]
    """
    global real_ts
    # Garbage value
    prev_timestamp = -9999

    for index in range(len(dataset)):
        current_timestamp = dataset[index]['_source']['timestamp']
        current_value = dataset[index]['_source']['value']
        
        # Eliminate duplicate data if there is any
        if prev_timestamp == current_timestamp: 
            continue
        
        # Convert to seconds
        cur_timestamp_seconds = current_timestamp / 1000000
        
        # Row for pandas
        date_from_timestamp = datetime.fromtimestamp(cur_timestamp_seconds)
        num_ppl_room = current_value
        
        df_row = pd.DataFrame([[date_from_timestamp,num_ppl_room]], columns=['t', 'count'])
        real_ts = real_ts.append(df_row)

        prev_timestamp = current_timestamp
    
    real_ts.index = real_ts.t
    sorted_ts = real_ts.sort_index()
    final_ts = sorted_ts.drop_duplicates()
    
    assert(len(final_ts) <= len(sorted_ts))
    
    return final_ts

def elastic_search_request(start_datetime, end_datetime, size = 10, sort = 'asc'):
    	
	master_ip = 'http://10.195.0.10:3000'
	worker_ip = 'http://10.195.2.229:3000'
	api = '/api/consumers/consume/1/_search'
	range_api = '?q=timestamp[' + str(start_datetime) + '+TO+' + str(end_datetime) + ']'
	size_api = '&size=' + str(size)
	sort_api = '&sort=timestamp:' + sort
	full_url = worker_ip + api + range_api + sort_api + size_api
    
    	# if we do not get response from worker than try master
	    
        # Fancy way o toggling
	while True: 
	    
		response = requests.get(full_url, data=query_body, headers = {"authorization": "Bearer " + jwt_token})
        
		if response.status_code == 200: 
		    break
		
		full_url = master_ip + api + range_api + sort_api + size_api
    
    
    
	data = response.json()

	results ,total_hits = parse_data(json_data = data)
    
    	# Parse data function to extract what we need

	return results, total_hits

def populate_data(timestamp_list, value_list, dataset):
    """ Get the list of timestamps and values
    
    Arguments:
        timestamp_list {[type]} -- [description]
        value_list {[type]} -- [description]
        dataset {[type]} -- [description]
    
    Returns:
        [type] -- [description]
    """

    for index in range(len(dataset)):

        current_timestamp = dataset[index]['_source']['timestamp']
        current_value = dataset[index]['_source']['value']
        
        # Eliminate duplicate data if there is any
        if prev_timestamp == current_timestamp: 
            continue
        

        # Two separate lists to hold the timestamps
        timestamp_list.append(current_timestamp)
        value_list.append(current_value)
        
        prev_timestamp = current_timestamp
        

        return timestamp_list, value_list


def elastic_search_data_retrieval(_start_date, _end_date):
    """ Get weekly elastic search data based on the provided dates
    
    Arguments:
        _start_date {String} -- Beginning date (Calendar format)
        _end_date {String} -- End date (Calendar format)
    """

    extended_ts_format = '%Y-%m-%d %H:%M:%S.%f'

    # Exact timestamps in string format
    start_time = _start_date + " 00:00:00.000"
    end_time = _end_date + " 23:59:00.000"

    start_timestamp = int(datetime.strptime(start_time,extended_ts_format).timestamp() * 1000 * 1000)
    end_timestamp = int(datetime.strptime(end_time, extended_ts_format).timestamp() * 1000 * 1000)

    response_list = []
    

    
    # We can only query for 10k times
    request_boundary = 10000
    hit_count = 0
    
    # The -1 refers to the last timestamp
    ts_idx = -1

    while True: 

        cur_data, hits = elastic_search_request(start_datetime = start_timestamp,end_datetime = end_timestamp,size = request_boundary)

        hit_count = hit_count + len(cur_data)

        response_list.append(cur_data)

        if hits < request_boundary: 
            break 

        # Final timestamp in list to continue
        start_timestamp = cur_data[ts_idx]['_source']['timestamp']
    
    merged_responses = list(itertools.chain.from_iterable(response_list))
    assert(len(merged_responses) == hit_count)

    build_pandas_db(dataset=merged_responses)    

    # logging.info("Total Number of hits: {}".format(hit_count))
    
    # All data is retrieved. 

def weekly_scrape_wrap():
    """ Wrapper function for th elastic search weekly scrape
    """
    logging.info("Beginning weekly data scrape")
    print("Weekly data scrape")


    global new_weekly_data_retrieved 
    calendar_format = '%Y-%m-%d'

    current_time_as_date = datetime.now()
    start_date = (current_time_as_date - timedelta(days = 6)).strftime(calendar_format)
    end_date = current_time_as_date.strftime(calendar_format)

    elastic_search_data_retrieval(_start_date=start_date, _end_date=end_date)

    new_weekly_data_retrieved = True
    print("scrape done. Sleeping for a minute..")
    logging.info("Sleeping for a minute...")
    time.sleep(60)
    print("Leaving now")
    print("new_weekly_data: {}".format(new_weekly_data_retrieved)) 





if __name__=='__main__':
    
    # Let's set up the logger for some bookkeeping

    

    # connect_to_http(ip_addr=edge_server)
    
    model_manager = model_manager()

    logging.basicConfig(filename='records.log', level=logging.INFO)
    logging.info("Model Manager created")

    # Time is an hour behind
    weekly_update_time = "22:59"
    
    # thursday_time = "14:52" 

    # connect_to_http(ip_addr=edge_server)
    # model_manager.run_infinite(server_ip=edge_server)
    # connected = connect_to_http(ip_addr=edge_server)

    # if connected: 
    #    model_manager.secure_connection(ip_addr=edge_server, port=12345)

    # Clear the schedulre first
    schedule.clear()
    schedule.every().sunday.at(weekly_update_time).do(weekly_scrape_wrap)

    # schedule.every().saturday.at(thursday_time).do(weekly_scrape_wrap)
    # After training we must clear the stuff

    # Basic Json message
    msg = {}
    battery = {}
    battery['battery'] = 0
    battery['people'] = 0
    msg['object'] = battery
    msg['model_updated'] = model_manager.is_model_updated

    print("Entering infinite loop...") 
    while True:

        schedule.run_pending()

        if new_weekly_data_retrieved: 
            print("Fitting the model now")
            model_manager.re_fit(new_ts=real_ts)
            new_weekly_data_retrieved = False
            print("Model is fitted")
                
        if model_manager.is_model_updated: 
            print("Model has been updated and will send now")
            
            # Inform edge of update
            msg['model_updated'] = model_manager.is_model_updated
            model_manager.post_to_http(ip_addr=edge_server,json_message=msg)
            model_manager.secure_model_transfer()
            print("Model has been sent")
            # Update back to regular
            msg['model_updated'] = False
            print("Oscillating model...")
  
        # Do nothing

            

        
        # Continue the infinite loop

