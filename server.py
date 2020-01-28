from http.server import BaseHTTPRequestHandler, HTTPServer
import http.client
import json
import time
import base64

import statsmodels.api as sm
import requests
SERVER_URL = 'edge.caps.in.tum.de:8080'
#SERVER_URL = 'localhost:8080'

# TODO: LoRaServer - your own port, JWT token, device EUI and device fPort
PORT = 9512
JWT = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJsb3JhLWFwcC1zZXJ2ZXIiLCJhdWQiOiJsb3JhLWFwcC1zZXJ2ZXIiLCJuYmYiOjE1MDg3NDgxNTAsImV4cCI6MTYwMDAwMDAwMCwic3ViIjoidXNlciIsInVzZXJuYW1lIjoid3MxOTIwdGVhbTAzc3R1ZGVudDAxIn0.Hdu3Wc8HFpzEfsR1SpzDn2oH6vigmOVM6YtApNWlhoI'
device_EUI = 'd9d866aa6eee9591'
device_fPort = 81

# TODO: IoT Platform - address of the gateway, JWT token for the device created in IoT Platform, sensor id of the device
GATEWAY_URL = '10.195.0.10:8083'
DEV_JWT = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1Nzk4MTE1NDcsImlzcyI6ImlvdHBsYXRmb3JtIiwic3ViIjoiMSJ9.RZl9vz4YveiF1rrKB7SVPHKgu9eiCAToyD_B21zb92PEOcVryUTLjY2iBlEt-eUNmrLpe_WH0ZznChVVp-XHzPksh2UjtDVlhiqH7K3xA7B6x5DFBSMOk0AootPsF14qZgp9nWeZcCmMM9fr1XPMEyiT2vBcqZxv2omyMkoz6f-Nn9UZc__7Osiv2bx02XlEWpz6e35vpNWAeSZ99HF8Kguy7u7CX5dVWlTxja8Ob1coLsWqmo-5fcrw61kR2HeBPMq3MUHjNyX2MSPkXkRmQdpjptSmpgiCvcdYWigGnpGcBIMV2d_-9w30LstAkJZApdRhrphEpR0dC-sJ8ZP6ZUWBYdVM5Ze2nb3BGskMQC34FfkHnGzRM9zGOuHDo8kMA-Fp2ZmGpFzCj68NO38M9Y5lLbkD0SeSPoApjxdjv4BrNcwUgGGimGG82NRNOdaCmiAf61BHaTsCuiOWnbaI7OIDXgH-rda-DnWQwvo84HyUcUTZCOHadR2E8GfCPHVNAudt9rBcz6m1GmDaZIyKhCdj8t-eEg1wMnl2Juqk7lfGJBqRXDg1OEJjXFM6HGosumKmda5i6qEKRbtIS2byLGLRbKw5H6Q3cT6CF5Fv67v65XEn08JORDUd_clBE8Sndov2svGi5DTb74iQcgBfRZzA3TldIy6_BuMrHP3SStA'
SENSOR_ID = '1'

# API endpoint to enqueue the downlink messages (messages to the end device)
api_dev_enqueue_url = '/api/devices/' + device_EUI + '/queue'

# Endpoint for listening to the model manager shit
model_manager_url = '/api/devices/model_manage'

# Relevant IP addresses
worker_ip = '10.195.2.229'
master_ip = '10.195.0.10'


# Download the model
import socket
import os 

def get_stuff():
    
	pass

model = sm.load('recieved.pkl')
print("model loaded")
# Defining an inherited Handler for HTTP requests that is able
# to handle POST requests.
class HandleRequests(BaseHTTPRequestHandler):
	

	def _set_headers(self):
		self.send_response(200)
		self.send_header('Content-Type', 'application/json')
		self.end_headers()

	def do_POST(self):
		
		print("1: Processing post request")
		self._set_headers()
		global model
		# print("header: {}".format(self.headers))
		# Processing HTTP POST request data
		# print("header type: {}".format((self.headers['Content-Length'])))


		length = int(self.headers['Content-Length'])
		#print("Content Length :{}".format(length))
		post_body = self.rfile.read(length)
		val_json = json.loads(post_body.decode('utf-8'))
		# val = val_json["object"]["battery"]
		battery = val_json["object"]["battery"]
		val = val_json["object"]["people"]
		print("battery_val: {}\npeople: {}".format(battery,val))
		#is_model_updated = val_json["model_updated"]
		print("Received post request")

		if "model_updated" in val_json:
			is_model_updated = val_json["model_updated"]
		else:
			is_model_updated = False 

		if is_model_updated: 
			self.finish()
			self.connection.close()
			time.sleep(1)
			self.iot_platform_connect(ip_addr=worker_ip, port=12345)
			is_model_updated = False

			# Reload the model
			model = sm.load('recieved.pkl')
			return
		print("1: Finished processing post request")
		
		# TODO: Apply learned prediction model on the 
		# measurement received within the POST request.
			
		prediction = int(model.forecast())
		print("2: Apply prediction model")
		print("prediction: {}".format(prediction))
		
		
		# TODO: Forward the observation received in the
		# POST request to the IoT Platform.
		
		print("3: Forwarding observation to IoT Platform")
		iot_url = GATEWAY_URL
		
		gw_conn = http.client.HTTPConnection(iot_url)
		response = gw_conn.connect()
		print("response: {}".format(response))
		print("Connection with gateway established")
		
		msg = {}
		msg['sensor_id'] = SENSOR_ID
		msg['timestamp'] = int(time.time() * 1000)
		msg['value'] = val
		gw_json_msg = json.dumps(msg)

		gw_headers = {  "Content-Type": "application/json",
				"Authorization": "Bearer " + DEV_JWT }
		gw_conn.request('POST', '/', gw_json_msg, gw_headers)
	

		"""print("Attempting post...")
		full_url = GATEWAY_URL
		while True: 
			response = requests.post(full_url, data = gw_json_msg, headers = gw_headers)
			if response.status_code == 200:
				break
			full_url = '10.195.0.10:8083'
		print("Connection with gateway successfully established")
		"""
		gw_resp = gw_conn.getresponse()
		print("Status: {}".format(gw_resp.status))
		#if gw_response == 200:
		#	pass
		
		gw_conn.close()
		print("3: Data sent to IoT Platform")
		
		
		# TODO: Adjust the downstream sending through LoRaServer API
		# to send the results of applying the prediction model to
		# the measurement received within the POST request.
		# Reference: http://localhost:8080/api#!/DeviceQueueService/Enqueue
		print("4: Sending prediction via LoRaServer API to board")
		api_conn = http.client.HTTPConnection(SERVER_URL)
		api_conn.connect()
		print("Connected to LoRaServer API")
		
		#predictionB64 = base64.b64encode(str(prediction2).encode('utf-8'))
		predictionB64 = base64.b64encode(bytes([prediction]))
		print("predictionB64 is: " + predictionB64.decode('utf-8'))
 
		# When the prediction is accomplished, the results is returned
		# to the device via LoRa App Server's API call that enqueues the data.
		# Below is the minimal possible API call returning 200 OK HTTP code.
		data = {}
		dev_queue_item = {}
		dev_queue_item['data'] = predictionB64.decode('utf-8')
		#dev_queue_item['data'] = "10"		
		dev_queue_item['fPort'] = device_fPort
		data['deviceQueueItem'] = dev_queue_item
		json_data = json.dumps(data)

		headers = { "Content-Type": "application/json",
			    "Accept": "application/json",
			    "Grpc-Metadata-Authorization": "Bearer " + JWT }
		api_conn.request('POST', api_dev_enqueue_url, json_data, headers)

		# Sending the prediction to the device was requested from API via POST request above.
		# TODO: you need to ensure that the response returned is correct.
		# Reference: https://docs.python.org/2.2/lib/httpresponse-objects.html
		print("Prediction send to board. Waiting for response")
		resp = api_conn.getresponse()
		
		if resp.status == 200:
			print("Status 200:")
			print("Data sent to enque")
		else:
			print("Data not sent properly:")
			print("Status {}".format(resp.status))
			print("Response: {}".format(resp.reason))

		api_conn.close()
		print("4: Closing connection to LoRaServer API")
		print("-----Finished processing DO_POST()-----")

	def iot_platform_connect(self, ip_addr, port):
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect((ip_addr,12345)) 

		f = open("recieved.pkl", "wb")
		data = None
		print("Entering the loop")
		while True:
			m = s.recv(1024)
			data = m
			if m:
				while m:
					m = s.recv(1024)
					data += m
					
				else:
					break
		f.write(data)
		f.close()
		s.close()
		print("Done receiving")

# Starting HTTP server that serves incoming requests
# HTTPServer(('localhost', PORT), HandleRequests).serve_forever()
print("Booting up the server. Waiting for connections...")
HTTPServer(('', PORT), HandleRequests).serve_forever()
