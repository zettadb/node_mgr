#!/usr/bin/python3

import os
import sys
import json
import requests
import _thread
import threading
import time

def http_get( url ):
	r = requests.get(url)
	if r.status_code != requests.codes.ok:
		return False
		
	http_len = r.headers.get('Content-Length')
	file_len = os.path.getsize("http_get.bin")
	if int(http_len) != file_len:
		return False
	
	http_content = r.content 
	start_len = 0
	read_file = open("http_get.bin", "rb")
	if read_file.mode == 'rb':
		while True:
			file_content = read_file.read(1024)
			read_len = len(file_content)
			if read_len>0:
				for i in range(read_len):
					if file_content[i] != http_content[start_len+i]:
						return False
				start_len = start_len + read_len
			elif start_len != file_len:
				return False
			else:
				return True
	return True

def http_get_thread( url, time_second, sem, id):
	s_time = time.time()
	total = 0
	succeed = 0 
	
	while True:
		ret = http_get(url)
		total = total + 1
		if ret:
			succeed = succeed + 1
		c_time = time.time()
		if c_time-s_time > time_second :
			break
		
	result = "http_get_thread " + str(id) + ":  total=" + str(total) + ", succeed=" + str(succeed) + ", ratio=" + str(succeed/total*100) + "%"
	print(result)
	sem.release()
	
	
if __name__ == "__main__":
	
	if len(sys.argv) != 2:
		raise RuntimeError("Usage: http_get.py http_get.json")
		
	print("Http get, start work")
	config_file = sys.argv[1]
	json_data = json.load(open(config_file))
	url = json_data["url"]
	thread_num = json_data["thread"]
	time_second = json_data["time"]
	
	r = requests.get(url)
	if r.status_code != requests.codes.ok:
		raise RuntimeError('http get prepare error')
		
	#print(r.headers.get('Content-Type'))
	#print(r.headers.get('Content-Length'))
		
	with open("http_get.bin", "wb") as save_file:
		save_file.write(r.content)  

	sem = threading.Semaphore(thread_num)
	
	try:
		for i in range(thread_num):
			sem.acquire()
			_thread.start_new_thread( http_get_thread, (url, time_second, sem, i) )
	except:
		raise RuntimeError("Error: unable to start thread")
		
	for i in range(thread_num):
		sem.acquire()
		
	
