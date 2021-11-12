#!/usr/bin/python3

import os
import sys
import json
import requests
import _thread
import threading
import time

			
def http_get_range( url, step ):
	start_len = 0
	file_len = os.path.getsize("http_get_range.bin")
	if file_len == 0:
		return False
		
	read_file = open("http_get_range.bin", "rb")
	if read_file.mode == 'rb':
		while True:
			file_content = read_file.read(step)
			read_len = len(file_content)
			if read_len>0:
				headers = {}
				headers['Range'] = 'bytes={}-{}'.format(start_len, start_len+read_len-1)
				
				r = requests.get(url, headers=headers)
				if r.status_code != 206:
					return False
					
				http_content = r.content
				for i in range(read_len):
					if file_content[i] != http_content[i]:
						return False

				start_len = start_len + read_len
			elif start_len != file_len:
				return False
			else:
				return True
	return True

def http_get_range_thread( url, step, time_second, sem, id):
	s_time = time.time()
	total = 0
	succeed = 0 
	
	while True:
		ret = http_get_range(url, step)
		total = total + 1
		if ret:
			succeed = succeed + 1
		c_time = time.time()
		if c_time-s_time > time_second :
			break
		
	result = "http_get_range_thread " + str(id) + ":  total=" + str(total) + ", succeed=" + str(succeed) + ", ratio=" + str(succeed/total*100) + "%"
	print(result)
	sem.release()
	
	
if __name__ == "__main__":
	
	if len(sys.argv) != 2:
		raise RuntimeError("Usage: http_get_range.py http_get_range.json")
		
	print("Http get range, start work")
	config_file = sys.argv[1]
	json_data = json.load(open(config_file))
	url = json_data["url"]
	step = json_data["step"]
	thread_num = json_data["thread"]
	time_second = json_data["time"]
	
	r = requests.get(url)
	if r.status_code != requests.codes.ok:
		raise RuntimeError('http get range prepare error')
		
	#print(r.headers.get('Content-Type'))
	#print(r.headers.get('Content-Length'))
		
	with open("http_get_range.bin", "wb") as save_file:
		save_file.write(r.content)  

	sem = threading.Semaphore(thread_num)
	
	try:
		for i in range(thread_num):
			sem.acquire()
			_thread.start_new_thread( http_get_range_thread, (url, step, time_second, sem, i) )
	except:
		raise RuntimeError("Error: unable to start thread")
		
	for i in range(thread_num):
		sem.acquire()
		
	
