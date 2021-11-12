#!/usr/bin/python3

import os
import sys
import json
import requests
import _thread
import threading
import time
import errno
from socket import error as SocketError

def http_post_para( url, para, back ):

	#header={"Content-Type":"application/x-www-form-urlencoded"}
	#datas = {para}
	#datas = {'parameter1':'12345','parameter2':'23456'}
	#r = requests.post(url=url, headers=headers, data=datas)
	#if r.status_code != requests.codes.ok:
	#	return False
	
	#print(r.content)
	#if r.content != back:
	#	return False
	
	try:
		data = {'key1':'value1','key2':'value2'}
		r = requests.post(url,data)
		if r.status_code != requests.codes.ok:
			return False
			
		if r.content != back:
			return False
	except SocketError as e:
		pass
		
	return True


def http_post_para_thread( url, para, back, time_second, sem, id):
	s_time = time.time()
	total = 0
	succeed = 0 
	
	while True:
		ret = http_post_para(url, para, back)
		total = total + 1
		if ret:
			succeed = succeed + 1
		c_time = time.time()
		if c_time-s_time > time_second :
			break
		
	result = "http_post_para_thread " + str(id) + ":  total=" + str(total) + ", succeed=" + str(succeed) + ", ratio=" + str(succeed/total*100) + "%"
	print(result)
	sem.release()
	
	
if __name__ == "__main__":
	
	if len(sys.argv) != 2:
		raise RuntimeError("Usage: http_post_para.py http_post_para.json")
		
	print("Http get, start work")
	config_file = sys.argv[1]
	json_data = json.load(open(config_file))
	url = json_data["url"]
	para = json_data["para"]
	back = json_data["back"]
	thread_num = json_data["thread"]
	time_second = json_data["time"]

	sem = threading.Semaphore(thread_num)
	
	try:
		for i in range(thread_num):
			sem.acquire()
			_thread.start_new_thread( http_post_para_thread, (url, para, back, time_second, sem, i) )
	except:
		raise RuntimeError("Error: unable to start thread")
		
	for i in range(thread_num):
		sem.acquire()
		
	
