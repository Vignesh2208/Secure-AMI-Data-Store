import socket
import sys
import os
import time
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
from Crypto.Hash import SHA256
import subprocess
import select
from Client import *
from random import randrange
from datetime import datetime
import matplotlib.pyplot as plt

arg_list = sys.argv
rate = 1
no_of_requests = 3600
INJECTED_KEYS_PATH ="injected_keys.txt"


#returns the mean and variance of the supplied list. USed to find mean, variance of access latency
def cal_mean_std(l):
	s2 = 0
	s = 0
	n = 0
	for e in l:
		s += e
		s2 += e * e
		n += 1
	mean = float(s)/float(n)
	variance = float(s2)/float(n) - float(mean*mean)
	std = math.sqrt(variance)
	return (mean, std)





with open(INJECTED_KEYS_PATH,'rb') as f:
	injected_keys = f.readlines()
f.close()


if len(arg_list) < 2:
	server_IP_address = 'localhost'
else :
	server_IP_address = arg_list[1]




h = SHA256.new()
h.update(server_IP_address)
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
host = server_IP_address
port = 2750
try :
	print "Connected to server"
	s.connect((host,port))
except s.error , msg:
	print 'Connect failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
	sys.exit(-2)

curr_request = 0
request_latencies = []
request_number = []
while curr_request < no_of_requests :
	
	if len(injected_keys) > 2 :
		
		random_index = randrange(0,len(injected_keys))
		print "Sending request number ", curr_request + 1, " Key : ", injected_keys[random_index][0:-1]
		tstart = time.time()
		response = send_RETRIEVE(s,injected_keys[random_index][0:-1])
		print "response = ",response
		tfinish = time.time()
		tdiff = tfinish - tstart
		request_latencies.append(tdiff)
		request_number.append(curr_request)

	else :
		print "Not enough injected keys "
		break

	curr_request += 1	

send_Exit(s)
print "plotting histogram of Retrieve request latencies now.."
plt.hist(request_latencies)
plt.show()
(mean,std) = cal_mean_std(request_latencies)
with open("Retrieve_statistics.txt","wb") as f :
	f.write("Mean  = " + str(mean) + "\n") 
	f.write("Std  = " + str(std) + "\n")
f.close()
print "Quitting evaluator now. Goodbye !"
s.close()
