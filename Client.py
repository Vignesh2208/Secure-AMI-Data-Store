import socket
import sys
import os
import time
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
from Crypto.Hash import SHA256
import subprocess
import select
import csv
import time
import matplotlib.pyplot as plt
from os.path import expanduser
import math

key = open("Public_key.pem", "r").read()
rsapubkey = RSA.importKey(key)
key = open("Private_key.pem", "r").read()
rsaprivatekey = RSA.importKey(key)

server_IP_address_dict = {}
server_IP_address_dict[1] = 'localhost'
server_IP_address_dict[2] = '130.126.245.43'


# Return size of folder pointed to by start_path
def get_size(start_path = '.'):
	total_size = 0
	for dirpath, dirnames, filenames in os.walk(start_path):
		for f in filenames:
			fp = os.path.join(dirpath, f)
			total_size += os.path.getsize(fp)
	return total_size
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

def get_file_content(file_path,line_number = -1) :
	if line_number == -1 :
		with open(file_path,'r') as f :
			content = f.read()
		f.close()
		return content
	else :
		f=open(file_path)
		lines=f.readlines()
		content = lines[line_number]
		f.close()
		return content


def send_message(sock,message,message_type) :
	# Send data
	len_of_message = 0
	len_of_message = str(len(message))
	
	while len(len_of_message) < 32 :
		len_of_message = '0' + len_of_message

	h = SHA256.new()
	h.update(message)
	message_digest = str(h.hexdigest())

	output = rsaprivatekey.sign(message_digest,0)
	message_digest_signature = str(output[0])
	message_digest_signature_len = str(len(message_digest_signature))

	while len(message_digest_signature_len) < 32 :
		message_digest_signature_len = '0' + message_digest_signature_len

	message_len = str(len(message) + len(message_digest_signature) + 64 + len(message_type))

	while len(message_len) < 32 :
		message_len = '0' + message_len


	transmit_message = len_of_message + message_digest_signature_len + message_type + message + message_digest_signature


	#print >>sys.stderr, 'sending "%s"' % transmit_message
	sock.sendall(message_len)
	sock.sendall(transmit_message)

def perform_operations(message_body,message_length_size,message_type_size,conn) :

		payload_length = long(message_body[0:message_length_size])
		signed_digest_length = long(message_body[message_length_size:2*message_length_size])
		message_type = message_body[2*message_length_size:2*message_length_size + message_type_size]
		payload = message_body[2*message_length_size + message_type_size :2*message_length_size+ message_type_size + payload_length]
		signed_digest = long(message_body[2*message_length_size + payload_length + message_type_size:])

		h = SHA256.new()
		h.update(payload)
		computed_message_digest = h.hexdigest()
		input_tuple = (signed_digest,None)
			
		
		
		if rsapubkey.verify(computed_message_digest,input_tuple) == True :
			print "Client : Signature Verification of the received message suceeded"

			if message_type == 'OK' : # ADD command
				
				data = payload
				return data
			else :
				return None




def send_ADD(sock,key,data_to_add):

	#sock.send("ADD")
	#send_message(sock,"ADD")
	#time.sleep(1)
	#sock.send(key + "@" + data_to_add)
	send_message(sock,key + "@" + data_to_add,"01")
	print "ADD command sent. Waiting for response ... "
	response = sock.recv(4096)
	if response == "OK" :
		print "ADD successfull"
		return 1
	else :
		print "ADD Failed"
		return -1

def send_RETRIEVE(sock,key) :
	#sock.send("RETRIEVE")
	#send_message(sock,"RETRIEVE")
	#time.sleep(1)
	#sock.send(key)
	send_message(sock,key,"02")
	print "RETRIEVE command sent. Waiting for response ... "

	message_length_received = 0
	message_body_received = 0
	message_body = ""
	message_length = 0
	message_length_size = 32
	message_type_size = 2
	# Receive the data in small chunks and retransmit it
	while True :
		if message_length_received == 0 :			
			data = sock.recv(message_length_size)
			if data == ""  :
				break
			else :
				if data == "-1" :
					print "RETRIEVE Failed. Decode operation failed in the server"
					return -1
				elif data == "-2" :
					print "RETRIEVE Failed. Specified Key doesn't exist in the database"
					return -1
				else :
			
					message_length = int(data)
					print "Client : Expecting a message of length",message_length
					message_length_received = 1
		elif message_body_received == 0 :
			data = ""
			while len(data) < message_length :
				data += sock.recv(4096)
				
			if data:
				#print >>sys.stderr, 'sending data back to the client'
				print "Client : Received a message of the expected length. Verifying the signature"
				#print "received message = ", data	
				if message_length > 0 :
					message_body = data
					data = perform_operations(message_body,message_length_size,message_type_size,sock)													
					message_body_received = 1
				break
					
			else:
				print >>sys.stderr, 'No data received from', addr
				break


        				
		

	
	if data != None :
		print "RETRIEVE Success"
		return data
	else :
		return None

def send_Exit(sock):
	sock.send("close")


def main() :
	arg_list = sys.argv
	if len(arg_list) < 2:
		server_IP_address = 'localhost'
		interactive_mode = 1
	else :
		server_IP_address = arg_list[1]
		interactive_mode = 0

	if interactive_mode == 1 :

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

		while True :
			try :
		
				user_command = raw_input('Enter any of the following commands : ADD <Key> <String> or ADD <Key> <Filepath> or RETREIVE <Key>\n')
				print "You entered : ",user_command
				command_list = user_command.split(' ')
				if command_list[0] != "ADD" and command_list[0] != "RETRIEVE" :
					print "Invalid command ... "
				
				else :
					key = command_list[1]
					command = command_list[0]
					if command == "ADD" :
						if os.path.exists(command_list[2]) :
							filepath = command_list[2]
							with open(filepath,'rb') as fp :
								value = fp.read()
							fp.close()
						else :
							value = command_list[2]
						result = send_ADD(s,key,value)
						if result == -1 :
							print "Error on server side. ADD failed. Exiting"
							send_Exit(s)
							s.close() 
							exit()
						#s.close()
						#time.sleep(1)
					else :
						response = send_RETRIEVE(s,key)
						if response != -1 :
							print "Received data = ", response
						#s.close()
						#time.sleep(1)
				
			except KeyboardInterrupt:
				print "Quitting Client now ! Good bye"
				send_Exit(s)
				s.close() 
				exit()
			except SystemExit:
				s.close()
				exit()
	else : #non interactive mode. Run as key injector.
		dataset = "/home/raven/Documents/ECE-542 Project/data archives/data_sept112013.csv"

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

		dataset = "/home/raven/Documents/ECE-542 Project/data archives/data_sept112013.csv"
		injected_keys_file = "/home/raven/Documents/ECE-542 Project/injected_keys.txt"
		
		with open(injected_keys_file,"wb") as f :
			pass
		f.close()

		with open(dataset, "rb") as csvfile:
			datareader = csv.reader(csvfile)
			count = 0
			request_latencies = []
			request_number = []
			actual_storage_size = 0
			for row in datareader:
				if count == 5 :
					break
				else :
					if count != 0 :
						print "Sending ADD request no ",count 
						timestamp_key = row[0]
						
						value = myString = ",".join(row)
						actual_storage_size += len(value)
						t_start = time.time()
						result = send_ADD(s,timestamp_key,value)
						if result == -1 :
							print "Error on server side. ADD failed. Exiting"
							send_Exit(s)
							s.close() 
							exit()
						else :
							t_stop = time.time()
							tdiff = t_stop - t_start
							request_latencies.append(tdiff)
							request_number.append(count)
							with open(injected_keys_file,"a") as f :
								f.write(timestamp_key + "\n")
							f.close()
						
					
				count += 1


		home = expanduser("~")	
		Disk_base_dir = home + '/Disk'
		total_storage_size = 0
		if os.path.exists(Disk_base_dir) :
			total_storage_size = get_size(Disk_base_dir)
			if total_storage_size != 0 :
				efficiency = float(actual_storage_size*100)/float(total_storage_size)
			else :
				efficiency = -1
		print "plotting histogram of Add request latencies now.."
		plt.hist(request_latencies)
		plt.show()
		(mean,std) = cal_mean_std(request_latencies)
		with open("Add_statistics.txt","wb") as f :
			f.write("Mean  = " + str(mean) + "\n") 
			f.write("Std  = " + str(std) + "\n")
			f.write("Storage efficiency  = " + str(efficiency) + "\n")
		f.close()
		send_Exit(s)
		s.close()
		exit()




if __name__ == "__main__":
	main()


