import socket
import sys
import os
import MySQLdb
from decoder import decode
from encoder import encode
from Crypto.PublicKey import RSA 
from Crypto.Cipher import PKCS1_OAEP 
from base64 import b64decode 
from Crypto.Cipher import AES
from Crypto import Random
from os.path import expanduser

from Crypto.Hash import SHA256
import subprocess
import time
import shutil
import threading
import datetime as dt
from pymongo import MongoClient



arg_list = sys.argv
if len(arg_list) == 1 :
	my_IP_address = 'localhost'
else :
	my_IP_address = str(arg_list[1])




#CONFIG
k = 10
m = 5
supported_ec_types = ["jerasure_rs_vand","reed_sol_van", "reed_sol_r6_op","jerasure_rs_cauchy","flat_xor_hd_3","flat_xor_hd_4","isa_l_rs_vand"]
ec_type = "flat_xor_hd_4"
TABLE_NAME = "METERDATA"
DATABASE_NAME = "testdb"
user_name="root"
passwd = "ece542"
MONGO_HOST = "104.154.71.244"
MONGO_PORT = 27017
MONGO_DB_NAME = "SMART_GRID"
MONGO_COLLECTION_NAME = "METERING_DATA"
CENTRAL_QUERY_SERVER_HOST = "localhost"
CENTRAL_QUERY_SERVER_PORT = 2800
sock_dict = {}
FREE_PORTS = []
USED_PORTS = []
Max_no_of_concurrent_connections = 2
base_free_port = 2750








i = 0
while i < Max_no_of_concurrent_connections :
	FREE_PORTS.append(base_free_port)
	base_free_port += 1
	i += 1
no_of_concurrent_connections = 0
mutex = 0


with open("Public_key.pem","r") as f :
	key = f.read()
f.close()
rsapubkey = RSA.importKey(key)

key = open("Private_key.pem", "r").read()
rsaprivatekey = RSA.importKey(key)
total_number_of_adds = 0
max_no_of_adds = 100
thread_stop = 0
flush_in_progress = 0



def flush_all(restarted):
	global flush_in_progress

	flush_in_progress = 0
	print "Flushing database ..."
	
	
		
	#Delete_command = "DELETE * FROM " + TABLE_NAME + ";"
	Delete_command = "TRUNCATE TABLE " + TABLE_NAME + ";"
	Select_all_command = "SELECT * FROM " + TABLE_NAME + ";"


	if restarted == 0 :
		# SELECT ALL FROM DATA BASE FOR TRANSMISSION TO CLOUD
		db = MySQLdb.connect(host="127.0.0.1", user= user_name, passwd= passwd, db=DATABASE_NAME) 
				# you must create a Cursor object. It will let
				#  you execute all the queries you need
		cur = db.cursor() 

		cur.execute(Select_all_command)
		fetched_data = cur.fetchall()
	
		db.commit()
		db.close()


	

	if restarted == 0 :
		#TRANSMIT DATA TO CLOUD HERE
		print "Flushing to Mongo DB"
		try :
			client = MongoClient(MONGO_HOST, MONGO_PORT)
			db = client[MONGO_DB_NAME]
			collection = db[MONGO_COLLECTION_NAME]
			posts = collection.posts
			count = 0
			min_timestamp = 10000000000
			max_timestamp = 0
			Timestamp_dict = {}
			
			for row in fetched_data:
				#print "Row = ", row		
				# row should be file name to decode and fetch
				decoded_data = decode(k,m,row[1],ec_type)
				transmit_dict = {}
				key_list = row[1].split('.')
				key = key_list[0]
				node_id_timestamp_list = key.split(':')
				node_id = node_id_timestamp_list[0]
				timestamp = node_id_timestamp_list[0]
				if node_id in Timestamp_dict.keys() :
					if int(timestamp) < Timestamp_dict[node_id]['min'] :
						Timestamp_dict[node_id]['min'] = int(timestamp)
					if Timestamp_dict[node_id]['max'] < int(timestamp) :
						Timestamp_dict[node_id]['max'] = int(timestamp)
					
				else :
					Timestamp_dict[node_id] = {}
					Timestamp_dict[node_id]['min'] = int(timestamp)
					Timestamp_dict[node_id]['max'] = int(timestamp)
				
				transmit_dict["_id"] = key
				transmit_dict["value"] = decoded_data
				post_obj = posts.insert_one(transmit_dict)

			client.close()
		except :
			print "Error during flush to MONGODB. Probably due to already existing record "

		# SEND a notification message to Central Query server with min and max timestamps flushed.
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		host = CENTRAL_QUERY_SERVER_HOST
		port = CENTRAL_QUERY_SERVER_PORT
		print "Flushed to Mongo DB"
		try :
			print "Connected to Central Query server"
			sock.connect((host,port))
			
			msg_to_transmit = str(my_IP_address)
			for node_id in Timestamp_dict.keys() :
				msg_to_strnamit += "@" + node_id + ":" + str(Timestamp_dict[node_id]['min']) + ":" + str(Timestamp_dict[node_id]['max'])
			sock.sendall(msg_to_transmit)
			sock.close()
		except socket.error , msg:
			print 'Connect failed to Central Query server. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]

	#FLUSH DATABASE.
	db = MySQLdb.connect(host="127.0.0.1", user= user_name, passwd= passwd, db=DATABASE_NAME) 
				# you must create a Cursor object. It will let
				#  you execute all the queries you need
	cur = db.cursor() 
	#print "Delete command = ",Delete_command
	cur.execute(Delete_command)
	db.commit()
	db.close()
	home = expanduser("~")	
	Disk_base_dir = home + '/Disk'
	i = 0
	print "Cleaning up all stored data .. "
	while i <= k+m :
		curr_folder = Disk_base_dir + "/Disk_" + str(i)
		if os.path.exists(curr_folder) :
			shutil.rmtree(curr_folder)
		i += 1
	flush_in_progress = 0
	print "Cleaned up all stored data"


def flush_thread():
	print "Flush database thread running .."
	#tstart = dt.datetime.now()
	global thread_stop
	t_elapsed = 0
	t_flush_freq = 30 	#Once a day.
	#t_flush_freq = 100
	while True :
		if thread_stop == 1 :
			break
		time.sleep(1)
		t_elapsed += 1
		if t_elapsed == t_flush_freq :
			flush_all(0) # should be changed to flush_all(0) once the MONGO DB and QUERY server are setup
			t_elapsed = 0
		
	print "Flush database thread stopped"
		
		

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
		#print "Just Echoing the message back to the client after verification of signature"
		payload_length = long(message_body[0:message_length_size])
		signed_digest_length = long(message_body[message_length_size:2*message_length_size])
		message_type = message_body[2*message_length_size:2*message_length_size + message_type_size]
		payload = message_body[2*message_length_size + message_type_size :2*message_length_size+ message_type_size + payload_length]
		signed_digest = long(message_body[2*message_length_size + payload_length + message_type_size:])

		h = SHA256.new()
		h.update(payload)
		computed_message_digest = h.hexdigest()
		input_tuple = (signed_digest,None)
		global total_number_of_adds
		global max_no_of_adds
		global flush_in_progress

		while flush_in_progress == 1 :
			pass
		
		if rsapubkey.verify(computed_message_digest,input_tuple) == True :
			print "Server : Signature Verification of the received message suceeded"

			if message_type == '01' : # ADD command
				print "Switched to ADD mode. Waiting for key@file_data ... "
				data = payload
				split_list = data.split('@')
				key = split_list[0]
				file_data_string = split_list[1]
				base_filename_to_store = key + ".txt"
				insert_command = "INSERT INTO " + TABLE_NAME + " SET " + "Timestamp=" + '"' + key + '"' + "," + " filename=" + '"' + key + ".txt" + '"' + ";"
				db = MySQLdb.connect(host="127.0.0.1", user= user_name, passwd= passwd, db=DATABASE_NAME) 
				# you must create a Cursor object. It will let
				#  you execute all the queries you need
				cur = db.cursor() 
				#print "Insert command = ", insert_command
				cur.execute(insert_command)			
				db.commit()
				db.close()
				encode(k=k,m=m,data_to_encode=file_data_string,base_filename_to_store = base_filename_to_store,ec_type = ec_type)

				#total_number_of_adds = total_number_of_adds + 1
				#if total_number_of_adds >= max_no_of_adds :
				#	total_number_of_adds = 0
				#	flush_all()
				conn.sendall("OK")
				
			elif message_type == '02' : #RETRIEVE Command
				#print "Switched to retrieve mode. Waiting for retrieve key ... "
				key = payload
				#print "Received key = ", key
				retrieve_command = "SELECT * FROM " + TABLE_NAME + " WHERE Timestamp=" + '"' + key + '";'
				#print "retrieve_command = ", retrieve_command
				db = MySQLdb.connect(host="127.0.0.1", user= user_name, passwd= passwd, db=DATABASE_NAME) 
				# you must create a Cursor object. It will let
				#  you execute all the queries you need
				cur = db.cursor() 
				cur.execute(retrieve_command)
				db.commit()
				db.close()
				# print all the first cell of all the rows
				decoded_data_list = []
				#print "Result of command = "
				for row in cur.fetchall() :
					#print "Row = ", row		
					# row should be file name to decode and fetch
					decoded_data = decode(k,m,row[1],ec_type)
		
					decoded_data_list.append(decoded_data)
				#print "Done"
				#print "Decoded data for transmission = "
				for entry in decoded_data_list :
					if entry == -1 :
						conn.sendall("-1")
					else :
						print entry
						#conn.sendall(entry)
						send_message(conn,entry,"OK")
				if len(decoded_data_list) == 0 :
					conn.sendall("-2")
				



		return


connection_opened = 0 
flush_all(1)
t = threading.Thread(target=flush_thread)
t.start()
lock = threading.Lock()
connection_threads = []

def start_connection_thread(conn,addr,port,lock) : 
	global sock_dict
	global FREE_PORTS
	global USED_PORTS
	global no_of_concurrent_connections
	global mutex
	sock = sock_dict[addr]
	connection_opened = 1
	if connection_opened == 1 :
		message_length_received = 0
		message_body_received = 0
		message_body = ""
		message_length = 0
		message_length_size = 32
		message_type_size = 2
		print "New connection thread started with client : ", addr
		try :

			# Receive the data in small chunks and retransmit it
			while True :
				if message_length_received == 0 :			
					print "Server : Waiting for next message from client .."
					data = conn.recv(message_length_size)
					if data == ""  or data == "close":
						break
					message_length = int(data)
					print "Server : Expecting a message of length",message_length
					message_length_received = 1
					message_body_received = 0
				elif message_body_received == 0 :
					data = ""
					while len(data) < message_length :
						data += conn.recv(4096)
				
					if data:
						#print >>sys.stderr, 'sending data back to the client'
						print "Server : Received a message of the expected length. Verifying the signature"
						
						if message_length > 0 :
							#time.sleep(5)
							message_body = data
							perform_operations(message_body,message_length_size,message_type_size,conn)													
							message_body_received = 1
							message_length_received = 0
							#break
					
					else:
						print >>sys.stderr, 'No data received from', addr
						#break
						message_length_received = 0
	
       				
			
				# Clean up the connection
		except socket.error, msg :
			print "Thread on src addr : ", addr ," Stopped"
			lock.acquire()
			FREE_PORTS.append(port)
			USED_PORTS.remove(port)
			sock_dict.pop(addr,None)
			lock.release()
			exit()


		conn.close()
		connection_opened = 0
		sock.close()

	lock.acquire()
	FREE_PORTS.append(port)
	USED_PORTS.remove(port)
	no_of_concurrent_connections -= 1
	sock_dict.pop(addr,None)
	lock.release()
	print "Connection thread started with client : ", addr, " Stopped"

while True :

	try:

		if no_of_concurrent_connections <= Max_no_of_concurrent_connections :
			HOST = my_IP_address   # Symbolic name meaning all available interfaces	
			
			PORT = FREE_PORTS[0] 	# Arbitrary non-privileged port
			USED_PORTS.append(PORT)
			FREE_PORTS.pop(0)
 
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			print 'New Socket created'
 
			try:
				s.bind((HOST, PORT))
			except socket.error , msg:
				print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
				sys.exit(-2)
     
			print 'Socket bind complete on HOST : ', my_IP_address, " PORT : ", PORT
			s.listen(10)		
			print 'Socket now listening. Waiting for new connection'
 
			#wait to accept a connection - blocking call
			conn, addr = s.accept()
			print >>sys.stderr, 'Server : Connection established with ', addr
			sock_dict[addr] = s
			connection_opened = 0
			t = threading.Thread(target=start_connection_thread, args=(conn,addr,PORT,lock,))
			connection_threads.append(t)
			t.start()
			no_of_concurrent_connections += 1
		'''
		if connection_opened == 1 :
			message_length_received = 0
			message_body_received = 0
			message_body = ""
			message_length = 0
			message_length_size = 32
			message_type_size = 2
		

			# Receive the data in small chunks and retransmit it
			while True :
				if message_length_received == 0 :			
					print "Server : Waiting for next message from client .."
					data = conn.recv(message_length_size)
					if data == ""  or data == "close":
						break

					message_length = int(data)
					print "Server : Expecting a message of length",message_length
					message_length_received = 1
					message_body_received = 0
				elif message_body_received == 0 :
					data = ""
					while len(data) < message_length :
						data += conn.recv(4096)
					
					if data:
						#print >>sys.stderr, 'sending data back to the client'
						print "Server : Received a message of the expected length. Verifying the signature"
						
						if message_length > 0 :
							#time.sleep(5)
							message_body = data
							perform_operations(message_body,message_length_size,message_type_size,conn)													
							message_body_received = 1
							message_length_received = 0
							#break
					
					else:
						print >>sys.stderr, 'No data received from', addr
						#break
						message_length_received = 0


        				
			
			# Clean up the connection
			conn.close()
			connection_opened = 0
			s.close()
 
		'''

	except  KeyboardInterrupt:
		print "Quitting server now ! Good bye"
		thread_stop = 1
		time.sleep(1)
		 
		for addr in sock_dict :
			s = sock_dict[addr]
			if s != None :
				s.close()
		exit()
	except Exception,e:
		print str(e)
		thread_stop = 1
		time.sleep(1)
		s.close() 
		exit()



	



			

	

		
