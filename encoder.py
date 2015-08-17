from pyeclib.ec_iface import ECDriver
import os
from os.path import expanduser
import ctypes



def encode(k,m,data_to_encode,base_filename_to_store,ec_type) :

	home = expanduser("~")	
	key_list = base_filename_to_store.split('.')
	Disk_base_dir = home + '/Disk'
	jencoder_types = ["reed_sol_van", "reed_sol_r6_op","cauchy_orig","cauchy_good","liberation","blaum_roth","liber8tion"]


	# Create a replicated copy first.
	#replica_dir = Disk_base_dir + "/Disk_0"
	#if not os.path.exists(replica_dir):
	#	os.makedirs(replica_dir)


	#with open(replica_dir + "/" + base_filename_to_store,'wb') as fp :
	#	fp.write(data_to_encode)
	#fp.close()


	if ec_type in jencoder_types :
		
		jencoder = ctypes.CDLL('/home/raven/Downloads/Jerasure-master/Examples/jencoder.so')
		w = 8
		key = key_list[0]
		jencoder.encode(key,data_to_encode,Disk_base_dir,k,m,ec_type,w,0)
		

	else :
		ec_driver = ECDriver(k = k, m = m, ec_type = ec_type)


		# encode
		fragments = ec_driver.encode(data_to_encode)

		
		# store
		i = 1
		for fragment in fragments:
			fragment_dir = Disk_base_dir + '/' + "Disk_" + str(i)
			if not os.path.exists(fragment_dir):
				os.makedirs(fragment_dir)
	
		 	with open("%s/%s" % (fragment_dir, base_filename_to_store + "_fragment_" + str(i)), "wb") as fp:
				fp.write(fragment)
			fp.close()
			i += 1
