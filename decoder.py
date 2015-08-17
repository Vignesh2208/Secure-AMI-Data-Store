from pyeclib.ec_iface import ECDriver
import os
from os.path import expanduser
import ctypes



def decode(k,m,base_filename_to_decode,ec_type) :

	#print("k = %d, m = %d" % (k, m))
	#print("ec_type = %s" % ec_type)
	home = expanduser("~")	
	Disk_base_dir = home + '/Disk'
	key_list = base_filename_to_decode.split('.')
	jdecoder_types = ["reed_sol_van", "reed_sol_r6_op","cauchy_orig","cauchy_good","liberation","blaum_roth","liber8tion"]
	if ec_type in jdecoder_types :
		print "Decoding started "
		jdecoder = ctypes.CDLL('/home/raven/Downloads/Jerasure-master/Examples/jdecoder.so')
		key = key_list[0]
		decoded_data = str(jdecoder.decode(key,Disk_base_dir))
		print "decoded data = ", decoded_data
	else :
		ec_driver = ECDriver(k = k, m = m, ec_type = ec_type)

		fragment_list = []
	
	
		i = 0
		while i <=k :
			if os.path.exists(Disk_base_dir + "/" + "Disk_" + str(i)) :
				curr_dir = Disk_base_dir + "/" + "Disk_" + str(i)
				if i == 0 :
					if os.path.exists(curr_dir + "/" + base_filename_to_decode) :
						with open(curr_dir + "/" + base_filename_to_decode,'rb') as fp :
							decoded_data = fp.read()					
						fp.close()
						return decoded_data
				else :
					if os.path.exists(curr_dir + "/" + base_filename_to_decode + "_fragment_" + str(i)) :
						with open(curr_dir + "/" + base_filename_to_decode + "_fragment_" + str(i),'rb') as fp :
							fragment = fp.read()					
						fp.close()
						fragment_list.append(fragment)
					
			i = i + 1
	
		if len(fragment_list) < k :
			return -1 # Not enough fragments to decode
		else :
			
			decoded_data = ec_driver.decode(fragment_list)	
	return decoded_data	
		
