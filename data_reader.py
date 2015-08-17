import csv

filename = "/home/raven/Documents/ECE-542 Project/data archives/data_sept112013.csv"



with open(filename, "rb") as csvfile:
	datareader = csv.reader(csvfile)
	count = 0
	
	for row in datareader:
		if count == 10 :
			break
		else :
			print "Row no ",count ," = ", row
			timestamp_fname = row[0] + ".txt"

			count += 1


