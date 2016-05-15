import numpy as np
import msgpack
from datetime import datetime

deviceType = 26126
appID = 28893190

fileName = '../Data/log_data_september_first_half_complete'

schemaIX =	{
				'battery' : 0,
				'back_battery' : 1,
				'cpu' : 2,
				'back_cpu' : 3,
				'run_time' : 4,
				'front_run_time' : 5,
				'data_all' : 6,
				'back_data' : 7,
				'data_wifi' : 8,
				'data_mobile' : 9,
				'memory' : 10,
				'data_size' : 11,
				'cache_size' : 12,
				'other_size' : 13
		 	}

# open database
dataFile = open(fileName, 'rb')
database = msgpack.unpackb(dataFile.read())
dataFile.close()

try:
	data = (database[deviceType])[appID]
	data_schema = 	[
						'cpu',
						'back_cpu',
						'run_time',
						'front_run_time',
						'back_data',
						'data_wifi',
						'data_mobile',
						'memory',
						'data_size',
						'cache_size',
						'other_size'
					]

	#create datasets
	timestamps = []
	X = []
	y = []

	for timestamp, entry in data.items():
		timestamps += [datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')]
		y += [ entry[schemaIX['battery']] * entry[schemaIX['run_time']] ]

		dataPt = []
		for field in data_schema:
			dataPt += [entry[schemaIX[field]]]

		X += [np.asarray(dataPt)]

	X = np.asarray(X)
	y = np.asarray(y)

except:
	print 'App not found on any such device'