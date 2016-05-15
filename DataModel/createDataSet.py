import numpy as np
import msgpack
from datetime import datetime
import msgpack_numpy as m

log_schemaIX =	{
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

log_fileNames = ['../Data/log_data_september_first_half_complete', '../Data/log_data_september_second_half_complete']
databases = []

for log_fileName in log_fileNames:
	dataFile = open(log_fileName, 'rb')
	half_database = msgpack.unpackb(dataFile.read())
	dataFile.close()
	print 'Finished loading', log_fileName

	databases += [half_database]

print 'Finished loading all log databases into memory'

dataFile = open('../Data/device_app_mappings', 'rb')
device_app_mapping = msgpack.unpackb(dataFile.read())
dataFile.close()

log_dataBase = {}

for deviceType, appIDs in device_app_mapping.items():
	assert (deviceType not in log_dataBase), 'Repeated device type'
	log_dataBase[deviceType] = {}
	print deviceType

	for appID in appIDs:
		dataPt_keys = []
		X = []
		y = []

		for database in databases:
			try:
				data = (half_database[deviceType])[appID]
				log_data_schema = 	[
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

				for device_id, log in data.items():
					for timestamp, entry in log.items():
						dataPt_keys += [ (device_id, datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')) ]
						y += [ entry[log_schemaIX['battery']] + entry[log_schemaIX['back_battery']] ]

						dataPt = []
						for field in log_data_schema:
							dataPt += [entry[log_schemaIX[field]]]

						X += [np.asarray(dataPt)]
			except:
				#print 'App', appID, 'not found on device type', deviceType
				pass

		X = np.asarray(X)
		y = np.asarray(y)

		assert (appID not in log_dataBase[deviceType].keys()), 'Repeated app ID'
		log_dataBase[deviceType][appID] = (X, y, dataPt_keys)
		#print '\t',appID