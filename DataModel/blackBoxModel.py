import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error as mse
import msgpack
from datetime import datetime

dataFile = open('../Data/device_app_mappings', 'rb')
device_app_mapping = msgpack.unpackb(dataFile.read())
dataFile.close()

models = {}

for deviceType, appIDs in device_app_mapping.items():
	assert (deviceType not in models), 'Repeated device type'
	models[deviceType] = {}
	print deviceType

	for appID in appIDs:
		try:
			X, y, ids = log_dataBase[deviceType][appID]
			blackBox = LinearRegression(normalize=True)
			blackBox.fit(X, y)

			p = blackBox.predict(X)
			model = (blackBox.coef_, blackBox.intercept_, blackBox.mse(y,p)/len(y))
			models[deviceType][appID] = model
		except:
			pass

file_name = 'blackBoxes'
print '========================================================================'
print 'Started packing into Msgpack ', file_name
print '========================================================================'
handle = open(file_name.split('.')[0] + '_complete', 'wb')
handle.write(msgpack.packb(models, default=m.encode))
handle.close()
print '========================================================================'
print 'Finished packing into Msgpack ', file_name
print '========================================================================'