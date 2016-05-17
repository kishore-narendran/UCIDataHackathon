import numpy as np
from scipy.stats.mstats import mode
import msgpack_numpy as msgnp

from connectDB import *
db = connectDB()
device_app_groups = db.read_table('device_app_groups')

device_app_groups.columns
len(device_app_groups)

all_devices = device_app_groups['device_type_id']
all_devices = list(set(all_devices))

all_apps = device_app_groups['application_version_id']
all_apps = list(set(all_apps))

device_groups = device_app_groups.groupby('device_type_id')
app_counts = device_groups.size()
max(app_counts)
min(app_counts)
np.mean(app_counts)
mode(app_counts)
np.median(app_counts)

devices_enough_apps = app_counts[app_counts >= 100].index
device_100app_groups = device_app_groups[device_app_groups['device_type_id'].isin(devices_enough_apps)]

schema = device_100app_groups.columns

device_appBase = {}
for device_type in devices_enough_apps:
	device_apps = device_100app_groups[device_100app_groups['device_type_id'] == device_type]
	X = device_apps.as_matrix(columns=schema[2:])
	app_ids = device_apps.as_matrix(columns = [schema[1]])
	device_appBase[device_type] = (X, app_ids)

handle = open('devAppBase_first_september', 'wb')
handle.write(msgnp.packb(device_appBase, default=msgnp.encode))
handle.close()

handle = open('devAppBase_first_september', 'rb')
dev_AppBase = msgnp.unpackb(handle.read(), object_hook=msgnp.decode)
handle.close()