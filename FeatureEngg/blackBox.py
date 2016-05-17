import numpy as np
from scipy.stats.mstats import mode
import msgpack_numpy as msgnp
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

from connectDB import *
db = connectDB()
apps = db.read_table('applications')

handle = open('devAppBase_september', 'rb')
dev_AppBase = msgnp.unpackb(handle.read(), object_hook=msgnp.decode)
handle.close()

k = 10

app_clusters = {}
for device_id, data in dev_AppBase.items():
	X, app_ids = data
	model = KMeans(n_clusters = k)
	labels = model.fit_predict(X)
	app_clusters[device_id] = (labels, app_ids)


def cluster(dev_AppBase, device_id, k = 3):
	import numpy as np
	from scipy.stats.mstats import mode
	import msgpack_numpy as msgnp
	from sklearn.cluster import KMeans
	from sklearn.metrics import silhouette_score

	data = dev_AppBase[device_id]
	X, app_ids = data

	clustering = {}

	for k in xrange(2, 11):
		model = KMeans(n_clusters = k)
		labels = model.fit_predict(X)
		score = silhouette_score(X, labels)
		print 'k =', k, ':', score

		k_cluster = {}
		for i in xrange(k):
			apps = app_ids[np.where(labels = i)[0]]
			k_cluster[i] = apps

		clustering[k] = k_cluster

	return clustering