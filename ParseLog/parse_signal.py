import csv
import numpy as np
from datetime import datetime

fileName = 'mobile_signal_info_all_september.csv'

reader = csv.reader(open(fileName, 'rb'))

def floatify(x):
	if len(x) == 0:
		return 0.0
	else:
		return float(x)

data = []
timestamps = []

for row in reader:

    i += 1

    if i == 1:
        schema = row

    else:
        timestamps += [datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')]
        info = row[1:18]
        data += [np.asarray([floatify(x) for x in info])]
        print i

data = np.asarray(data)