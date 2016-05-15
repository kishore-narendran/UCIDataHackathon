import msgpack
import csv

file_name = 'mobile_signal_info_all_september.csv'

print '========================================================================'
print 'Started parsing ', file_name
print '========================================================================'
signal_info = dict()
signal_file = open( file_name, 'r' )
reader = csv.reader( signal_file )
i = 0
for row in reader:
    if i == 0:
        i += 1
        first_row_length = len(row)
        continue

    row_length = len(row)

    if row_length != first_row_length:
        print 'First Row Length', first_row_length
        print 'Row Length', row_length
        break
    try:
        device_id = int(row[1])
        timestamp = row[0]
        cdma_dbm = float(row[2]) if len(row[2]) is not 0 else 0.0
        evdo_dbm = float(row[5]) if len(row[5]) is not 0 else 0.0
        gsm_evdo = float(row[8]) if len(row[8]) is not 0 else 0.0
        lte_dbm = float(row[10]) if len(row[10]) is not 0 else 0.0
        longitude = float(row[16]) if len(row[16]) is not 0 else 0.0
        latitude = float(row[17]) if len(row[17]) is not 0 else 0.0

        if device_id not in signal_info:
            signal_info[device_id] = {}
            signal_info[device_id][timestamp] = [cdma_dbm, evdo_dbm, gsm_evdo, lte_dbm, longitude, latitude]
        elif timestamp not in signal_info[device_id]:
            signal_info[device_id][timestamp] = [cdma_dbm, evdo_dbm, gsm_evdo, lte_dbm, longitude, latitude]

    except:
        print 'Row', i
        print 'File Name', file_name

    i += 1
    # if i == 2:
    #     break
    if i & 0b1111111111111111 == 0b1111111111111111:
        print 'Parsed', i, 'mobile signal info'

# print signal_info

signal_file.close()
print '========================================================================'
print 'Finished parsing ', file_name
print '========================================================================'

print '========================================================================'
print 'Started packing into Msgpack ', file_name
print '========================================================================'
handle = open(file_name.split('.')[0] + '_complete', 'wb')
handle.write(msgpack.packb(signal_info))
handle.close()
print '========================================================================'
print 'Finished packing into Msgpack ', file_name
print '========================================================================'
