import csv
import json

file_name = 'log_data_september_second_half.csv'
log_info = dict()
log_file = open( file_name, 'r' )
reader = csv.reader( log_file )
i = 0
print '========================================================================'
print 'Started parsing ', file_name
print '========================================================================'
for row in reader:
    if i == 0:
        i += 1
        first_row_length = len(row)
        continue

    try:
        row_length = len(row)

        if row_length != first_row_length:
            print 'First Row Length', first_row_length
            print 'Row Length', row_length
            break
        device_id = int(row[0])
        timestamp = row[4]
        app_version_id = int(row[7])
        battery = float(row[9]) if len(row[9]) is not 0 else 0.0
        back_battery = float(row[10]) if len(row[10]) is not 0 else 0.0

        if device_id not in log_info:
            log_info[device_id] = {}
            log_info[device_id][timestamp] = {}
            log_info[device_id][timestamp][app_version_id] = {'app_version_id': app_version_id, 'battery': battery, 'back_battery': back_battery}
        elif timestamp not in log_info[device_id]:
            log_info[device_id][timestamp] = {}
            log_info[device_id][timestamp][app_version_id] = {'app_version_id': app_version_id,'battery': battery, 'back_battery': back_battery}
        elif app_version_id not in log_info[device_id][timestamp]:
            log_info[device_id][timestamp][app_version_id] = {'app_version_id': app_version_id,'battery': battery, 'back_battery': back_battery}

    except:
        print 'Device ID', row[0], 'Timestamp', row[4], 'App Version ID', row[7], 'Battery', row[9], 'Back battery', row[10]
        print 'Row', i
        print 'File Name', file_name

    i += 1

    if i %1000000 == 0:
        print 'Parsed', i,'logs'

log_file.close()
print '========================================================================'
print 'Finished parsing ', file_name
print '========================================================================'


print '========================================================================'
print 'Started packing into JSON ', file_name
print '========================================================================'
with open(file_name.split('.')[0] + '_partial' + '.json', 'w') as handle:
    json.dump(log_info, handle)
print '========================================================================'
print 'Finished packing into JSON ', file_name
print '========================================================================'
