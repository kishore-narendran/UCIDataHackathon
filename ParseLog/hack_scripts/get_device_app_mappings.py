import msgpack
import csv

devices_file = open('devices.csv', 'r')
reader = csv.reader(devices_file)

id_to_type_id = {}
for row in reader:
    device_id = int(row[0])
    device_type_id = int(row[6])
    id_to_type_id[device_id] = device_type_id

file_names = [ 'log_data_september_first_half.csv']
                # 'log_data_september_second_half.csv',
                # 'log_data_october_first_half.csv',
                # 'log_data_october_second_half.csv',
                # 'log_data_november_first_half.csv',
                # 'log_data_november_second_half.csv', ]

for file_name in file_names:
    print '========================================================================'
    print 'Started parsing ', file_name
    print '========================================================================'
    device_app_mappings = dict()
    log_file = open( file_name, 'r' )
    reader = csv.reader( log_file )
    i = 0
    for row in reader:
        if i == 0:
            i += 1
            first_row_length = len(row)
            continue

        row_length = len(row)
        device_id = int(row[0])
        app_version_id = int(row[7])
        device_type_id = id_to_type_id[device_id]

        if device_type_id not in device_app_mappings:
            device_app_mappings[device_type_id] = set()
            device_app_mappings[device_type_id].add(app_version_id)
        else:
            device_app_mappings[device_type_id].add(app_version_id)

        i += 1
        if i & 0b1111111111111111 == 0b1111111111111111:
            print 'Parsed', i, 'logs'
    log_file.close()
    print '========================================================================'
    print 'Finished parsing ', file_name
    print '========================================================================'

for key in device_app_mappings.keys():
    device_app_mappings[key] = list(device_app_mappings[key])


print '========================================================================'
print 'Started packing into Msgpack ', file_name
print '========================================================================'
handle = open('device_app_mappings', 'wb')
handle.write(msgpack.packb(device_app_mappings))
handle.close()
print '========================================================================'
print 'Finished packing into Msgpack ', file_name
print '========================================================================'
