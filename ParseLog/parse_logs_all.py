import msgpack
import csv

devices_file = open('devices.csv', 'r')
reader = csv.reader(devices_file)

id_to_type_id = {}
for row in reader:
    device_id = int(row[0])
    device_type_id = int(row[6])
    id_to_type_id[device_id] = device_type_id

file_name = 'log_data_september_first_half.csv'

print '========================================================================'
print 'Started parsing ', file_name
print '========================================================================'
log_info = dict()
log_file = open( file_name, 'r' )
reader = csv.reader( log_file )
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
        device_id = int(row[0])
        device_type_id = id_to_type_id[device_id]
        device_os = row[1]
        timestamp = row[4]
        app_version_id = int(row[7])
        battery = float(row[9]) if len(row[9]) is not 0 else 0.0
        back_battery = float(row[10]) if len(row[10]) is not 0 else 0.0
        cpu = float(row[11]) if len(row[11]) is not 0 else 0.0
        back_cpu = float(row[12]) if len(row[12]) is not 0 else 0.0
        run_time = float(row[19]) if len(row[19]) is not 0 else 0.0
        front_run_time = float(row[20]) if len(row[20]) is not 0 else 0.0
        data_all = float(row[14]) if len(row[14]) is not 0 else 0.0
        back_data = float(row[15]) if len(row[15]) is not 0 else 0.0
        data_wifi = float(row[16]) if len(row[16]) is not 0 else 0.0
        data_mobile = float(row[17]) if len(row[17]) is not 0 else 0.0
        memory = float(row[13]) if len(row[13]) is not 0 else 0.0
        data_size = float(row[22]) if len(row[22]) is not 0 else 0.0
        cache_size = float(row[23]) if len(row[23]) is not 0 else 0.0
        other_size = float(row[24]) if len(row[24]) is not 0 else 0.0

        if battery == 0.0 and back_battery == 0.0:
            continue

        if device_type_id not in log_info:
            log_info[device_type_id] = {}
            log_info[device_type_id][app_version_id] = {}
            log_info[device_type_id][app_version_id][timestamp] = [battery, back_battery, cpu, back_cpu, run_time, front_run_time, data_all, back_data, data_wifi, data_mobile, memory, data_size, cache_size, other_size]
        elif app_version_id not in log_info[device_type_id]:
            log_info[device_type_id][app_version_id] = {}
            log_info[device_type_id][app_version_id][timestamp] = [battery, back_battery, cpu, back_cpu, run_time, front_run_time, data_all, back_data, data_wifi, data_mobile, memory, data_size, cache_size, other_size]
        else:
            log_info[device_type_id][app_version_id][timestamp] = [battery, back_battery, cpu, back_cpu, run_time, front_run_time, data_all, back_data, data_wifi, data_mobile, memory, data_size, cache_size, other_size]

    except:
        print 'Row', i
        print 'File Name', file_name

    i += 1
    # if i == 100000:
    #     break
    if i & 0b1111111111111111 == 0b1111111111111111:
        print 'Parsed', i, 'logs'

log_file.close()
print '========================================================================'
print 'Finished parsing ', file_name
print '========================================================================'

print '========================================================================'
print 'Started packing into Msgpack ', file_name
print '========================================================================'
handle = open(file_name.split('.')[0] + '_complete', 'wb')
handle.write(msgpack.packb(log_info))
handle.close()
print '========================================================================'
print 'Finished packing into Msgpack ', file_name
print '========================================================================'
