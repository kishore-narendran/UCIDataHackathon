import pickle
import csv


file_names = ['log_data_september_first_half.csv',
            'log_data_september_second_half.csv',
            'log_data_october_first_half.csv',
            'log_data_october_second_half.csv',
            'log_data_november_first_half.csv',
            'log_data_november_second_half.csv']

all_log_info = dict()
for file_name in file_names:
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

        try:
            if row_length != first_row_length:
                print 'First Row Length', first_row_length
                print 'Row Length', row_length
                break
            device_id = int(row[0])
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

            if device_id not in log_info:
                log_info[device_id] = {}
                log_info[device_id][timestamp] = {}
                log_info[device_id][timestamp][app_version_id] = {'app_version_id': app_version_id,
                                                                    'battery': battery,
                                                                    'back_battery': back_battery,
                                                                    'cpu': cpu,
                                                                    'back_cpu': back_cpu,
                                                                    'run_time': run_time,
                                                                    'front_run_time': front_run_time,
                                                                    'data_all': data_all,
                                                                    'back_data': back_data,
                                                                    'data_wifi': data_wifi,
                                                                    'data_mobile': data_mobile}
            elif timestamp not in log_info[device_id]:
                log_info[device_id][timestamp] = {}
                log_info[device_id][timestamp][app_version_id] = {'app_version_id': app_version_id,
                                                                    'battery': battery,
                                                                    'back_battery': back_battery,
                                                                    'cpu': cpu,
                                                                    'back_cpu': back_cpu,
                                                                    'run_time': run_time,
                                                                    'front_run_time': front_run_time,
                                                                    'data_all': data_all,
                                                                    'back_data': back_data,
                                                                    'data_wifi': data_wifi,
                                                                    'data_mobile': data_mobile}
            elif app_version_id not in log_info[device_id][timestamp]:
                log_info[device_id][timestamp][app_version_id] = {'app_version_id': app_version_id,
                                                                'battery': battery,
                                                                'back_battery': back_battery,
                                                                'cpu': cpu,
                                                                'back_cpu': back_cpu,
                                                                'run_time': run_time,
                                                                'front_run_time': front_run_time,
                                                                'data_all': data_all,
                                                                'back_data': back_data,
                                                                'data_wifi': data_wifi,
                                                                'data_mobile': data_mobile}

        except:
            print 'Row', i
            print 'File Name', file_name

        i += 1

    log_file.close()
    print 'Finished parsing ', file_name
    with open(file_name.split('.')[0] + '_complete' + '.pickle', 'wb') as handle:
      pickle.dump(log_info, handle)
    all_log_info.update(log_info)
    log_info = dict()

with open('loginfo_complete.pickle', 'wb') as handle_allinfo:
  pickle.dump(all_log_info, handle_allinfo)
