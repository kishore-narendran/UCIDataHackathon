DROP TABLE device_log_data;
DROP TABLE sub_device_log_data;
DROP TABLE device_app_groups;
DROP TABLE sub_device_app_groups;

SELECT 	device_type_id, devices.device_id, log_timestamp, application_version_id, 
		battery, back_battery, cpu, back_cpu, memory, 
		data_all, run_time, front_run_time, 
		data_size, cache_size, code_size 
INTO device_log_data 
FROM devices INNER JOIN log_data
ON devices.device_id = log_data.device_id;

-- SELECT * 
-- INTO sub_device_log_data
-- FROM device_log_data
-- ORDER BY random()
-- LIMIT 100;

SELECT 	device_type_id, application_version_id,
		AVG(battery) as battery, AVG(back_battery) as back_battery, AVG(cpu) as cpu, AVG(back_cpu) as back_cpu,
		AVG(memory) as memory, AVG(data_all) as data_all, AVG(run_time) as run_time, AVG(front_run_time) as front_run_time,
		AVG(data_size) as data_size, AVG(cache_size) as cache_size, AVG(code_size) as code_size
INTO device_app_groups
FROM device_log_data
GROUP BY device_type_id, application_version_id;