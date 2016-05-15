import msgpack

file_name = 'log_data_september_first_half_complete'
handle = open(file_name, 'rb')
b = msgpack.unpackb(handle.read())
handle.close()
