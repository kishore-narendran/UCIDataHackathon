import msgpack

file_name = 'mobile_signal_info_all_september_complete'
handle = open(file_name, 'rb')
b = msgpack.unpackb(handle.read())
handle.close()

for key in b.keys():
    print b[key]
    break
