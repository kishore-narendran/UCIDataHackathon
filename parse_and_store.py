import csv
import psycopg2

def print_status(started, file_name):
    if started:
        print '========================================================================'
        print 'Started parsing and writing', file_name, 'to PostgreSQL'
        print '========================================================================'
    else:
        print '========================================================================'
        print 'Finished parsing and writing', file_name, 'to PostgreSQL'
        print '========================================================================'

def insert_into_records(conn, records):
    #args_str = ','.join(cur.mogrify("(%s, %s, %s, %s, %s, %s)", x) for x in records)
    # Wifi Info
    # args_str = ','.join(cur.mogrify("(%s, %s, %s, %s, %s)", x) for x in records)
    # Mobile Signal Info
    # args_str = ','.join(cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s)", x) for x in records)
    # Log Info
    # args_str = ','.join(cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", x) for x in records)
    cur.execute("INSERT INTO mobile_signal_info VALUES " + args_str)
    conn.commit()

def is_real(num):
    try:
        num = float(num)
        return True
    except ValueError:
        return False

file_name = 'mobdata/log_data_september_first_half.csv'
first_row_length = 41
print_status(True, file_name)

# Reading the CSV file, and creating a CSV reader handle
handle = open( file_name, 'r' )
reader = csv.reader( handle )

try:
    conn = psycopg2.connect("dbname='phone_database' user='phone_user' host='localhost' password='phone_password'")
except:
    print 'I am unable to connect to the database'

cur = conn.cursor()
i = 0

records = []
for row in reader:
    if i == 0:
        i += 1
        continue

    print row
    row_length = len(row)

    # if row_length != first_row_length:
    #     print '# of Attributes Mismatch, Skipping Line', i
    #     continue

    # i t t t i i 0 1 2 3 5 7 - Application Versions
    # i i t 0 1 2 - Carriers
    # i i i t i t 0 1 6 7 8 14 - Devices
    # t i f f f i - Device Battery Stats
    # t i i i i 0 1 4 5 6 - Wifi Info
    # t i f f f f f f f - 0 1 2 5 8 10 14 16 17 - Mobile Signal Info
    # i t t i f x 4 i x 12
    out_record = [
        int(row[0]) if row[0].isdigit() else -1,
        row[4],
        row[5],
        int(row[7]) if row[7].isdigit() else -1,
        float(row[9]) if is_real(row[9]) else None,
        float(row[10]) if is_real(row[10]) else None,
        float(row[11]) if is_real(row[11]) else None,
        float(row[12]) if is_real(row[12]) else None,
        int(row[13]) if row[13].isdigit() else -1,
        int(row[14]) if row[14].isdigit() else -1,
        int(row[15]) if row[15].isdigit() else -1,
        int(row[16]) if row[16].isdigit() else -1,
        int(row[17]) if row[17].isdigit() else -1,
        int(row[18]) if row[18].isdigit() else -1,
        int(row[19]) if row[19].isdigit() else -1,
        int(row[20]) if row[20].isdigit() else -1,
        int(row[21]) if row[21].isdigit() else -1,
        int(row[22]) if row[22].isdigit() else -1,
        int(row[23]) if row[23].isdigit() else -1,
        int(row[24]) if row[24].isdigit() else -1,
    ]
    # #For device battery stats
    # out_record = [
    #     row[0],
    #     int(row[1]) if row[1].isdigit() else -1,
    #     float(row[2]) if is_real(row[2]) else 0.0,
    #     float(row[3]) if is_real(row[3]) else 0.0,
    #     float(row[4]) if is_real(row[4]) else 0.0,
    #     't' if row[5] == 1 else 'f',
    # ]
    #
    # # For Wifi Info
    # out_record = [
    #     row[0],
    #     int(row[1]) if row[1].isdigit() else -1,
    #     int(row[4]) if row[4].isdigit() else -1,
    #     int(row[5]) if row[5].isdigit() else -1,
    #     int(row[6]) if row[6].isdigit() else -1
    #  ]

    # # For Mobile Signal Info
    # out_record = [
    #     row[0],
    #     int(row[1]) if row[1].isdigit() else -1,
    #     float(row[2]) if is_real(row[2]) else None,
    #     float(row[5]) if is_real(row[5]) else None,
    #     float(row[8]) if is_real(row[8]) else None,
    #     float(row[10]) if is_real(row[10]) else None,
    #     float(row[14]) if is_real(row[14]) else None,
    #     float(row[16]) if is_real(row[16]) else None,
    #     float(row[17]) if is_real(row[17]) else None,
    # ]
    if out_record[0] == -1:
        continue
    records.append(out_record)

    i += 1

    if i % 500000 == 0:
        insert_into_records(conn, records)
        print 'Parsed, executed and inserted', i ,'records'
        records = []

print 'Total records written',i
insert_into_records(conn, records)
handle.close()
conn.close()
print_status(False, file_name)
