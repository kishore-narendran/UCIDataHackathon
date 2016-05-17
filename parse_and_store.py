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
    args_str = ','.join(cur.mogrify("(%s, %s, %s)", x) for x in records)
    cur.execute("INSERT INTO carriers VALUES " + args_str)
    conn.commit()

file_name = 'mobdata/carriers.csv'
first_row_length = 3
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

    try:
        row_length = len(row)

        if row_length != first_row_length:
            print '# of Attributes Mismatch, Skipping Line', i

        out_record = [int(row[0]) if row[0].isdigit() else -1, int(row[1]) if row[1].isdigit() else -1, row[2]]
        records.append(out_record)

    except:
        print 'Device ID', row[0], 'Timestamp', row[4], 'App Version ID', row[7], 'Battery', row[9], 'Back battery', row[10]
        print 'Row', i
        print 'File Name', file_name

    i += 1

    if i % 10000 == 0:
        insert_into_records(conn, records)
        print 'Parsed, executed and inserted', i ,'records'
        records = []

insert_into_records(conn, records)
handle.close()
conn.close()
print_status(False, file_name)
