import csv
log_schema = open( 'mobile_signal_info_all_september.csv', 'r' )
reader = csv.reader( log_schema )
j = 0
for row in reader:
    i = 0
    for r in row:
        print i, r
        i += 1
    j += 1
    if j == 2:
        break
