import csv
from collections import Counter
path = '/Users/aravindreddy/Downloads/SJSU ClassWork/275 EAD/275_Mini2_Dataset/parking_violations_combined.csv'
county = Counter()
precinct = Counter()
body = Counter()
state = Counter()
unregistered = Counter()
violation = Counter()
plate = Counter()

def norm(x):
    return x.strip()

with open(path, newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        county[norm(row['Violation County'])] += 1
        precinct[norm(row['Violation Precinct'])] += 1
        body[norm(row['Vehicle Body Type'])] += 1
        state[norm(row['Registration State'])] += 1
        unregistered[norm(row['Unregistered Vehicle?'])] += 1
        violation[norm(row['Violation Code'])] += 1
        plate[norm(row['Plate ID'])] += 1

print('Top counties:', county.most_common(10))
print('Top precincts:', precinct.most_common(10))
print('Top body types:', body.most_common(20))
print('Top states:', state.most_common(20))
print('Unregistered:', unregistered)
print('Top violations:', violation.most_common(20))
print('Top plates:', plate.most_common(20))
