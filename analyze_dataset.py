import csv
from datetime import datetime

path = '/Users/aravindreddy/Downloads/SJSU ClassWork/275 EAD/275_Mini2_Dataset/parking_violations_combined.csv'
count = 0
plate_counts = {}
violation_counts = {}
county_counts = {}
precinct_counts = {}
body_type_counts = {}
state_counts = {}
unregistered_counts = {}
feet_counts = {}

with open(path, newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        count += 1
        if count > 100000: break  # Sample first 100k for speed
        plate = row['Plate ID'].strip()
        violation = row['Violation Code'].strip()
        county = row['Violation County'].strip()
        precinct = row['Violation Precinct'].strip()
        body = row['Vehicle Body Type'].strip()
        state = row['Registration State'].strip()
        unregistered = row['Unregistered Vehicle?'].strip()
        feet = row['Feet From Curb'].strip()
        
        plate_counts[plate] = plate_counts.get(plate, 0) + 1
        violation_counts[violation] = violation_counts.get(violation, 0) + 1
        county_counts[county] = county_counts.get(county, 0) + 1
        precinct_counts[precinct] = precinct_counts.get(precinct, 0) + 1
        body_type_counts[body] = body_type_counts.get(body, 0) + 1
        state_counts[state] = state_counts.get(state, 0) + 1
        unregistered_counts[unregistered] = unregistered_counts.get(unregistered, 0) + 1
        feet_counts[feet] = feet_counts.get(feet, 0) + 1

print('Top plates:', sorted(plate_counts.items(), key=lambda x: x[1], reverse=True)[:5])
print('Top violations:', sorted(violation_counts.items(), key=lambda x: x[1], reverse=True)[:5])
print('Top counties:', sorted(county_counts.items(), key=lambda x: x[1], reverse=True)[:5])
print('Top precincts:', sorted(precinct_counts.items(), key=lambda x: x[1], reverse=True)[:5])
print('Top body types:', sorted(body_type_counts.items(), key=lambda x: x[1], reverse=True)[:5])
print('Top states:', sorted(state_counts.items(), key=lambda x: x[1], reverse=True)[:5])
print('Unregistered:', unregistered_counts)
print('Feet from curb:', sorted(feet_counts.items(), key=lambda x: x[1], reverse=True)[:5])