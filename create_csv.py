import csv

rows = [
    ['John', 'Doe', '25'],
    ['Jane', 'Smith', '30'],
    ['Bob', 'Johnson', '40'],
    ['Samantha', 'Lee', '20'],
    ['David', 'Brown', '35'],
    ['Emily', 'Davis', '28'],
    ['Michael', 'Garcia', '33'],
    ['Olivia', 'Wilson', '27'],
    ['William', 'Taylor', '42'],
    ['Isabella', 'Anderson', '23'],
    ['Ethan', 'Thomas', '37'],
    ['Ava', 'Moore', '31'],
    ['Daniel', 'Clark', '29'],
    ['Sophia', 'Scott', '24'],
    ['Matthew', 'Allen', '38']
]

with open('data.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(rows)
