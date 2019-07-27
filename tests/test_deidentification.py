from sys import path
import os
import csv
path.append('../src/')
from veil_test import veil

def test_deidentification():
    errors = []
    with open('../tests/test_with_ts2.csv', 'r', encoding='utf-8-sig') as r, \
    open('../tests/test2_results.csv', 'w') as w:
        reader = csv.DictReader(r)
        writer = csv.DictWriter(w, reader.fieldnames)

        v = veil()
        v.add_id_map('ID')
        v.deidentify(reader, writer, \
        time_columns = ['Timestamp 1 ','Timestamp 2'], datetime_base_column = 'ID',\
            id_columns = 'ID')
        
    with open('../tests/test_with_ts2.csv', 'r', encoding='utf-8-sig') as r, \
    open('../tests/test2_results.csv', 'r') as w:
        reader = csv.DictReader(r)
        reader2 = csv.DictReader(w)

        if reader.fieldnames != reader2.fieldnames:
            errors.append("Field names are not consistent")
        
        field_dicts = {'ID': [], 'Timestamp 1 ': [], 'Timestamp 2': []}

        for row in reader:
            field_dicts['ID'].append(row['ID'])
            field_dicts['Timestamp 1 '].append(row['Timestamp 1 '])
            field_dicts['Timestamp 2'].append(row['Timestamp 2'])
        
        for i, row in enumerate(reader2):
            if row['ID'] == field_dicts['ID'][i]:
                errors.append('Matching IDs found')
            elif row['Timestamp 1 '] == field_dicts['Timestamp 1 '][i]:
                errors.append('Matching timestamps found')
            elif row['Timestamp 2'] == field_dicts['Timestamp 2'][i]:
                errors.append('Matching timestamps found')
    assert not errors, "errors occured:\n{}".format("\n".join(errors))
