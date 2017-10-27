
#This program loads the raw .csv datafiles into a SQL database

import csv
import sqlite3
# Process data dictionary
sqlite_types = {
    "integer": "INTEGER",
    "string":  "TEXT",
    "float":   "REAL"
}
name_col=1
type_col=0
value_col=2
label_col=3
previous_col = None
columns = {}
for (i, row) in enumerate(csv.reader(open("/home/vagrant/datacourse/Capstone/Data/CollegeScorecard_DataDictionary.csv"))):
    if i==0:
        assert row[name_col]=="VARIABLE NAME"
        assert row[type_col]=="API data type"
        assert row[value_col]=="VALUE"
        assert row[label_col]=="LABEL"
        continue
    if row[name_col].strip() != "":
        previous_col = row[name_col]
        if row[type_col]=="integer" and row[value_col].strip() != "":
            assert row[label_col].strip() != ""
            columns[row[name_col]] = {"type": sqlite_types["string"],
                                      "key": {row[value_col]: row[label_col]}}
        elif row[type_col] in sqlite_types:
            assert row[value_col].strip() == ""
            columns[row[name_col]] = {"type": sqlite_types[row[type_col]]}
        else:
            raise Exception("Unexpected type: %s" % row[type_col])
    else:
        assert row[value_col] != ""
        assert row[label_col] != ""
        columns[previous_col]["key"][row[value_col]] = row[label_col]
print(len(columns))
columns["Year"] = {"type": sqlite_types["integer"]}
columns["Id"]   = {"type": "INTEGER PRIMARY KEY"}

# Correcting errors in dictionary/data combination
for col in ["CIP01CERT1", "CIP01CERT2", "CIP01ASSOC"]:
    if "0" not in columns[col]["key"]:
        columns[col]["key"]["0"] = "Program not offered"
    if "1" not in columns[col]["key"]:
        columns[col]["key"]["1"] = "Program offered"
    if "2" not in columns[col]["key"]:
        columns[col]["key"]["2"] = "Program offered through an exclusively distance-education program"
if "68" not in columns["ST_FIPS"]["key"]:
    columns["ST_FIPS"]["key"]["68"] = "Unknown"
if "0" not in columns["CCUGPROF"]["key"]:
    columns["CCUGPROF"]["key"]["0"] = "Unknown"

#Process data files

filename_from_years = []
years = range(1996, 2016)
for year in years:
    end_year = str("%02d" % ((year+1)%100,))
    year = str(year)
    filename_from_years.append("/home/vagrant/datacourse/Capstone/Data/MERGED" + year + "_" + end_year + "_PP.csv")


#filename_from_years = lambda year: "/home/vagrant/datacourse/Capstone/Data/MERGED%d_%d_PP.csv" % year

r = csv.reader(open(filename_from_years[0]))
rows = list(r)
header_raw = rows[0]

w = csv.writer(open("/home/vagrant/datacourse/Capstone/Data/Scorecard.csv", "w"))

if header_raw[0]=='\xef\xbb\xbfUNITID':
    print("removing unicode from header")
    header = ['UNITID'] + header_raw[1:] + ['Year']
else:
    header = header_raw + ['Year']
header = ['Id'] + header

for missing in set(header).difference(set(columns.keys())):
    print("Adding column %s as string type" % missing)
    columns[missing] = {"type": sqlite_types["string"]}


print header

in_dictionary_not_header = set(columns.keys()).difference(set(header))
if in_dictionary_not_header:
    raise Exception("Not handling case where items in dictionary aren't in header: %s" % in_dictionary_not_header)
#Exception: Not handling case where items in dictionary aren't in header:
#set(['D150_L4_HISPOld', 'D150_L4_AIANOld', 'D150_4_AIANOld', 'D150_4_HISPOld'])

w.writerow(header)

sqlite_schema = ["    %s %s," % (col, columns[col]["type"]) for col in header]
sqlite_schema[-1] = sqlite_schema[-1][:-1] + ");"
sqlite_schema = ["CREATE TABLE IF NOT EXISTS Scorecard ("] + sqlite_schema
sqlite_schema = "\n".join(sqlite_schema)
conn = sqlite3.connect("/home/vagrant/datacourse/Capstone/Data/database.sqlite")
conn.text_factory = str
curs = conn.cursor()
curs.execute(sqlite_schema)

def transform(x, col_name, columns):
    if x=="NULL":
        return ""
    if "key" in columns[col_name]:
        try:
            return columns[col_name]["key"][x]
        except:
            print("Key %s not found in column %s" %(x, col_name))
    return x

row_id=0

for year_index in range(len(years)):
    rows = list(csv.reader(open(filename_from_years[year_index])))
    if header_raw != rows[0]:
        raise Exception("Different headers")
    for row in rows[1:]:
        if row_id % 1000 == 0:
            print("row: %d" % row_id)
        row_id += 1
        #add NULL values for old variables
        row = [row_id] + row + [str(years[year_index])] + ['NULL', 'NULL', 'NULL', 'NULL']
        row = [transform(row[i], col, columns) for (i, col) in enumerate(header)]
        insert_statement = "INSERT INTO Scorecard (%s) VALUES (%s)" % (",".join([col for (i, col) in enumerate(header[:900]) if row[i]]), ",".join(["?" for el in row[:900] if el]))
        curs.execute(insert_statement, [el for el in row[:900] if el])
        update_statement = "UPDATE Scorecard SET %s WHERE Id=%d" % (",".join([col+"=?" for (i, col) in enumerate(header[900:]) if row[i+900]]), row_id)
        curs.execute(update_statement, [el for el in row[900:] if el])
        w.writerow(row)

conn.commit()

curs = conn.cursor()
curs.execute("CREATE INDEX scorecard_instnm_ix ON Scorecard (INSTNM);")
conn.commit()

curs = conn.cursor()
curs.execute("VACUUM;")
conn.commit()



#errors:
#row: 132000 - row: 139000
#Key -3 not found in column CCBASIC
