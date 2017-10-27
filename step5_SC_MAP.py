#This program creates a map from the UNITID variable to variables
#that are of interest, but not used in model building

import dill
import sqlite3
import numpy as np
import pandas

#load SCORES data (from Model_Generate_Results.py), and extract UNITIDs from SCORES
#Do this to get UNITDS of colleges in the analysis
SCORES = dill.load(open('SCORES.dill', 'r'))
SCORES.reset_index(inplace = True)
UNITID_list = SCORES['UNITID'].tolist()
UNITID_list = [str(x) for x in UNITID_list]

#connect to SQL database
connection = sqlite3.connect("/home/vagrant/datacourse/Capstone/Data/database.sqlite")
cursor = connection.cursor()

#what information to we want to link to UNITID?
map_vars = [u'UNITID', u'INSTNM']

#specify SQL code
sql = "SELECT DISTINCT " + ",".join(map_vars) + " FROM Scorecard " +
                "WHERE (Year BETWEEN 2001 AND 2015) AND UNITID IN (" + ",".join(UNITID_list) +");"
cursor.execute(sql)
result = cursor.fetchall()

#load results into pandas dataframe, and save
import pandas as pd
SC_MAP = pd.read_sql_query(sql, sqlite3.connect("/home/vagrant/datacourse/Capstone/Data/database.sqlite"))
SC_MAP.sort_values(['UNITID'], inplace =True)
dill.dump(SC_MAP, open('SC_MAP.dill', 'w'))
