
###This step gets the predictors data from the SQL database, and filters/wrangles/imputes it.
#Feeds results into step2_Wrangle_Data_Target.py, which generates target variables and links them to the predictor variables


import sqlite3
import pandas as pd
import numpy as np
from sklearn.preprocessing import Imputer

#connect to SQL database
connection = sqlite3.connect("/home/vagrant/datacourse/Capstone/Data/database.sqlite")
cursor = connection.cursor()
cursor.execute("PRAGMA table_info(Scorecard);")
result = cursor.fetchall()
#print out variable names with column #
for r in result:
    print(r)

#gather variable names - select by column number
filter_vars = [u'PREDDEG', u'HIGHDEG', u'CCBASIC', u'CCUGPROF', u'CCSIZSET', u'ICLEVEL', u'OPENADMP', u'GRADS', u'UGDS']
id_vars = [u'UNITID', u'INSTNM', u'Year']
predictor_var_list = set(range(37, 62) + range(62, 100) + range(300, 307) + range(1408, 1434) + range(1607, 1631))
predictor_var_list = predictor_var_list.difference([43, 44, 47, 54, 55, 59, 1608, 1618, 1619])
predictor_vars = []
for r in result:
    if r[0] in predictor_var_list:
        predictor_vars.append(r[1])

#check predictor variables
print predictor_vars

#specify SQL query for getting data from SQL database
#Start with data from 2001 to 2006
get_vars = id_vars + predictor_vars
sql = "SELECT " + ",".join(get_vars) + " FROM Scorecard " +
        "WHERE (Year BETWEEN 2001 AND 2006) AND PREDDEG='Predominantly bachelor''s-degree granting' AND ICLEVEL='4-year' AND " +
        "'OPENADMP'!= 'Yes' AND UGDS IS NOT NULL AND (UGDS > GRADS/4 OR GRADS IS NULL);"

#run SQL query, load into pandas database
SC_Dataframe = pd.read_sql_query(sql, sqlite3.connect("/home/vagrant/datacourse/Capstone/Data/database.sqlite"))
SC_Dataframe.set_index(['UNITID','Year'], inplace=True)

#display database
with pd.option_context('display.max_rows', 200, 'display.max_columns', 100):
    print SC_Dataframe.count()

#get unique UNITID values
UNITID_VALUES = sorted(list(set([row[0] for row in SC_Dataframe.index])))

#replace all 'PrivacySuppressed' with NaN
SC_Dataframe.replace('PrivacySuppressed',np.NaN, inplace=True)

#if 'ADM_RATE' is na/nan, fill with 'ADM_RATE_ALL'
SC_Dataframe['ADM_RATE'].fillna(SC_Dataframe['ADM_RATE_ALL'], inplace=True)

#sort data by school first, then year
SC_Dataframe.sort_index(level=['UNITID','Year',], inplace=True)

#backward fill na values from future years value for a particular school
for i in UNITID_VALUES:
    for var in predictor_vars:
        SC_Dataframe[var].loc[i,].fillna(method='backfill', inplace = True)

#forward fill na values from future years value for a particular school
for i in UNITID_VALUES:
    for var in predictor_vars:
        SC_Dataframe[var].loc[i,].fillna(method='ffill', inplace = True)

#set back to sorted on year first then school
SC_Dataframe.sort_index(level=['Year','UNITID'], inplace=True)

#drop 2006 values
SC_Dataframe.drop(2006, level='Year', inplace = True)

#drop all observations that have fewer than 80 out of 115 value not NA
#might want to make this threshold higher
SC_Dataframe.dropna(axis=0, thresh=80, inplace=True)

#drop name from dataframe
SC_Dataframe_no_name = SC_Dataframe[SC_Dataframe.columns.difference(['INSTNM'])]

#impute remaining missing values
mean_imputer = Imputer(missing_values='NaN', strategy='mean', axis=0)
mean_imputer.fit(SC_Dataframe_no_name)
SC_Dataframe_imputed = pd.DataFrame(mean_imputer.transform(SC_Dataframe_no_name))
SC_Dataframe_imputed.columns = SC_Dataframe_no_name.columns
SC_Dataframe_imputed.index = SC_Dataframe_no_name.index

#save imputed dataset
import dill
dill.dump(SC_Dataframe_imputed, open('SC_Dataframe_imputed.dill', 'w'))
