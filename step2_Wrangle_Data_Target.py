
#this program processes the predictor variables data, and merges it with the
#indepent variables to create to final output dataset SC. This is used as input
#to step3_Model_Build.py

import dill
import sqlite3
import numpy as np
import pandas

#connect to SQL database
connection = sqlite3.connect("/home/vagrant/datacourse/Capstone/Data/database.sqlite")
cursor = connection.cursor()

#specify dependent variable information
dep_vars = [u'UNITID', u'MD_EARN_WNE_P10', u'Year']

#specify SQL query
sql = "SELECT " + ",".join(dep_vars) + " FROM Scorecard WHERE (Year BETWEEN 2011 AND 2013);"

#load into database
SC_Dep = pd.read_sql_query(sql, sqlite3.connect("/home/vagrant/datacourse/Capstone/Data/database.sqlite"))

#replace 'privacy suppressed' with nan, drop nan values
SC_Dep.replace('PrivacySuppressed',np.NaN, inplace=True)
SC_Dep.dropna(inplace = True)

#subtract 10 from year to prepare to merge with predictor vars
SC_Dep['Year'] = SC_Dep['Year'] - 10
SC_Dep.set_index(['UNITID','Year'], inplace=True)


#load predictor variables
SC_Ind = dill.load(open('SC_Dataframe_imputed.dill', 'r'))

#drop year not used
SC_Ind.drop(2005, level='Year', inplace = True)
SC_Ind.drop(2004, level='Year', inplace = True)

#sort ind and dep before combining
SC_Ind.sort_index(level=['Year', 'UNITID'], inplace=True)
SC_Dep.sort_index(level=['Year', 'UNITID'], inplace=True)

#combine ind and dep variables to create SC dataframe
SC = pd.concat([SC_Ind, SC_Dep], axis=1)

#drop rows with nan values
SC.dropna(inplace=True)

#create year value variable for use as a predictor variable,
#and turn it into a dummy variable,
# and join with SC dataframe
SC['Year_Value'] = SC.index.get_level_values('Year')
dummies = pd.get_dummies(SC['Year_Value'], prefix='Year', drop_first=True)
SC = pd.concat([SC, dummies], axis=1)

#drop Year_Value after dummies are joined
SC.drop(['Year_Value'], axis = 1, inplace = True)

#save final SC dataset
dill.dump(SC, open('SC.dill', 'w'))
