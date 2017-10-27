
###This program takes the model built in Model_Build.py, and generates predictions,
#and caluclates the residuals

import dill
import pandas as pd
import numpy as np

#load data from Wrangle_Data.py
SC = dill.load(open('SC.dill', 'r'))
y = np.log(SC['MD_EARN_WNE_P10'])
X = SC.drop(['MD_EARN_WNE_P10'], axis = 1)

#load model from Model_Build.py
model = dill.load(open('model_INC.dill', 'r'))
model.fit(X,y)

#get R^2 value
model.score(X,y)

#examine coefficent values
coef_values = (list(zip(list(X), model.coef_)))
print coef_values

#create new dataframe, SCORES, with log residuals, residuals, actual and predicted values
SCORES = pd.DataFrame(y - model.predict(X))
SCORES.columns = ['Log_Residual']
SCORES['Residual'] = np.exp(y) - np.exp(model.predict(X))
SCORES['Actual'] = np.exp(y)
SCORES['Predicted'] = np.exp(model.predict(X))

#average over the years by UNITID (unique id for each school)
SCORES = SCORES.mean(axis=0, level='UNITID')

#sort scores by log residuals
SCORES.sort_values(['Log_Residual'], ascending=False, inplace =True)
SCORES.reset_index(inplace = True)

#load in SC_MAP data from SC_MAP.py
SC_MAP = dill.load(open('SC_MAP.dill', 'r'))

#merge with SC_MAP to get details on colleges
SCORES = SCORES.merge(SC_MAP, how = 'left', on = 'UNITID')
SCORES.set_index(['UNITID'], inplace=True)
dill.dump(SCORES, open('SCORES.dill', 'w'))
