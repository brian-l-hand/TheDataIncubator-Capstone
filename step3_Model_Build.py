
#This program loads the output from step2_Wrangle_Data_Target.py, and builds a model
#by using gridesearch to find hyperparameters for a Elastic Net regression model

import dill
import numpy as np
from sklearn.linear_model import ElasticNet
from sklearn.model_selection import GridSearchCV

#load data from Wrangle_Data.py output
SC = dill.load(open('SC.dill', 'r'))
y = np.log(SC['MD_EARN_WNE_P10'])
X = SC.drop(['MD_EARN_WNE_P10'], axis = 1)

#use elastic to net combine LASSO and RIDGE penalties
model = ElasticNet(normalize=True)

#first run of gridsearch, run over very wide range of values
parameters = {
    'alpha': np.logspace(-6,6,30),
    'l1_ratio': np.arange(0.0, 1.1, 0.1)
}
GSCV = GridSearchCV(model, parameters, cv = 5)
GSCV.fit(X, y)

#optimal alpha is selected from previous gridsearch for a second, more specific gridsearch
alpha_init = GSCV.cv_results_['param_alpha'][np.argmax(GSCV.cv_results_['mean_test_score'])]

parameters = {
    'alpha': np.logspace(np.log10(alpha_init)-1,np.log10(alpha_init)+1,30),
    'l1_ratio': np.arange(0.0, 1.1, 0.1)
}
GSCV = GridSearchCV(model, parameters, cv = 5)
GSCV.fit(X, y)

#final model is selected from second round of gridsearch
model_INC = GSCV.best_estimator_

#save model
dill.dump(model_INC, open('model_INC.dill', 'w'))
