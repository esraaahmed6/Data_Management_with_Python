import sqlalchemy as db
import pandas as pd
from sklearn import model_selection
import numpy as np
from sklearn import svm

# create connection with the database
con = db.create_engine('postgresql://postgres:123@localhost/postgres')

# Create a SQL query to load the entire iris table
query = """
SELECT *
FROM iris
"""
    
# Load to dataframe
df = pd.read_sql(query, con)
df_desc = df.describe()
    
iris_features = ['sepal-length', 'sepal-width', 'petal-length', 'petal-width' ,"species"]
# Extract features
X = df.loc[:, iris_features].values
# Extract target i.e. iris species
Y = df.loc[:, ['target']].values

# Now using scikit-learn model_selection module, split the iris data into train/test data sets
# keeping 40% reserved for testing purpose and 60% data will be used to train and form model.

X_train, X_test, Y_train, Y_test = model_selection.train_test_split(X, Y, test_size=0.4, random_state=0)

# Build an SVC (Support Vector Classification) model using linear regression
clf_ob = svm.SVC(kernel='linear', C=1).fit(X_train, Y_train)
print(clf_ob.score(X_test, Y_test))
scores_res = model_selection.cross_val_score(clf_ob, X, Y, cv=5)

# Print the accuracy of each fold (i.e. 5 as above we asked cv 5)
print(scores_res)

# And the mean accuracy of all 5 folds.
print(scores_res.mean())

data_prediction = [[4.9876, 3.348, 1.8488, 0.2], [5.3654, 2.0853, 3.4675, 1.1222], [5.890, 3.33, 5.134, 1.6]]

p_res = clf_ob.predict(data_prediction)

print('Given first iris is of type:', p_res[0])
print('Given second iris is of type:', p_res[1])
print('Given third iris is of type:', p_res[2])


def __init__(self, C=1.0, solver='liblinear', max_iter=100, X_train=None, Y_train=None):
        LogisticRegression.__init__(self, C=C, solver=solver, max_iter=max_iter)
        self.X_train = X_train
        self.Y_train = Y_train
        
# A method for saving object data to JSON file
def save_json(self, filepath):
    dict_ = {}
    dict_['C'] = self.C
    dict_['max_iter'] = self.max_iter
    dict_['solver'] = self.solver
    dict_['X_train'] = self.X_train.tolist() if self.X_train is not None else 'None'
    dict_['Y_train'] = self.Y_train.tolist() if self.Y_train is not None else 'None'
        
    # Creat json and save to file
    json_txt = json.dumps(dict_, indent=4)
    with open(filepath, 'w') as file:
         file.write(json_txt)
    
# A method for loading data from JSON file
def load_json(self, filepath):
    with open(filepath, 'r') as file:
        dict_ = json.load(file)           
    self.C = dict_['C']
    self.max_iter = dict_['max_iter']
    self.solver = dict_['solver']
    self.X_train = np.asarray(dict_['X_train']) if dict_['X_train'] != 'None' else None
    self.Y_train = np.asarray(dict_['Y_train']) if dict_['Y_train'] != 'None' else None
filepath = "mylogreg.json"

# Create a model and train it
mylogreg = MyLogReg(X_train=Xtrain, Y_train=Ytrain)
mylogreg.save_json(filepath)

# Create a new object and load its data from JSON file
json_mylogreg = MyLogReg()
json_mylogreg.load_json(filepath)
json_mylogreg
