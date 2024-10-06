from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split

from crawler import create_dataframes


def xgboost_classify(dataframes, drop_idf=False, drop_pagerank=False):
   results = {}

   for dataframe in dataframes.keys():
      X = dataframes[dataframe].loc[:, "revision_text_bytes":]
      X["article_quality_score"].replace(['B', 'Start', 'C', 'FA', 'Stub', 'GA'],
                                       [0, 1, 2, 3, 4, 5], inplace=True) # replace the string to numeric categories
      if drop_idf:
         X = X.drop("idf", axis=1)
      
      if drop_pagerank:
         X = X.drop("rank", axis=1)
      
      y = dataframes[dataframe]["has_template"]
      y = y.iloc[:] == 1.0
      X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=1)

      bst = XGBClassifier(n_estimators=2, max_depth=2, learning_rate=1, objective='binary:logistic')
      # fit model
      bst.fit(X_train, y_train)
      # make predictions
      preds = bst.predict(X_test)

      results[dataframe] = sum((preds == y_test).astype(int).iloc[:]) / y_test.shape[0]
   
   return results
