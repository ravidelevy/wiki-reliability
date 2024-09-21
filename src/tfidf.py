import pandas as pd


def get_features(directory, filename):
    return pd.read_csv(f"{directory}/{filename}")

def lookup(dataframe, term):
    return dataframe.loc[dataframe["token"] == term]