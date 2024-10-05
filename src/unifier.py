import pandas as pd


def get_features(directory, filename):
    return pd.read_csv(f"{directory}/{filename}")

def lookup(dataframe, term, key):
    return dataframe[dataframe[key] == term]

def unify_record(reliability_dataset, record, revision_id, value):
    reliability_record = reliability_dataset[reliability_dataset["revision_id"] == revision_id]
    new_record = reliability_record.to_dict(orient="records")[0]
    new_record[value] = record.to_dict(orient="records")[0][value]
    return new_record
