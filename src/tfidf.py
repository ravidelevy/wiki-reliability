import pandas as pd


def get_features(directory, filename):
    return pd.read_csv(f"{directory}/{filename}")

def lookup(dataframe, term):
    return dataframe[dataframe["token"] == term]

def unify_record(reliability_dataset, idf_record, revision_id):
    reliability_record = reliability_dataset[reliability_dataset["revision_id"] == revision_id]
    new_record = reliability_record.to_dict(orient="records")[0]
    idf_value = idf_record.to_dict(orient="records")[0]["idf"]
    new_record["idf"] = idf_value
    return new_record
