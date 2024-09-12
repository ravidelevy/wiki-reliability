from wikipedia_api import get_wikipedia_page

import os
import json
import random
import pandas as pd

def create_dataframes(directory):
    dataframes = {}
    for filename in os.listdir(directory):
        dataframes[filename.split("_")[0]] = pd.read_csv(f"{directory}/{filename}")
    
    return dataframes

def crawl_from_samples(samples):
    pages = []
    for _, row in samples.iterrows():
        page_id = row["page_id"]
        revision_id = row["revision_id"]
        has_template = row["has_template"]
        page = get_wikipedia_page(page_id, revision_id)
        if len(page) != 2:
            continue
        
        title, content = page
        pages.append({
            "page_id": page_id,
            "revision_id": revision_id,
            "title": title,
            "content": content,
            "has_template": has_template
        })
    
    return pages

def crawl(directory, number_of_records=1):
    dataframes = create_dataframes(directory)
    pages = {}
    random.seed(42)
    for dataset in dataframes.keys():
        pages[dataset] = []
        dataframe = dataframes[dataset]
        negative = dataframe.loc[dataframe["has_template"] == 0]
        positive = dataframe.loc[dataframe["has_template"] == 1]
        negative_samples = crawl_from_samples(negative.sample(n=number_of_records, random_state=1))
        positive_samples = crawl_from_samples(positive.sample(n=number_of_records, random_state=1))
        samples = negative_samples + positive_samples
        random.shuffle(samples)
        pages[dataset] = samples
    
    return pages

def save(pages, directory):
    for page in pages.keys():
        with open(f"{directory}/{page}.json", "w") as file:
            file.write(json.dumps(pages[page]))
