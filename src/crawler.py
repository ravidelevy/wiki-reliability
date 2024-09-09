from wikipedia_api import get_wikipedia_page

import os
import json
import pandas as pd

def create_dataframes(directory):
    dataframes = {}
    for filename in os.listdir(directory):
        dataframes[filename.split("_")[0]] = pd.read_csv(f"{directory}/{filename}", nrows=1000)

    return dataframes


def crawl(directory):
    dataframes = create_dataframes(directory)
    pages = {}
    for dataset in dataframes.keys():
        pages[dataset] = []
        for _, row in dataframes[dataset][:1].iterrows():
            page_id = row["page_id"]
            revision_id = row["revision_id"]
            has_template = row["has_template"]
            title, content = get_wikipedia_page(page_id, revision_id)
            pages[dataset].append({
                "page_id": page_id,
                "revision_id": revision_id,
                "title": title,
                "content": content,
                "has_template": has_template
            })
    
    return pages

def save(pages, directory):
    for page in pages.keys():
        with open(f"{directory}/{page}.json", "w") as file:
            file.write(json.dumps(pages[page]))
