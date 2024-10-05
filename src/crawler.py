from wikipedia_api import get_wikipedia_page

import os
import json
import random
import asyncio
import pandas as pd
from pathlib import Path

def create_dataframes(directory):
    dataframes = {}
    for filename in os.listdir(directory):
        dataframes[filename.split("_")[0].split(".")[0]] = pd.read_csv(f"{directory}/{filename}")
    
    return dataframes

def save_dataframes(dataframes, directory):
    for dataframe in dataframes.keys():
        dataframes[dataframe].drop('Unnamed: 0', axis=1).to_csv(f"{directory}/{dataframe}.csv")

async def process_row(pages, index, row):
    page_id = row["page_id"]
    revision_id = row["revision_id"]
    has_template = row["has_template"]
    await asyncio.sleep(1)
    page = await get_wikipedia_page(page_id, revision_id)
    if len(page) != 2:
        return
    
    title, content = page
    pages.append({
        "page_id": page_id,
        "revision_id": revision_id,
        "title": title,
        "content": content,
        "has_template": has_template
    })

async def crawl_from_samples(samples):
    pages = []
    tasks = []
    for index, row in samples.iterrows():
        tasks.append(process_row(pages, index, row))
    await asyncio.gather(*tasks)
    
    return pages

async def crawl(directory, number_of_records=1):
    dataframes = create_dataframes(directory)
    pages = {}
    random.seed(42)
    for dataset in dataframes.keys():
        pages[dataset] = []
        dataframe = dataframes[dataset]
        negative = dataframe.loc[dataframe["has_template"] == 0]
        positive = dataframe.loc[dataframe["has_template"] == 1]
        negative_samples = await crawl_from_samples(negative.sample(n=number_of_records, random_state=1))
        positive_samples = await crawl_from_samples(positive.sample(n=number_of_records, random_state=1))
        samples = negative_samples + positive_samples
        random.shuffle(samples)
        pages[dataset] = samples
    
    return pages

def save_jsons(pages, directory):
    for page in pages.keys():
        with open(f"{directory}/{page}.json", "w") as file:
            file.write(json.dumps(pages[page]))

def load_jsons(directory):
    path = Path(directory)
    pages = {}
    for filename in path.iterdir():
        dataset = str(filename).split(".")[0].split("\\")[1]
        with open(filename, "r") as file:
            pages[dataset] = json.load(file)
    
    return pages
