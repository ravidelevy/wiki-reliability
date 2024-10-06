from wikipedia_api import get_wikipedia_page

import os
import json
import random
import asyncio
import pandas as pd
from pathlib import Path

def create_dataframes(directory):
    """
    Reads all CSV files from given directory and creates a dictionary of DataFrames.
    Params:
    directory (string): name of directory containing all csv files
    
    Returns:
    A dictionary with keys as dataset names, derived from file names and values as DataFrames
    """
    dataframes = {}
    for filename in os.listdir(directory):
        dataframes[filename.split("_")[0].split(".")[0]] = pd.read_csv(f"{directory}/{filename}")
    
    return dataframes

def save_dataframes(dataframes, directory):
    """
    Save each DataFrame in the dataframes directory as a CSV file in given directory

    Params:
    dataframes (dictionary): A dictionary of DataFrames to be saved.
    directory (string): The directory where CSV files will be stored.
    """
    for dataframe in dataframes.keys():
        dataframes[dataframe].drop('Unnamed: 0', axis=1).to_csv(f"{directory}/{dataframe}.csv")

async def process_row(pages, index, row):
    """
    Fetches a Wikipedia page based on the page_id and revision_id from a row and adds the page data to a list.

    Params:
    pages (list): A list to store the page data as dictionaries.
    index (int): The index of the current row being processed.
    row (pd.Series): A row from the DataFrame containing page_id, revision_id, and has_template.
    """
    page_id = row["page_id"]
    revision_id = row["revision_id"]
    has_template = row["has_template"]
    # Delaying to avoid overloading the Wikipedia API
    await asyncio.sleep(1)
    # Fetch page details using Wikipedia API
    page = await get_wikipedia_page(page_id, revision_id)
    # If response does not contain expected data, do nothing
    if len(page) != 2:
        return
    # Extract title and content and add to pages list
    title, content = page
    pages.append({
        "page_id": page_id,
        "revision_id": revision_id,
        "title": title,
        "content": content,
        "has_template": has_template
    })

async def crawl_from_samples(samples):
    """
    Processes each row of the samples DataFrame asynchronously to gather Wikipedia page data.

    Params:
    samples (pd.DataFrame): A DataFrame containing rows to process.

    Returns:
    A list of dictionaries containing page information.
    """
    pages = []
    tasks = []
    for index, row in samples.iterrows():
        tasks.append(process_row(pages, index, row))
    await asyncio.gather(*tasks)
    
    return pages

async def crawl(directory, number_of_records=1):
    """
    Main crawl function which processes datasets from CSV files in the directory, samples positive and negative records, 
    and retrieves corresponding Wikipedia page data.

    Params:
    directory (str): The directory containing CSV files.
    number_of_records (int): The number of positive and negative records to sample from each dataset (default: 1).

    Returns:
    A dictionary where keys are dataset names and values are lists of Wikipedia page data.
    """
    # Load dataframes from the directory
    dataframes = create_dataframes(directory)
    pages = {}
    # Seed for random shuffling
    random.seed(42)
    # Process for each dataset
    for dataset in dataframes.keys():
        pages[dataset] = []
        dataframe = dataframes[dataset]
        # Split dataset according to positive and negative results
        negative = dataframe.loc[dataframe["has_template"] == 0]
        positive = dataframe.loc[dataframe["has_template"] == 1]
        # From each part - take n specified amount of records for sample
        negative_samples = await crawl_from_samples(negative.sample(n=number_of_records, random_state=1))
        positive_samples = await crawl_from_samples(positive.sample(n=number_of_records, random_state=1))
        # Unify both taken negative and positive results and shuffle them randomally
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
