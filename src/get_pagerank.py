import json
import pandas as pd
import wikipediaapi

# get title from id (based on csv, wasn't used)
contradict_features_df = pd.read_csv('../data/contradict_features.csv')
# pagerank_sorted_without_ids = pd.read_csv('../data/pageranks_sorted_without_id.csv')

wiki_wiki = wikipediaapi.Wikipedia('wiki-reliability-datascience', 'en')

def get_title_from_id(page_id):
    page = wiki_wiki.page(page_id)
    return page.title if page.exists() else None

contradict_features_df['title'] = contradict_features_df['page_id'].apply(lambda x: get_title_from_id(x))
contradict_features_df.to_csv('../data/contradict_features_with_titles.csv', index=False)

# get id and title into csv using json (was used)
json_file_path = '../pages/contradict.json'
with open(json_file_path) as file:
    data = json.load(file)

page_data = []
for item in data:
    page_id = item.get('page_id')
    title = item.get('title')
    page_data.append({'page_id': page_id, 'title': title})

df = pd.DataFrame(page_data)

output_csv_path = '../data/title_page_id.csv'
df.to_csv(output_csv_path, index=False)

# inner join between title and page id csv into title and pagerank score csv (on title as key)
title_page_id_df = pd.read_csv('../data/title_page_id.csv')
# pagerank_df = pd.read_csv('../data/pageranks_sorted_without_id.csv')

title_page_id_df['title'] = title_page_id_df['title'].str.strip().str.lower()
# pagerank_df['title'] = pagerank_df['title'].str.strip().str.lower().str.replace("_", " ")

# merged_df = pd.merge(title_page_id_df, pagerank_df, on='title', how='inner')
output_csv_path = '../pagerank/pagerank_score_features.csv'
# merged_df.to_csv(output_csv_path)
