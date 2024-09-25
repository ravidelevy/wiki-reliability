import json
import pandas as pd
import wikipediaapi

# Initialize Wikipedia API
wiki_wiki = wikipediaapi.Wikipedia('wiki-reliability-datascience', 'en')

# Function to fetch title from Wikipedia API by page_id (is not used in pagerank data fetching)
def get_title_from_id(page_id):
    page = wiki_wiki.page(page_id)
    return page.title if page.exists() else None

# Function to add titles to a dataframe based on page_id
def add_titles_to_dataframe(input_csv, output_csv):
    df = pd.read_csv(input_csv)
    df['title'] = df['page_id'].apply(lambda x: get_title_from_id(x))
    df.to_csv(output_csv, index=False)
    print(f"Updated CSV with titles saved to {output_csv}")

# Function to extract page_id and title from a JSON file and save to a CSV (this is used for pagerank data fetching)
def extract_ids_and_titles_from_json(json_file_path, output_csv_path):
    with open(json_file_path) as file:
        data = json.load(file)

    page_data = [{'page_id': item.get('page_id'), 'title': item.get('title')} for item in data]
    df = pd.DataFrame(page_data)
    df.to_csv(output_csv_path, index=False)
    print(f"Extracted data saved to {output_csv_path}")

# Function to merge two dataframes on title
def merge_titles_with_pagerank(title_page_id_csv, pagerank_csv, output_csv):
    title_page_id_df = pd.read_csv(title_page_id_csv)
    pagerank_df = pd.read_csv(pagerank_csv)

    # Standardize title column for merging
    title_page_id_df['title'] = title_page_id_df['title'].str.strip().str.lower()
    pagerank_df['title'] = pagerank_df['title'].str.strip().str.lower().str.replace("_", " ")

    merged_df = pd.merge(title_page_id_df, pagerank_df, on='title', how='inner')
    merged_df.to_csv(output_csv, index=False)
    print(f"Merged data saved to {output_csv}")

def main():
    # Add titles to contradict_features CSV - this part is not used as part of gathering pagerank data
    contradict_features_csv = '../data/contradict_features.csv'
    contradict_features_with_titles_csv = '../data/contradict_features_with_titles.csv'
    add_titles_to_dataframe(contradict_features_csv, contradict_features_with_titles_csv)

    # Extract page_id and title from JSON and save as CSV
    json_file_path = '../pages/contradict.json'
    title_page_id_csv = '../data/title_page_id.csv'
    extract_ids_and_titles_from_json(json_file_path, title_page_id_csv)

    # Merge title and pagerank score data
    pagerank_csv = '../data/pageranks_sorted_without_id.csv'
    output_csv = '../pagerank/pagerank_score_features.csv'
    merge_titles_with_pagerank(title_page_id_csv, pagerank_csv, output_csv)

if __name__ == '__main__':
    main()
