import asyncio
import pandas as pd

from crawler import crawl, create_dataframes, load_jsons, save_dataframes, save_jsons
from unifier import get_features, lookup, unify_record
from xgboost_classifier import xgboost_classify

# async def main():
    # data = await crawl("data", number_of_records=1350)
    # save_jsons(data, "pages")

def unify_idf(original_data_dir, pages_dir, tfidf_dir, unified_tfidf_dir):
    """
    Unifies reliability dataset with TF-IDF dataset by integrating the 'idf' values into the records.

    Parameters:
    original_data_dir (string): The directory containing the original reliability datasets.
    pages_dir (string): The directory containing the crawled Wikipedia page data in JSON format.
    tfidf_dir (string): The directory containing the TF-IDF values for Wikipedia terms.
    unified_tfidf_dir (string): The directory where the unified TF-IDF-enhanced datasets will be saved.
    """
    # Load original reliability datasets and pages data
    wiki_reliability = create_dataframes(original_data_dir)
    pages = load_jsons(pages_dir)
    # Load TF-IDF values for Wikipedia terms
    wiki_idf = get_features(tfidf_dir, "wiki_tfidf_terms.csv")
    unified_datasets = {}
    for dataset in pages.keys():
        reliability_dataset = wiki_reliability[dataset]
        new_rows = []
        # For each page in the dataset, match it with the relevant TF-IDF record and unify the data
        for page in pages[dataset]:
            title = page["title"].lower().replace(" ", "-")
            tfidf_record = lookup(wiki_idf, title, "token")
            if tfidf_record.shape[0] > 0:
                # Unify the reliability dataset record with the TF-IDF record
                new_record = unify_record(reliability_dataset, tfidf_record, page["revision_id"], "idf")
                new_rows.append(new_record)
                
        unified_datasets[dataset] = pd.DataFrame(new_rows)
    
    save_dataframes(unified_datasets, unified_tfidf_dir)

def unify_pagerank(original_data_dir, pages_dir, pagerank_dir, unified_pagerank_dir):
    """
    Unifies reliability dataset with the PageRank dataset by integrating the 'rank' values into the records.

    Parameters:
    original_data_dir (string): The directory containing the original reliability datasets.
    pages_dir (string): The directory containing the crawled Wikipedia page data in JSON format.
    pagerank_dir (string): The directory containing the PageRank values for Wikipedia pages.
    unified_pagerank_dir (string): The directory where the unified PageRank-enhanced datasets will be saved.
    """
    # Load the original reliability datasets and the pages data
    wiki_reliability = create_dataframes(original_data_dir)
    pages = load_jsons(pages_dir)
    # Load the PageRank values for Wikipedia pages and clean the titles
    wiki_pagerank = get_features(pagerank_dir, "pageranks_sorted_without_id.csv")
    wiki_pagerank["title"] = wiki_pagerank["title"].str.strip().str.lower().str.replace("_", " ")
    unified_datasets = {}
    for dataset in pages.keys():
        reliability_dataset = wiki_reliability[dataset]
        new_rows = []
         # For each page in the dataset, match it with the relevant PageRank record and unify the data
        for page in pages[dataset]:
            title = page["title"].lower()
            pagerank_record = lookup(wiki_pagerank, title, "title")
            if pagerank_record.shape[0] > 0:
                new_record = unify_record(reliability_dataset, pagerank_record, page["revision_id"], "rank")
                new_rows.append(new_record)
        
        unified_datasets[dataset] = pd.DataFrame(new_rows)
    
    save_dataframes(unified_datasets, unified_pagerank_dir)

def main():
    # unify_idf("data", "pages", "tfidf", "tfidf_unified")
    # unify_pagerank("data", "pages", "pagerank", "pagerank_unified")
    
    # Load datasets with and without additional fields
    wiki_with_idf = create_dataframes("tfidf_unified")
    wiki_with_pagerank = create_dataframes("pagerank_unified")
    wiki_original = create_dataframes("data")
    # Run XGBoost classification on all datasets and store the results
    original_results = xgboost_classify(wiki_original)
    original_results_idf_dataset = xgboost_classify(wiki_with_idf, drop_idf=True)
    idf_results = xgboost_classify(wiki_with_idf)
    original_results_pagerank_dataset = xgboost_classify(wiki_with_pagerank, drop_pagerank=True)
    pagerank_results = xgboost_classify(wiki_with_pagerank)

    # Print all different results for each possible scenario (with\without PageRank\TF-IDF) for comparison
    print("Original:")
    print(sum([original_results[dataset] * len(wiki_original[dataset]) for dataset in wiki_original.keys()]) / 
          sum([len(wiki_original[dataset]) for dataset in wiki_original.keys()]))
    print(original_results)

    print("Original TFIDF Dataset without IDF field:")
    print(sum([original_results_idf_dataset[dataset] * len(wiki_with_idf[dataset]) for dataset in wiki_with_idf.keys()]) / 
          sum([len(wiki_with_idf[dataset]) for dataset in wiki_with_idf.keys()]))
    print(original_results_idf_dataset)

    print("TFIDF:")
    print(sum([idf_results[dataset] * len(wiki_with_idf[dataset]) for dataset in wiki_with_idf.keys()]) / 
          sum([len(wiki_with_idf[dataset]) for dataset in wiki_with_idf.keys()]))
    print(idf_results)

    print("Original PageRank Dataset without rank field:")
    print(sum([original_results_pagerank_dataset[dataset] * len(wiki_with_pagerank[dataset]) for dataset in wiki_with_pagerank.keys()]) / 
          sum([len(wiki_with_pagerank[dataset]) for dataset in wiki_with_pagerank.keys()]))
    print(original_results_pagerank_dataset)

    print("PageRank:")
    print(sum([pagerank_results[dataset] * len(wiki_with_pagerank[dataset]) for dataset in wiki_with_pagerank.keys()]) / 
          sum([len(wiki_with_pagerank[dataset]) for dataset in wiki_with_pagerank.keys()]))
    print(pagerank_results)

if __name__ == '__main__':
    # asyncio.run(main())
    main()
