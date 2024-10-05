import asyncio
import pandas as pd

from crawler import crawl, create_dataframes, load_jsons, save_dataframes, save_jsons
from unifier import get_features, lookup, unify_record
from xgboost_classifier import xgboost_classify

# async def main():
    # data = await crawl("data", number_of_records=1350)
    # save_jsons(data, "pages")

def unify_idf(original_data_dir, pages_dir, tfidf_dir, unified_tfidf_dir):
    wiki_reliability = create_dataframes(original_data_dir)
    pages = load_jsons(pages_dir)
    wiki_idf = get_features(tfidf_dir, "wiki_tfidf_terms.csv")
    unified_datasets = {}
    for dataset in pages.keys():
        reliability_dataset = wiki_reliability[dataset]
        new_rows = []
        for page in pages[dataset]:
            title = page["title"].lower().replace(" ", "-")
            tfidf_record = lookup(wiki_idf, title, "token")
            if tfidf_record.shape[0] > 0:
                new_record = unify_record(reliability_dataset, tfidf_record, page["revision_id"], "idf")
                new_rows.append(new_record)
        
        unified_datasets[dataset] = pd.DataFrame(new_rows)
    
    save_dataframes(unified_datasets, unified_tfidf_dir)

def unify_pagerank(original_data_dir, pages_dir, pagerank_dir, unified_pagerank_dir):
    wiki_reliability = create_dataframes(original_data_dir)
    pages = load_jsons(pages_dir)
    wiki_pagerank = get_features(pagerank_dir, "pageranks_sorted_without_id.csv")
    wiki_pagerank["title"] = wiki_pagerank["title"].str.strip().str.lower().str.replace("_", " ")
    unified_datasets = {}
    for dataset in pages.keys():
        reliability_dataset = wiki_reliability[dataset]
        new_rows = []
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
    unify_pagerank("data", "pages", "pagerank", "pagerank_unified")
    # wiki_with_idf = create_dataframes("tfidf_unified")
    # wiki_with_pagerank = create_dataframes("pagerank_unified")
    # wiki_original = create_dataframes("data")
    # results = xgboost_classify(wiki_original)
    # print(sum([results[dataset] * len(wiki_original[dataset]) for dataset in wiki_original.keys()]) / 
    #       sum([len(wiki_original[dataset]) for dataset in wiki_original.keys()]))
    # print(results)

if __name__ == '__main__':
    # asyncio.run(main())
    main()
