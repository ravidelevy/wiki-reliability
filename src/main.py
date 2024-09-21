import asyncio
import pandas as pd

from tfidf import get_features, lookup, unify_record
from crawler import crawl, create_dataframes, load_jsons, save_dataframes, save_jsons
from xgboost_classify import xgboost_classify

# async def main():
    # data = await crawl("data", number_of_records=1350)
    # save_jsons(data, "pages")

def unify_idf(original_data_dir, pages_dir, tfidf_dir, unified_idf_dir):
    wiki_reliability = create_dataframes(original_data_dir)
    pages = load_jsons(pages_dir)
    wiki_idf = get_features(tfidf_dir, "wiki_tfidf_terms.csv")
    unified_datasets = {}
    for dataset in pages.keys():
        reliability_dataset = wiki_reliability[dataset]
        new_rows = []
        for page in pages[dataset]:
            title = page["title"].lower().replace(" ", "-")
            idf_record = lookup(wiki_idf, title)
            if idf_record.shape[0] > 0:
                new_record = unify_record(reliability_dataset, idf_record, page["revision_id"])
                new_rows.append(new_record)
        
        unified_datasets[dataset] = pd.DataFrame(new_rows)
    
    save_dataframes(unified_datasets, unified_idf_dir)

def main():
    #unify_idf("data", "pages", "tfidf", "idf_unified")
    wiki_with_idf = create_dataframes("idf_unified")
    results = xgboost_classify(wiki_with_idf)
    print(sum([results[dataset] * len(wiki_with_idf[dataset]) for dataset in wiki_with_idf.keys()]) / 
          sum([len(wiki_with_idf[dataset]) for dataset in wiki_with_idf.keys()]))
    print(results)

if __name__ == '__main__':
    # asyncio.run(main())
    main()
