import os
import numpy as np
import pandas as pd

# todo choose one of both plot libraries
import matplotlib.pyplot as plt

def data_labels_distribution():
    counts = {0: {}, 1: {}}
    csv_paths = os.listdir('data')
    for csv_path in csv_paths:
        df = pd.read_csv(f"data/{csv_path}")
        value_counts = df["has_template"].value_counts()
        counts[0][csv_path.split('_')[0]] = value_counts[0]
        counts[1][csv_path.split('_')[0]] = value_counts[1]
    
    counts_df = pd.DataFrame(counts).fillna(0)
    counts_df.plot(kind='bar', figsize=(10, 6))
    plt.title('Number of True and False Labels for Each Unreliability Type')
    plt.xlabel('Unreliability')
    plt.ylabel('Count')
    plt.tick_params(labelsize=6)
    plt.xticks(ticks=[i for i in range(10)], labels=[csv_path.split('_')[0] for csv_path in csv_paths], rotation=0)
    plt.legend(title='has_template values')
    plt.savefig('data_labels_distribution.png')

def data_attributes_scale():
    csv_paths = os.listdir('data')
    dataframes = []
    for csv_path in csv_paths:
        df = pd.read_csv(f"data/{csv_path}")
        numerical_df = df.drop(columns=["article_quality_score"])
        for column in numerical_df.columns:
            df[column] = df[column].astype(np.float64)
        
        dataframes.append(df)
    
    unified_df = dataframes[0]
    for df in dataframes[1:]:
        unified_df = pd.merge(unified_df, df, how='outer')
    
    no_indexes_df = unified_df.drop(columns=["Unnamed: 0", "page_id", "revision_id",
                                             "revision_id.key", "has_template"])
    numerical_columms = no_indexes_df.select_dtypes(include=['number']).columns
    numerical_df = no_indexes_df[numerical_columms]
    plt.figure(figsize=(10, 6))
    numerical_df.boxplot()
    plt.yscale('log')
    plt.title('Box Plot of the Wiki Reliability Dataset attributes')
    plt.xticks(ticks=[i for i in range(1, 20)],
               labels=[column.replace("_", "\n") for column in numerical_columms], rotation=0)
    plt.tick_params(labelsize=5)
    plt.xlabel('Attribute')
    plt.ylabel('Values')
    plt.savefig('data_attributes_scale.png')

if __name__ == "__main__":
    data_labels_distribution()
    data_attributes_scale()
