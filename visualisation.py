import os
import pandas as pd

# todo choose one of both plot libraries
import matplotlib.pyplot as plt
import seaborn as sns

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
    plt.xlabel('Unreliablity')
    plt.ylabel('Count')
    plt.tick_params(labelsize=6)
    plt.xticks(ticks=[i for i in range(10)], labels=[csv_path.split('_')[0] for csv_path in csv_paths], rotation=0)
    plt.legend(title='has_template values')
    plt.savefig('data_labels_distribution.png')

if __name__ == "__main__":
    data_labels_distribution()

