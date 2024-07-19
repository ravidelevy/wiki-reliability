import pandas as pd

# todo choose one of both plot libraries
import matplotlib.pyplot as plt
import seaborn as sns

csv_paths = ["contradict_features.csv", "disputed_features.csv", "hoax_features.csv", "more-citations-needed_features.csv", "one-source_features.csv", "original-research_features.csv", "pov_features.csv", "third-party_features.csv", "unreferenced_features.csv", "unreliable-sources_features.csv"]

def visualisation():
    for csv_path in csv_paths:
        df = pd.read_csv("../../" + csv_path)
        print(df["has_template"].value_counts())
        # todo create graphs based on output

if __name__ == "__main__":
    visualisation()