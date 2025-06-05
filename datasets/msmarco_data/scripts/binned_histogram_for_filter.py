import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Load your CSV
df = pd.read_csv("how_many_queries_have_x_relevant_nodes.csv")

# Create bins: adjust bin edges as needed
bin_edges = [1, 10, 100, 1000, 10000, 100000]
bin_labels = ["1–10", "11–100", "101–1k", "1k–10k", "10k–100k"]

# Bin the 'count' values
df['bin'] = pd.cut(df['number of nodes'], bins=bin_edges, labels=bin_labels, right=True, include_lowest=True)

# Count how many filters fall into each bin
bin_counts = df['bin'].value_counts().sort_index()

# Sum the number of filters in each bin
#filters_per_bin = df.groupby('bin')['number of queries'].sum()

# Plot with Matplotlib
plt.figure(figsize=(8, 5))
plt.bar(bin_counts.index, bin_counts.values, color='skyblue')

# Adding labels and title
plt.xlabel("Number of Datapoints per Filter (Binned)")
plt.ylabel("Number of Filters in Bin")
plt.title("Binned Distribution of Filter Sizes")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("binned_distribution_plot.png", dpi=300)

