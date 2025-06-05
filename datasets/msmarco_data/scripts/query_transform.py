from sentence_transformers import SentenceTransformer
from utils import time_check, clean_file_content, write_to_file, transform_to_vector, divide_into_percent
import time
import datetime as dt
import pandas as pd
import numpy as np

query_list = []
error_list = []
col_names = ['Id', 'Query']
percents = [50, 10, 5, 1, 0]
query_file = '../train/msmarco-doctrain-queries.tsv'
#query_file = 'test_files/10k_queries.tsv'
vector_file = "../transformed_data/queries_woh.fvecs"
host_file = "../transformed_data/query_label.txt"
docs_label_file = "../transformed_data/host_labels.txt"
label_file = "query_ids.txt"
label_error_file = "error_list_write_query_label.txt" 
start_time = time.time()


print(f"Time start: {time_check(start_time)}")

df = pd.read_csv(query_file, names=col_names, sep='\t')
print(len(df.index))

with open(label_error_file) as f: 
    error_list = f.read().splitlines()
error_list.append(502557)
for i in error_list: 
    df = df.loc[df["Id"] != int(i)]

tot_elements = len(df.index)
print(tot_elements)

with open(host_file) as f:
    host_list = f.read().splitlines()
df["Hosts"] = host_list

print(df)

queries, ids, labels = clean_file_content(col_names, error_list, df, None, tot_elements, start_time)
del df


q_labeldict = divide_into_percent(host_file, docs_label_file, percents)
divided_labels = [[], [], [], [], []]
divided_sentences = [[], [], [], [], []]
for l in range(0, len(labels)): 
    perc = q_labeldict[labels[l]]
    divided_labels[perc].append(labels[l])
    divided_sentences[perc].append(queries[l])

for l_list in range(0, len(divided_labels)): 
    print(f"Length l_list: {len(divided_labels[l_list])}")
    write_to_file(f"../transformed_data/query_label_{percents[l_list]}.txt", divided_labels[l_list], None, start_time)

for s_list in range(0, len(divided_sentences)): 
    print(f"Length s_list: {len(divided_sentences[s_list])}")
    transform_to_vector(f"../transformed_data/queries_{percents[s_list]}_woh.fvecs", divided_sentences[s_list], start_time)

