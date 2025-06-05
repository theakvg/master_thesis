from utils import time_check, clean_file_content, write_to_file, transform_to_vector
import pandas as pd
import time


col_names = ['Id', 'Host', 'Title', 'Content']
prestrings = ["https://www1.", "https://www.", "https://", "http://www1.", "http://www.", "http://"]
label_file = "../transformed_data/host_labels.txt"
id_file = "docs_ids.txt"
docs_file = "../corpus/msmarco-docs.tsv"
#docs_file = "test_files/one-docs.tsv"
#docs_file = "test_files/medium-docs.tsv"
vector_file = "../transformed_data/msmarco_base_4_host.fvecs"
error_list = []
start_time = time.time()

print(f"Time start: {time_check(start_time)}")

df = pd.read_csv(docs_file, names=col_names, sep='\t')
tot_elements = len(df.index)

print(tot_elements)

sentences, ids, hosts = clean_file_content(col_names, error_list, df, prestrings, tot_elements, start_time)
del df
write_to_file(label_file, hosts, None, start_time)
write_to_file(id_file, ids, hosts, start_time)

transform_to_vector(vector_file, sentences, start_time)










