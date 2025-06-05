import time 
import pandas as pd
import math
from utils import check_if_df_elements_are_correct_type, time_check, handle_find_label, write_to_file
from collections import defaultdict

col_names = ['Qid', 'Q0', 'Did', 'Rank', 'Score', 'Runstring']
label_list = []
error_list = []
rank_list = []
query_ids = []
docs_ids = {}
query_col_names = ['Id', 'Query']
query_file = '../train/msmarco-doctrain-queries.tsv'
#query_file = 'test_files/10k_queries.tsv'
docs_id_file = "docs_ids.txt"
query_label_file = "../transformed_data/query_label.txt"
#query_label_file = "rm/ql.txt"
error_file = "error_list_write_query_label.txt"
rank_file = "../train/msmarco-doctrain-top100"
#rank_file = "test_files/10k_top100"



start_time = time.time()
print(f"Time start: {time_check(start_time)}")

query_df = pd.read_csv(query_file, names=query_col_names, sep='\t')
query_ids = query_df['Id'].tolist()
del query_df
df = pd.read_csv(rank_file, names=col_names, sep=' ')

print(f"Finished read from df file: time lapsed {time_check(start_time)}")
tot_elements = len(df.index)


count = 0
q_dict = defaultdict(list)
    
jump = math.ceil(tot_elements/10)
for index, row in df.iterrows(): 
    check_elements = check_if_df_elements_are_correct_type(['Qid', 'Did'], error_list, row)
    if not check_elements: 
        continue 
    q_dict[row['Qid']].append(row['Did'])
    count += 1
    if count % jump == 0:
        print(f"Clean content: time lapsed {time_check(start_time)}, elements processed {math.ceil((count/tot_elements)*100)} %")


del df


print(f"Start read from files: time lapsed {time_check(start_time)}")
with open(docs_id_file) as f: 
    for line in f: 
        splitted = line.split(',')
        docs_ids[splitted[0]] = splitted[1]
print(f"Finish read from file: time lapsed {time_check(start_time)}")

if len(query_ids) != len(q_dict):
    print(f"WARNING: query_ids and q-dict are of different length: query_ids ({len(query_ids)}), q_dict({len(q_dict)})")


count = 0
q_error_list = []
test_docs = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0, 8: 0, 9: 0}
tot_elements = len(query_ids)
jump = math.ceil(tot_elements/10)
for q_id in query_ids: 
    q_label = handle_find_label(q_dict[int(q_id)], docs_ids, test_docs)
    if q_label == False: 
        q_error_list.append(q_id)
    else: 
        label_list.append(q_label)
    count += 1
    if count % jump == 0: 
        print(f"Find label: time lapsed {time_check(start_time)}, elements processed {math.ceil((count/tot_elements)*100)} %")

if q_error_list: 
    #print("These ids did not get labels for some reason: ", q_error_list)
    print("q_error_list was printed")


write_to_file(query_label_file, label_list, None, start_time)


write_to_file(error_file, q_error_list, None, start_time)


print(test_docs)
