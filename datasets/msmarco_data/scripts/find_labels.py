from struct import unpack, pack
import pandas as pd
import numpy as np
import time 
import datetime as dt


start = time.time()


lookup_dict = {}
lines = []
missing_base_ids = []
host_labels = []

col_names = ['Qid', 'Q0', 'Did', 'Rank', 'Score', 'Runstring']
df = pd.read_csv('../train/msmarco-doctrain-top100', names=col_names, sep=' ')
lookup = pd.read_csv('pos_lookup_total.csv', names=['Did', 'Pos'], sep=',')
lookup_dict = dict(zip(lookup['Did'], lookup['Pos']))
with open("ids-missing-fields-from-base.txt") as f: 
    lines = f.read().splitlines()
with open("../transformed_data/host_labels.txt") as f: 
    host_labels = f.read().splitlines()

num_of_nan = 0
missing_did = 0
not_missing_did = 0
tot_count = 0
len_list = len(lookup_dict)

print(f"Time starts: {time.time()-start}, read from file")

groundtruth_list = []
current_qid = ""
gt_count = 0
qid_count = 100
temp_list = []

for index, row in df.iterrows():
    tot_count += 1
    if type(row['Qid']) == float:
        num_of_nan += 1
        continue
    if type(row['Did']) == float:
        num_of_nan += 1
        continue
    if type(row['Rank']) == float:
        num_of_nan += 1
        continue
    
    if current_qid != row['Qid']:
        if qid_count != 100: 
            missing_base_ids.append(row['Qid'])
        qid_count = 0
        gt_count += 1
        current_qid = row['Qid']
        if temp_list: 
            groundtruth_list.append(temp_list)
        temp_list = []
    try: 
        new_id = lookup_dict[row['Did']]
        temp_list.append(new_id)
        qid_count += 1
    except:
        if row['Did'] in lines: 
            not_missing_did += 1
        missing_did += 1
    if tot_count % 1000000 == 0: 
        print(f"Data clean: {time.time()-start}, {tot_count} rows processed, {len_list-tot_count} left")

print(f"Data clean: {time.time()-start}, finished")

if qid_count != 100: 
    print("Last qid: ", current_qid, " found only ", qid_count)
groundtruth_list.append(temp_list)

check_lowest_value = 100
list_key_values = []
label_dict = {}
for batch in groundtruth_list: 
    for did in batch: 
        current_label = host_labels[did]
        if current_label in label_dict: 
            label_dict[current_label] = label_dict[current_label] + 1
        else: 
            label_dict[current_label] = 1
    temp = max(label_dict.items(), key=lambda x: x[1])
    list_key_values.append([temp[0], temp[1]])
    if temp[1] < check_lowest_value: 
        check_lowest_value = temp[1]
    label_dict={}
#print(label_dict)
print(check_lowest_value)
print("grountruth count: ", gt_count)
print("missing did: ", missing_did)
print("NOT missing did: ", not_missing_did)
print("total count: ", tot_count)
print("number of nan: ", num_of_nan)
