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
qid_list = []

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
len_list = len(df)
k = 1

print(f"Time starts: {time.time()-start}, read from file")

groundtruth_list = []
current_qid = ""
gt_count = 0
qid_count = 100
temp_list = []
query_id_list = []

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
    if row['Qid'] == '502557': 
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

print(f"Binary: {time.time()-start}, start write to file")
#groundtruth_list = np.array(groundtruth_list).astype(np.int32)

new_label_list = []
print("number of elements in gt_list: ", len(groundtruth_list))
for i in groundtruth_list: 
    new_label_list.append(host_labels[i[0]])

with open("../transformed_data/query_labels.txt", "w") as f: 
    f.write(f"{len(new_label_list)} 1\n")
    for i in new_label_list:
        f.write(i + "\n")

f = open("../transformed_data/msmarco_groundtruth.ivecs", "wb")
f.truncate(0)
len_gtlist = len(groundtruth_list)
for vector in range(0, len_gtlist): 
    #dim = np.int32(len(groundtruth_list[vector]))
    dim = np.int32(1)
    f.write(dim.tobytes())
    f.write(np.array(groundtruth_list[vector][0]).astype(np.int32).tobytes())
    if vector % 1000000 == 0: 
        print(f"Binary: {time.time()-start}, {vector} rows processed, {len_gtlist-vector}")

print("grountruth count: ", gt_count)
print("missing did: ", missing_did)
print("NOT missing did: ", not_missing_did)
print("total count: ", tot_count)
print("number of nan: ", num_of_nan)
