import pandas as pd
col_names = ['Id', 'Host', 'Query', 'Content']
df = pd.read_csv('test-docs.tsv', names=col_names, sep='\t')

prestring = ["https://www1.", "https://www.", "https://", "http://www1.", "http://www.", "http://"] 

host_dict = {}
count = 3 

for index, row in df.iterrows():
    if type(row['Host']) == float:
        num_of_nan += 1
        continue
    wc = row['Host']
    check = False
    for ps in prestring: 
        if ps in wc: 
            check = True
            wc = wc.split(ps)[1]
    if not check: 
        print(wc)
    res = wc.split('/')[0]
    if count < res.count('.'): 
        count = res.count('.')
        print(res)
    if res in host_dict: 
        host_dict[res] = host_dict[res]+1
    else: 
        host_dict[res] = 1

#print(host_dict)
print("number of . : ", count)
print("highest occurrence: ", max(host_dict.values()))
print("number of hosts: ", len(host_dict.keys()))
