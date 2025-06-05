list_q = []
list_gt = []

with open("query_id_list.txt") as f: 
    list_q = f.read().splitlines()

with open("query_id_list_2.txt") as f: 
    list_gt = f.read().splitlines()

for i in list_q: 
    if i not in list_gt: 
        print("Missing: ", i)
