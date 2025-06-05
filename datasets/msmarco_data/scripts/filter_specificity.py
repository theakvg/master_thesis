import csv

query_filter_file = "../transformed_data/query_label.txt"
docs_filter_file = "../transformed_data/host_labels.txt"
dump_file = "how_many_nodes_with_filter.csv"
q_dump_file = "how_many_queries_have_x_relevant_nodes.csv"
how_many_nodes_with_filter = {}
how_many_queries_have_x_relevant_nodes = {}

with open(query_filter_file) as f: 
    qfilter = f.read().splitlines()

with open(docs_filter_file) as f: 
    dfilter = f.read().replace(",", "\n").split()

for key in qfilter: 
    c = 0
    if key not in how_many_nodes_with_filter.keys():
        c = dfilter.count(key)
        how_many_nodes_with_filter[key] = c
    else: 
        c = how_many_nodes_with_filter[key]
    try:
        how_many_queries_have_x_relevant_nodes[c] += 1
    except: 
        how_many_queries_have_x_relevant_nodes[c] = 1

with open(dump_file, "w") as f: 
    writer = csv.writer(f)
    writer.writerow(["queries", "number of nodes"])
    for key, value in how_many_nodes_with_filter.items(): 
        writer.writerow([key, value])

with open(q_dump_file, "w") as f: 
    writer = csv.writer(f)
    writer.writerow(["number of nodes", "number of queries"])
    for key, value in how_many_queries_have_x_relevant_nodes.items():
        writer.writerow([key, value])

