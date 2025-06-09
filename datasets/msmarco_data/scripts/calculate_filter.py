import numpy as np
from graphs.recall_plot.plot_recall_method_diskann import print_images

def load_gt_file(path): 
    with open(path, 'rb') as f: 
        num_queries = np.fromfile(f, dtype=np.int32, count=1)[0]
        k = np.fromfile(f, dtype=np.int32, count=1)[0]
        ids = np.fromfile(f, dtype=np.int32, count=num_queries*k)
        ids = ids.reshape((num_queries, k))
        distances = np.fromfile(f, dtype=np.float32, count=num_queries*k)
        distances = distances.reshape((num_queries, k))
        return ids, distances

def load_result_file(path): 
    with open(path+"_idx_uint32.bin", 'rb') as f: 
        num_queries = np.fromfile(f, dtype=np.int32, count=1)[0]
        k = np.fromfile(f, dtype=np.int32, count=1)[0]
        ids = np.fromfile(f, dtype=np.int32, count=num_queries*k)
        ids = ids.reshape((num_queries, k))
    #with open(path+"_dists_float.bin", 'rb') as f: 
        #num_queries = np.fromfile(f, dtype='<i4', count=1)[0]
        #k = np.fromfile(f, dtype='<i4', count=1)[0]
        #distances = np.fromfile(f, dtype='<f4', count=num_queries*k)
        #distances = distances.reshape((num_queries, k))
        distances = []
    return ids, distances



def load_filters(path):
    with open(path) as f: 
        filters = f.read().splitlines()
        return filters

def remove_wrong_filters(ids, host_filters, query_filters): 
    for i in range(ids.shape[0]):
        current_query_filter = query_filters[i]
        for j in range(ids.shape[1]):
            if current_query_filter not in host_filters[ids[i, j]]:
                ids[i, j] = -1
    return ids

def compute_recall_k(gt_ids, result_ids, k=None): 
    if k is None: 
        k = gt_ids.shape[1]
    num_queries = gt_ids.shape[0]
    recall_scores = []
    filter_scores = []
    for i in range(num_queries): 
        gt_set = set(gt_ids[i])
        ids_row = result_ids[i]
        #retrieved = result_ids[i][:k]
        valid = ids_row[ids_row != -1][:k]
        padded = np.full(k, -1)
        padded[:len(valid)] = valid
        retrieved = padded
        match_count = len(set(retrieved) & gt_set)
        recall = match_count / k
        recall_scores.append(recall)
        filter_scores.append(len(valid)/k)
    avg_recall = np.mean(recall_scores)
    filter_recall = np.mean(filter_scores)
    return avg_recall*100, filter_recall*100

def compute_all(result_path, gt_ids, host_filters, query_filters): 
    result_ids, result_distances = load_result_file(result_path)
    result_ids = remove_wrong_filters(result_ids, host_filters, query_filters)
    avg_recall = compute_recall_k(gt_ids, result_ids)
    return avg_recall

if __name__ == "__main__": 
    result_origin = "in_memory/result"
    gt_origin = "../gt/"
    host_filters_path = "../org_files/host_labels.txt"
    query_filters_path = "../org_files/query_label"
    host_filters = load_filters(host_filters_path)
    result_origin = ["filter_in_memory/result", "in_memory/result"]
    algorithms = ["FilteredDiskANN", "DiskANN"]
    l_list = [10, 20, 30, 40, 50, 100]
    #spes = [50, 10, 5, 1, 0]
    methods = ["base", "subhost"]#, "subhost", "1_host", "2_host", "3_host"]
    queries = ["query_wh"]#, "query_wh"]
    for res_origin in range(len(result_origin)): 
        for method in methods: 
           for query in queries:
                print(f"Filter Recall for spes={method} From: {result_origin[res_origin]}/{query}/ with spes gt")
                tot_recall_list = []
                #for s in spes: 
                query_filters = load_filters(query_filters_path+"_0"+".txt")
                gt_path = f"{gt_origin}{query}/{method}_0_gt.bin"
                gt_ids, gt_distances = load_gt_file(gt_path)
                    #print(f"\nRECALL for in_memory/{query}/{method}")
                print(f"{'L':<6} {'Recall@10':<12} {'Filter recall@10':<6}") 
                print("======================================")
                recall_list = []
                for l in l_list:
                    result_path = f"{result_origin[res_origin]}/{query}/{method}/specific/result_0_{l}"
                    avg_recall, filter_recall = compute_all(result_path, gt_ids, host_filters, query_filters)
                    recall_list.append(avg_recall)
                    print(f"{l:<6} {avg_recall:<12} {filter_recall:<6}")
                    print(avg_recall)
                #tot_recall_list.append(recall_list)
                #print_images(tot_recall_list, query, method, algorithms[res_origin]) 
            
    


