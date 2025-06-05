import math 
from sentence_transformers import SentenceTransformer
import numpy as np
import time
import datetime


def check_if_df_elements_are_correct_type(name_list, error_list, row): 
    for name in name_list: 
        if type(row[name]) == float: 
            error_list.append(row['Id']) 
            return False
    return True

def prepare_host(host, prestrings):
    check = False
    for ps in prestrings: 
        if ps in host: 
            host = host.split(ps)[1]
            check = True
            break
    if not check: 
        print("Found no prestring for following host: ", host)
    
    host = host.split('/')[0]
    host = host.split(':')[0]
    num_dot = host.count('.')
    host_res = host
    for i in range(0, num_dot): 
        host = host[host.index(".")+1:]
        host_res += ","+host
    return host_res


    
def time_check(start_time):
    sec = time.time() - start_time
    td = datetime.datetime.utcfromtimestamp(sec)
    return td.strftime('%H:%M:%S.%f')[:-4]

def clean_file_content(name_list, error_list, df, prestrings, tot_elements, start_time): 
    sentences = []
    labels = []
    ids = []
    count = 0
    for index, row in df.iterrows(): 
        check_elements = check_if_df_elements_are_correct_type(name_list, error_list, row)
        if not check_elements:
            continue
        if prestrings is not None: 
            host = prepare_host(row['Host'], prestrings)
            if host == False: 
                continue
            labels.append(host)
            #sentence = row['Title'][:100]+row['Content'][:2000]  
            #sentence = host.replace(",", " ")+" "+row['Title'][:100]+row['Content'][:2000]  
            part_host = host.split(',')[0]
            #sentence = part_host+" "+row['Title'][:100]+row['Content'][:2000]
            #sentence = part_host+" "+part_host+" "+row['Title'][:100]+row['Content'][:2000] 
            #sentence = part_host+" "+part_host+" "+part_host+" "+row['Title'][:100]+row['Content'][:2000] 
            sentence = part_host+" "+part_host+" "+part_host+" "+part_host+" "+row['Title'][:100]+row['Content'][:2000] 
        else: 
            #sentence = row['Hosts'] + row['Query']
            sentence = row['Query']
            labels.append(row['Hosts'])
        ids.append(row['Id'])
        sentences.append(sentence)
        count += 1 
        if count % (tot_elements/10) == 0:
            print(f"Clean content: time lapsed {time_check(start_time)}, elements processed {math.ceil((count/tot_elements)*100)} %")
    return sentences, ids, labels

def write_to_file(file_name, host_list, list_2, start_time): 
    if list_2 != None: 
        if len(host_list) != len(list_2): 
            print(f"The lists are not of equal length: list 1 {len(host_list)}, list 2 {len(list_2)}")
    with open(file_name, "w") as f:
        tot_elements = len(host_list)
        jump = math.ceil(tot_elements/10)
        for line in range(0, tot_elements): 
            if line % jump == 0: 
                print(f"Write element to file: time lapsed {time_check(start_time)}, element written {math.ceil((line/tot_elements)*100)} %")
            if list_2 == None:
                f.write(f"{host_list[line]}\n") 
            else: 
                f.write(f"{host_list[line]},{list_2[line]}\n")


def transform_to_vector(file_name, sentences, start_time): 
    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    f = open(file_name, "wb")
    f.truncate(0)
    tot_elements = len(sentences)
    jump = math.ceil(tot_elements/10)
    print(f"Embeddings start: time lapsed {time_check(start_time)}")
    for x in range(0, tot_elements, jump): 
        temp_list = sentences[x:x+jump]
        embeddings = model.encode(temp_list)
        for vector in embeddings: 
            dim = np.int32(len(vector))
            f.write(dim.tobytes())
            f.write(np.array(vector, dtype=np.float32).tobytes())
        if x % (jump*100) == 0:
            print(f"Embeddings: time lapsed {time_check(start_time)}, elements processed {math.ceil((x/tot_elements)*100) }%")


def handle_find_label(rank_list, docs_ids, test_docs): 
    pos_hosts = []
    if not rank_list: 
        return False
    rank_list = list(dict.fromkeys(rank_list))
    for d_id in rank_list: 
        try: 
            item = docs_ids[str(d_id)]
        except KeyError: 
            continue
        item = item.strip('\n')
        pos_hosts.append(item)
    if not pos_hosts: 
        return False
    test = max(pos_hosts,key=pos_hosts.count)
    org = test
    join_host = '|'+'|'.join(pos_hosts)+'|'
    num_occ = join_host.count('|'+test+'|')
    num_dot = test.count('.')
    for i in range(0, num_dot): 
        if num_occ > 10: 
            break
        test = test[test.index(".")+1:]
        num_occ = join_host.count("."+test+'|')
        if num_occ < 1:
            print("handle find label: test, num_occ", test, num_occ)
    if num_occ < 10: 
        test_docs[num_occ] = test_docs[num_occ] + 1
        return False
    #if test == "gumball.com": 
    #    print(test, org, num_occ, join_host, rank_list)
    return test 
        

def divide_into_percent(query_label_filepath, host_label_filepath, percent): 
    labeldict = {}
    q_labeldict = {}
    size_host = len(open(host_label_filepath, 'r').readlines())
    with open(host_label_filepath, "r") as f: 
        for line in f: 
            line = line.strip()
            labels = line.split(',')
            for label in labels: 
                if label in labeldict: 
                    labeldict[label] = labeldict[label] + 1
                else: 
                    labeldict[label] = 1
    with open(query_label_filepath, "r") as f: 
        for line in f: 
            line = line.strip()
            if line not in q_labeldict: 
                for p in range(0, len(percent)): 
                    if (labeldict[line]/size_host)*100 > percent[p]: 
                        q_labeldict[line] = p
                        break
    return q_labeldict





