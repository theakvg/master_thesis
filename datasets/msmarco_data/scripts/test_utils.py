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
        for ps in prestring: 
            if ps in host: 
                host = host.split(ps)[1]
                check = True
                break
        if not check: 
            print("Found no prestring for following host: ", host)
        
        host = host.split('/')[0]
        return host

    def tester(): 
        print("heii")

        
    def time_check(start_time):
        sec = time.time() - start_time
        td = datetime.datetime.utcfromtimestamp(sec)
        return td.strftime('%H:%M:%S.%f')[:-4]

    def clean_file_content(self, name_list, error_list, df, prestrings, tot_elements, start_time): 
        sentences = []
        labels = []
        count = 0
        self.tester()
        for index, row in df.iterrows(): 
            check_elements = check_if_df_elements_are_correct_type(name_list, error_list, row)
            if not check_elements:
                continue
            host = prepare_host(row['Host'], prestrings)
            labels.append(host)
            sentence = row['Title'][:100]+row['Content'][:2000]
            sentences.append(sentence)
            count += 1 
            if count % (tot_elements/10) == 0:
                print(f"Clean content: time lapsed {time_check(start_time)}, elements processed {math.ceil(count/tot_elements)} %")
        return sentences, labels

    def write_labels_to_file(file_name, host_list, start_time): 
        with open(file_name, "w") as f:
            tot_elements = len(host_list)
            for line in range(0, tot_elements): 
                if line % tot_elements/10 == 0: 
                    print(f"Write label: time lapsed {time_check(start_time)}, label written {math.ceil(line/tot_elements)} %")
                    f.write("{host_list[line]}\n")


    def transform_to_vector(file_name, sentences, start_time): 
        model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
        f = open(file_name, "wb")
        f.truncate(0)
        tot_elements = len(sentences)
        jump = math.ceil(tot_elements/20)
        for x in range(0, tot_elements, jump): 
            temp_list = sentences[x:x+jump]
            embeddings = model.encode(temp_list)
            for vector in embeddings: 
                dim = np.int32(len(vector))
                f.write(dim.tobytes())
                f.write(np.array(vector, dtype=np.float32).tobytes())
            print(f"Embeddings: time lapsed {time_check(start_time)}, elements processed {math.ceil(tot_elements/x) }%")




