import pandas as pd
import nltk
from nltk.corpus import stopwords 
from nltk.tokenize import word_tokenize

 
col_names = ['Id', 'Host', 'Title', 'Content']
df = pd.read_csv('small.tsv', names=col_names, sep='\t')

stop_words = set(stopwords.words('english'))


f = open("content.txt", "w")

highest_wc = 0
tot_wc = 0
num_of_data = 0
num_of_q_nan = 0
num_of_c_nan = 0
num_c_over_256 = 0
num_q_over_256 = 0
num_fc_over_256 = 0
num_fq_over_256 = 0
tot_cont_word_filtered = 0


for index, row in df.iterrows(): 
    if type(row['Content']) == float: 
        num_of_c_nan += 1 
        continue
    if type(row['Title']) == float:
        num_of_q_nan += 1
        continue

    wc = len(row['Content'].split())
    wcq = len(row['Title'].split())
    print(wc, wcq, wc+wcq)

    word_tokens_cont = word_tokenize(row['Content'])
    word_tokens_query = word_tokenize(row['Title'])
    
    filtered_sentence_cont = [w for w in word_tokens_cont if not w.lower() in stop_words]
    filtered_sentence_query = []
    filtered_sentence_cont = []

    for w in word_tokens_cont: 
        if w not in stop_words: 
            filtered_sentence_cont.append(w)

    for w in word_tokens_query: 
        if w not in stop_words: 
            filtered_sentence_query.append(w)
    
    if wc > highest_wc: 
        highest_wc = wc
    if wc > 256: 
        num_c_over_256 += 1
    if len(filtered_sentence_cont) > 256: 
        num_fc_over_256 += 1
    if len(filtered_sentence_query) > 256:
        num_fq_over_256 += 1
    if wcq > 256: 
        num_q_over_256 += 1

    tot_cont_word_filtered += wc-len(filtered_sentence_cont)
    tot_wc += wc
    num_of_data += 1
    f.write(row['Content']+'\n')
f.close()

print("highest wc: ", highest_wc)
print("total wc: ", tot_wc)
print("number of datapoints: ", num_of_data)
print("snitt of wc per datapoint: ", tot_wc/num_of_data)
print("number of empty content cells: ", num_of_c_nan)
print("number of empty title cells: ", num_of_q_nan)
print("number of contents over 256 wc: ", num_c_over_256)
print("number of titles over 256 wc: ", num_q_over_256)
print("number of filtered contents over 256 wc: ", num_fc_over_256)
print("number of filtered title over 256 wc: ", num_fq_over_256)
print("average words filtered per dp: ", tot_cont_word_filtered/num_of_data)
print("total content words filtered: ", tot_cont_word_filtered)
