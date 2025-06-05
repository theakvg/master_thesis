from struct import unpack, pack
import pandas as pd
import numpy as np
import time 
import datetime as dt


start = time.time()

file_list = ["x00", "x01", "x02", "x03", "x04"]

for file in file_list:
    query_id_list = []

    col_names = ['Qid', 'Q0', 'Did', 'Rank', 'Score', 'Runstring']
    df = pd.read_csv(f"test_files/{file}", names=col_names, sep=' ')
    len_list = len(df)

    print(f"Time starts: {time.time()-start}, read from file: {file}")


    for index, row in df.iterrows(): 
        if row['Qid'] not in query_id_list: 
            query_id_list.append(row['Qid'])
        if index % 1000000 == 0: 
            print(f"{time.time()-start}, current index: {index}, {len_list-index} left")
    print(f"Done with add to list, {time.time()-start}")

    with open("query_id_list_2.txt", "a") as f: 
        for i in query_id_list: 
            f.write(str(i) + '\n')

    print(f"Done with write to file, {time.time()-start}, finished {file}")

    del df
    del query_id_list

