import pandas as pd 
import os
import json


def clean_data(user_query_list, filepath):
    gp_data = pd.read_csv(filepath, index_col=0).dropna().drop_duplicates()
    name = user_query_list[0]
    gp_data.name = name
    return gp_data
    

def load_data_to_file_system(df, currentdb=None):
    if currentdb is not None:
        if os.path.exists("./" + df.name + "_chunks"):
            print("Dataset already chunked!")
        else:
            os.mkdir("./" + df.name + "_chunks")
    else:
        print("Not in a database, please enter a database with USEDB first")
    chunk_count = 1
    cols = list(df.columns)
    nums = [j for j in range (1, len(list(df.columns))+1)]
    iter = df.itertuples()
    data = {}
    for row in iter:
        data[row[0]] = {k: row[v] for k, v in zip(cols, nums)}
        if len(data) == 1000:
            json_object = json.dumps(data, indent=1)
            with open("./" + df.name + "_chunks/" + df.name + "_chunk" + str(chunk_count) + ".json", "w") as outfile:
                outfile.write(json_object)
            chunk_count += 1
            data = {}
    print("Created ", str(chunk_count - 1), " json files.")