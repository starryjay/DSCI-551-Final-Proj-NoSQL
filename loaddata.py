import pandas as pd 
import os
import json


def clean_data(gplay_filename):
    re_data = pd.read_csv(gplay_filename).dropna().drop_duplicates()
    return re_data
    

def load_gp_data_to_file_system(gplay_df):
    os.mkdir("./chunks")
    chunk_count = 1
    cols = list(gplay_df.columns)[1:]
    nums = [j for j in range (1, 13)]
    iter = gplay_df.itertuples(index=False)
    data = {}
    key_counter = 0
    for row in iter:
        data[row[0]] = {k: row[v] for k, v in zip(cols, nums)}
        key_counter += 1
        if key_counter == 1000:
            json_object = json.dumps(data, indent=1)
            with open("./chunks/chunk" + str(chunk_count) + ".json", "w") as outfile:
                outfile.write(json_object)
            chunk_count += 1
            key_counter = 0
            data = {}
    print("Created ", str(chunk_count - 1), " json files.")
    
if __name__ == "__main__":
    
    
    gp_data = clean_data("googleplaystore.csv")
    load_gp_data_to_file_system(gp_data)
    
    
    
    
    
   
    
    
