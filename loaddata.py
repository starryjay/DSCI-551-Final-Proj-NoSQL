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
        
'''
    os.mkdir("./chunks")
    i = 0
    chunk_count = 1
    for j in range(1000, len(gplay_df), 1000):
        print("Inside for loop")
        gplay_df.iloc[i:j].to_csv(os.path.join("./chunks/chunk" + str(chunk_count) + ".csv"))
        chunk_count += 1
        i += 1000
    print("Created ", str(chunk_count), " files.")
'''
'''

Make chunks directory
Initialize chunk count to 1
for loop (cycle through df, row by row):
    Create dict
    Keep counter of how many keys are in dict
    Put 1000 App names in dict as keys, specifying value for each App as the other column names and values in their own dictionary - i.e. dict of dicts
    Once counter reaches 1000, write dictionary to json object using chunk count (json.dumps())
    Reset dictionary to empty and continue



'''
    

'''
   
file1: {
    
    app1: {
       
       rating: ""
       review: ""
       
    }
    app2: {

    }
}
   
      
   
'''
    
    
    
    
    
if __name__ == "__main__":
    
    
    gp_data = clean_data("googleplaystore.csv")
    load_gp_data_to_file_system(gp_data)
    
    
    
    
    
   
    
    