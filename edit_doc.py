import os
from datetime import date
from loaddata import load_data_to_file_system, clean_data
import json 

def edit(user_query_list, current_db):
    if not os.path.exists('./document/' + user_query_list[0] + ".json"):
        print("Invalid query - tablename does not exist!")
        return
    if user_query_list[1].upper() == 'INSERT':
        if user_query_list[2].upper() != 'FILE':
            ret = insert(user_query_list)
        else:
            ret = insert_file(user_query_list, current_db)
    elif user_query_list[1].upper() == 'DELETE':
        ret = delete(user_query_list)
    elif user_query_list[1].upper() == "UPDATE": 
        ret = update(user_query_list)
    return ret

def insert(user_query_list):
    doc = None 
    with open("./document/" + user_query_list[0] + ".json"):
        doc = json.load("./document/" + user_query_list[0] + ".json")
    record_read = [tuple(data.split('=')) for data in user_query_list[2:]]
    record_data = record_read[1:]
    record_name = record_read[0][1]
    record = {record_name: dict(record_data)}
    for k, v in record[record_name]:
        datatype = doc[k]
        if datatype == "int":
            doc[k] = int(v)
        elif datatype == "str":
            doc[k] == str(v)
        elif datatype == "float":
            doc[k] == float(v)
        elif datatype == "datetime64":
            doc[k] = date(v)
    print(record)
    chunk_path = "./" + user_query_list[0] + "_chunks"
    if os.path.exists(chunk_path):
        if os.listdir(chunk_path):
            chunknolist = []
            for chunk in os.listdir(chunk_path):
                if os.path.isfile(chunk_path + "/" + chunk):
                    chunkno = ""
                    for c in chunk: 
                        if c.isdigit(): 
                            chunkno += c 
                    chunkno = int(chunkno)
                    chunknolist.append(chunkno)
            last = user_query_list[0] + "_chunk" + str(max(chunknolist)) + ".json"
            with open(chunk_path+"/"+last) as curr_chunk:
                curr_doc = json.load(curr_chunk)
            if len(curr_doc) < 1000:
                newnode = json.dumps(record, indent=1)
                with open(chunk_path+"/"+last, "w") as curr_chunk:
                    curr_chunk.write(newnode)
                print("Inserted into existing chunk number (", last, "): \n", record)
            else:
                curr_doc = json.dumps(record, indent=1)
                new = user_query_list[0] + "_chunk" + str(max(chunknolist) + 1) + ".json"
                with open(chunk_path + "/" + new, "w") as new_chunk:
                    new_chunk.write(curr_doc)
                print("Inserted into new chunk (", new, "): ", record)
    else: 
        os.mkdir(chunk_path)
        curr_doc = json.dumps(record, indent=1)
        new = user_query_list[0] + "_chunk" + str(1) + ".json"
        with open(chunk_path + "/" + new, "w") as new_chunk:
            new_chunk.write(curr_doc)
        print("No chunks exist. Inserted into new chunk (", new, "): ", record)
        
def insert_file(user_query_list, current_db):
    doc = None 
    with open("./document/" + user_query_list[0] + ".json"):
        doc = json.load("./document/" + user_query_list[0] + ".json")
    filepath = user_query_list[3]
    filename = os.path.basename(filepath)
    df = clean_data(user_query_list, filepath)
    load_data_to_file_system(df, current_db)
    chunk_path = "./" + user_query_list[0] + "_chunks"
    if not os.path.exists(chunk_path): 
        print("Chunk path not found!")
        return
    else:   
        for chunk in os.listdir(chunk_path):
            curr_chunk = json.load(chunk_path + "/" + chunk)
            for k, v in curr_chunk.values():
                datatype = doc[k]
                if datatype == "int":
                    curr_chunk[k] = int(v)
                elif datatype == "str":
                    curr_chunk[k] == str(v)
                elif datatype == "float":
                    curr_chunk[k] == float(v)
                elif datatype == "datetime64":
                    curr_chunk[k] = date(v)
            cast_chunk = json.dumps(curr_chunk, indent=1)
            with open(chunk_path + "/" + curr_chunk) as outfile:
                outfile.write(cast_chunk)
        print("Inserted file", filename)

def update(user_query_list):
    with open("./document/" + user_query_list[0] + ".json"):
        doc = json.load("./document/" + user_query_list[0] + ".json")
    listoftuples = [tuple(data.split('=')) for data in user_query_list[2:]]
    nodename = listoftuples[0][1]
    listoftuples = listoftuples[1:]
    mods = {i[0]: i[1] for i in listoftuples}
    chunk_path = "./" + user_query_list[0] + "_chunks"
    if os.path.exists(chunk_path):
        for chunk in os.listdir(chunk_path):
            curr_chunk = json.load(chunk_path + "/" + chunk)
            if nodename in curr_chunk.keys():
                for k in curr_chunk[nodename].keys():
                    #update values
                    if k in mods.keys():
                        curr_chunk[nodename][k] = mods[k]
                        datatype = doc[k]
                        if datatype == "int":
                            curr_chunk[nodename][k] = int(mods[k])
                        elif datatype == "str":
                            curr_chunk[nodename][k] == str(mods[k])
                        elif datatype == "float":
                            curr_chunk[nodename][k] == float(mods[k])
                        elif datatype == "datetime64":
                            curr_chunk[nodename][k] = date(mods[k])
                out_chunk = json.dumps(curr_chunk, indent=1)
                with open(chunk_path + "/" + chunk, "w") as outfile:
                    outfile.write(out_chunk)

def delete(user_query_list):
    nodename = user_query_list[2].split("=")[1]
    chunk_path = "./" + user_query_list[0] + "_chunks"
    if os.path.exists(chunk_path):
        for chunk in os.listdir(chunk_path):
            curr_chunk = json.load(chunk_path + "/" + chunk)
            if nodename in curr_chunk.keys():
                curr_chunk.pop(nodename)
                out_chunk = json.dumps(curr_chunk, indent=1)
                with open(chunk_path + "/" + chunk, "w") as outfile:
                    outfile.write(out_chunk)
                break
        print("deleted node", nodename)