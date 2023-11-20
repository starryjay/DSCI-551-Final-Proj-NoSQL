import json
import pandas as pd
import numpy as np
import os
import warnings
from printoutput import find_directory
from datetime import date
warnings.simplefilter(action='ignore', category=FutureWarning)

# [docname, NODES, nodes, TOTALNUM/SUM/MEAN/MIN/MAX, node, 
# BUNCH, node, SORT, node, ASC/DESC, MERGE, docname2, HAS, conditions]


# Check for bunch/agg, pass into bunch_agg()   
# Else check for bunch or agg individually, pass into bunch() or agg_functions()
# Check for sort, pass into sort()
# Check for merge without sort, pass into sort_merge()
# Check for HAS, pass into has()
# Filter columns with get_columns()
# Locate most updated directory and print final doc


def fetch(user_query_list):
    with open("./" + user_query_list[0] + "_chunks/" + user_query_list[0] + "_chunk1.json", "r") as docread:
        doc = json.load(docread)
    doc = json.dumps(doc, indent=1)
    agglist = ["TOTALNUM", "SUM", "MEAN", "MIN", "MAX"]
    uqlupper = list(map(str.upper, user_query_list))
    if "BUNCH" in uqlupper and (not set(agglist).isdisjoint(set(uqlupper))):
        if type(doc) != dict:
            with open("./" + user_query_list[0] + "_chunks/" + user_query_list[0] + "_chunk1.json", "r") as docread:
                doc = json.load(docread)
        doc = bunch_agg(user_query_list, doc)
    elif "BUNCH" in uqlupper:
        if type(doc) != dict:
            with open("./" + user_query_list[0] + "_chunks/" + user_query_list[0] + "_chunk1.json", "r") as docread:
                doc = json.load(docread)
        doc = bunch(user_query_list, doc)
    elif (not set(agglist).isdisjoint(set(uqlupper))):
        if type(doc) != dict:
            with open("./" + user_query_list[0] + "_chunks/" + user_query_list[0] + "_chunk1.json", "r") as docread:
                doc = json.load(docread)
        doc = agg_functions(user_query_list, doc)
        if type(doc) == str:
            return
    if "SORT" in uqlupper:
        if type(doc) != dict:
            with open("./" + user_query_list[0] + "_chunks/" + user_query_list[0] + "_chunk1.json", "r") as docread:
                doc = json.load(docread)
        doc = sort(user_query_list)
    elif "MERGE" in uqlupper:
        doc = merge(user_query_list)
    if "HAS" in uqlupper:
        doc = has(user_query_list)
    if "NODES" in uqlupper:
        nodes_list = get_columns(user_query_list)
        if type(doc) != dict:
            with open("./" + user_query_list[0] + "_chunks/" + user_query_list[0] + "_chunk1.json", "r") as docread:
                doc = json.load(docread)
        newdoc = {}
        for k, v in doc.items():
            for k2, v2 in v.items():
                if k2 in nodes_list and k not in newdoc.keys():
                    newdoc[k] = {k2: v2}
                elif k2 in nodes_list:
                    newdoc[k]
                    newdoc[k][k2] = v2
        newdoc = json.dumps(newdoc, indent=1)
        print(newdoc)
        return
    print(doc)

def get_columns(user_query_list):
    uqlupper = list(map(str.upper, user_query_list))
    kwlist = ["TOTALNUM", "SUM", "MEAN", "MIN", "MAX", "BUNCH", "SORT", "MERGE", "HAS"]
    idxlist = {}
    for kw in kwlist:
        if kw in uqlupper:
            idxlist[uqlupper.index(kw)] = kw
    if idxlist:
        nextkwidx = min(idxlist)
        keys_list = user_query_list[2:nextkwidx]
    else:
        keys_list = user_query_list[2:]
    agglist = kwlist[:5]
    if not set(agglist).isdisjoint(set(uqlupper)):
        if "SUM" in uqlupper:
            aggkey = user_query_list[uqlupper.index("SUM") + 1]
            aggkey = "sum_" + aggkey
            keys_list.append(aggkey)
        elif "TOTALNUM" in uqlupper:
            aggkey = user_query_list[uqlupper.index("TOTALNUM") + 1]
            aggkey = "totalnum_" + aggkey
            keys_list.append(aggkey)
        elif "MEAN" in uqlupper:
            aggkey = user_query_list[uqlupper.index("MEAN") + 1]
            aggkey = "mean_" + aggkey
            keys_list.append(aggkey)
        elif "MIN" in uqlupper:
            aggkey = user_query_list[uqlupper.index("MIN") + 1]
            aggkey = "min_" + aggkey
            keys_list.append(aggkey)
        elif "MAX" in uqlupper:
            aggkey = user_query_list[uqlupper.index("MAX") + 1]
            aggkey = "max_" + aggkey
            keys_list.append(aggkey)
    return keys_list

def agg_functions(user_query_list, doc): 
    if "SUM" in list(map(str.upper, user_query_list)):
        return docsum(user_query_list, doc)
    elif "TOTALNUM" in list(map(str.upper, user_query_list)):
        return totalnum(user_query_list, doc)
    elif "MEAN" in list(map(str.upper, user_query_list)):
        return mean(user_query_list, doc)
    elif "MIN" in list(map(str.upper, user_query_list)):
        return docmin(user_query_list, doc)
    elif "MAX" in list(map(str.upper, user_query_list)):
        return docmax(user_query_list, doc)

def docsum(user_query_list, doc):
    uqlupper = list(map(str.upper, user_query_list))
    key = user_query_list[uqlupper.index("SUM") + 1]
    keytype = list(doc.items())[0][1][key]
    sumkey = "sum_" + key
    total_sum = 0
    if not (isinstance(keytype, int) or isinstance(keytype, float)): 
        print("Error: Cannot sum on this type")
        return "failed"
    if "nodes" in user_query_list and key in get_columns(user_query_list):
        chunk_path = "./" + user_query_list[0] + "_chunks"
        if not os.path.exists(chunk_path + "/col_agg"):
            os.mkdir(chunk_path + "/col_agg")
        for chunk in os.listdir(chunk_path):
            if os.path.isfile(chunk_path + "/" + chunk) and chunk[0] != ".":
                with open(chunk_path+"/"+chunk) as curr_chunk:
                    curr_doc = json.load(curr_chunk)
                for v in curr_doc.values():
                    if key in v:
                        total_sum += v[key]
        for chunk in os.listdir(chunk_path):
            if os.path.isfile(chunk_path + "/" + chunk) and chunk[0] != ".":
                with open(chunk_path+"/"+chunk) as curr_chunk:
                    curr_doc = json.load(curr_chunk)
                for v in curr_doc.values():
                    v[sumkey] = total_sum
                outdoc = json.dumps(curr_doc, indent=1)
                with open(chunk_path + "/col_agg/" + chunk[:-5] + "_col_sum.json", "w") as outfile:
                    outfile.write(outdoc)
            else:
                continue
        with open(chunk_path + "/col_agg/" + user_query_list[0] + "_chunk1_col_sum.json") as docread:
            doc = json.load(docread)
    elif key in get_columns(user_query_list):
        chunk_path = "./" + user_query_list[0] + "_chunks"
        if not os.path.exists(chunk_path + "/agg"):
            os.mkdir(chunk_path + "/agg")
        for chunk in os.listdir(chunk_path):
            if os.path.isfile(chunk_path + "/" + chunk) and chunk[0] != ".":
                with open(chunk_path+"/"+chunk) as curr_chunk:
                    curr_doc = json.load(curr_chunk)
                for v in curr_doc.values():
                    if key in v:
                        total_sum += v[key]
        for chunk in os.listdir(chunk_path):
            if os.path.isfile(chunk_path + "/" + chunk) and chunk[0] != ".":
                with open(chunk_path+"/"+chunk) as curr_chunk:
                    curr_doc = json.load(curr_chunk)
                for v in curr_doc.values():
                    v[sumkey] = total_sum
                outdoc = json.dumps(curr_doc, indent=1)
                with open(chunk_path + "/agg/" + chunk[:-5] + "_sum.json", "w") as outfile:
                    outfile.write(outdoc)
        with open(chunk_path + "/agg/" + user_query_list[0] + "_chunk1_sum.json") as docread:
            doc = json.load(docread)
    return doc 

def totalnum(user_query_list, doc):
    uqlupper = list(map(str.upper, user_query_list))
    key = user_query_list[uqlupper.index("TOTALNUM") + 1]
    totalnumkey = "totalnum_" + key
    total = 0
    if "nodes" in user_query_list and key in get_columns(user_query_list):
        chunk_path = "./" + user_query_list[0] + "_chunks"
        if not os.path.exists(chunk_path + "/col_agg"):
            os.mkdir(chunk_path + "/col_agg")
        for chunk in os.listdir(chunk_path):
            if os.path.isfile(chunk_path + "/" + chunk) and chunk[0] != ".":
                with open(chunk_path+"/"+chunk) as curr_chunk:
                    curr_doc = json.load(curr_chunk)
                for v in curr_doc.values():
                    if key in v:
                        total += 1
        for chunk in os.listdir(chunk_path):
            if os.path.isfile(chunk_path + "/" + chunk) and chunk[0] != ".":
                with open(chunk_path+"/"+chunk) as curr_chunk:
                    curr_doc = json.load(curr_chunk)
                for v in curr_doc.values():
                    v[totalnumkey] = total
                outdoc = json.dumps(curr_doc, indent=1)
                with open(chunk_path + "/col_agg/" + chunk[:-5] + "_col_totalnum.json", "w") as outfile:
                    outfile.write(outdoc)
        with open(chunk_path + "/col_agg/" + user_query_list[0] + "_chunk1_col_totalnum.json") as docread:
            doc = json.load(docread)
    elif key in get_columns(user_query_list):
        chunk_path = "./" + user_query_list[0] + "_chunks"
        if not os.path.exists(chunk_path + "/agg"):
            os.mkdir(chunk_path + "/agg")
        for chunk in os.listdir(chunk_path):
            if os.path.isfile(chunk_path + "/" + chunk) and chunk[0] != ".":
                with open(chunk_path+"/"+chunk) as curr_chunk:
                    curr_doc = json.load(curr_chunk)
                for v in curr_doc.values():
                    if key in v:
                        total += 1
        for chunk in os.listdir(chunk_path):
            if os.path.isfile(chunk_path + "/" + chunk) and chunk[0] != ".":
                with open(chunk_path+"/"+chunk) as curr_chunk:
                    curr_doc = json.load(curr_chunk)
                for v in curr_doc.values():
                    v[totalnumkey] = total
                outdoc = json.dumps(curr_doc, indent=1)
                with open(chunk_path + "/agg/" + chunk[:-5] + "_totalnum.json", "w") as outfile:
                    outfile.write(outdoc)
        with open(chunk_path + "/agg/" + user_query_list[0] + "_chunk1_totalnum.json") as docread:
            doc = json.load(docread)
    return doc


def mean(user_query_list, doc):
    uqlupper = list(map(str.upper, user_query_list))
    key = user_query_list[uqlupper.index("MEAN") + 1]
    keytype = list(doc.items())[0][1][key]
    if not (isinstance(keytype, int) or isinstance(keytype, float)): 
        print("Error: Cannot calculate mean on this type")
        return "failed"
    meankey = "mean_" + key
    total = 0
    num_nodes = 0
    if "nodes" in user_query_list and key in get_columns(user_query_list):
        chunk_path = "./" + user_query_list[0] + "_chunks"
        if not os.path.exists(chunk_path + "/col_agg"):
            os.mkdir(chunk_path + "/col_agg")
        for chunk in os.listdir(chunk_path):
            if os.path.isfile(chunk_path + "/" + chunk) and chunk[0] != ".":
                with open(chunk_path+"/"+chunk) as curr_chunk:
                    curr_doc = json.load(curr_chunk)
                for v in curr_doc.values():
                    if key in v:
                        total += v[key]
                        num_nodes += 1
        for chunk in os.listdir(chunk_path):
            if os.path.isfile(chunk_path + "/" + chunk) and chunk[0] != ".":
                with open(chunk_path+"/"+chunk) as curr_chunk:
                    curr_doc = json.load(curr_chunk)
                for v in curr_doc.values():
                    v[meankey] = total/num_nodes
                outdoc = json.dumps(curr_doc, indent=1)
                with open(chunk_path + "/col_agg/" + chunk[:-5] + "_col_mean.json", "w") as outfile:
                    outfile.write(outdoc)
        with open(chunk_path + "/col_agg/" + user_query_list[0] + "_chunk1_col_mean.json") as docread:
            doc = json.load(docread)
    elif key in get_columns(user_query_list):
        chunk_path = "./" + user_query_list[0] + "_chunks"
        if not os.path.exists(chunk_path + "/agg"):
            os.mkdir(chunk_path + "/agg")
        for chunk in os.listdir(chunk_path):
            if os.path.isfile(chunk_path + "/" + chunk) and chunk[0] != ".":
                with open(chunk_path+"/"+chunk) as curr_chunk:
                    curr_doc = json.load(curr_chunk)
                for v in curr_doc.values():
                    if key in v:
                        total += v[key]
                        num_nodes += 1
        for chunk in os.listdir(chunk_path):
            if os.path.isfile(chunk_path + "/" + chunk) and chunk[0] != ".":
                with open(chunk_path+"/"+chunk) as curr_chunk:
                    curr_doc = json.load(curr_chunk)
                for v in curr_doc.values():
                    v[meankey] = total/num_nodes
                outdoc = json.dumps(curr_doc, indent=1)
                with open(chunk_path + "/agg/" + chunk[:-5] + "_mean.json", "w") as outfile:
                    outfile.write(outdoc)
        with open(chunk_path + "/agg/" + user_query_list[0] + "_chunk1_mean.json") as docread:
            doc = json.load(docread)
    return doc

def docmin(user_query_list, doc):
    uqlupper = list(map(str.upper, user_query_list))
    key = user_query_list[uqlupper.index("MIN") + 1]
    minkey = "min_" + key
    curr_min = float("inf")
    if "nodes" in user_query_list and key in get_columns(user_query_list):
        chunk_path = "./" + user_query_list[0] + "_chunks"
        if not os.path.exists(chunk_path + "/col_agg"):
            os.mkdir(chunk_path + "/col_agg")
        for chunk in os.listdir(chunk_path):
            if os.path.isfile(chunk_path + "/" + chunk) and chunk[0] != ".":
                with open(chunk_path+"/"+chunk) as curr_chunk:
                    curr_doc = json.load(curr_chunk)
                for v in curr_doc.values():
                    if key in v and v[key] < curr_min:
                        curr_min = v[key]
        for chunk in os.listdir(chunk_path):
            if os.path.isfile(chunk_path + "/" + chunk) and chunk[0] != ".":
                with open(chunk_path+"/"+chunk) as curr_chunk:
                    curr_doc = json.load(curr_chunk)
                for v in curr_doc.values():
                    v[minkey] = curr_min
                outdoc = json.dumps(curr_doc, indent=1)
                with open(chunk_path + "/col_agg/" + chunk[:-5] + "_col_min.json", "w") as outfile:
                    outfile.write(outdoc)
        with open(chunk_path + "/col_agg/" + user_query_list[0] + "_chunk1_col_min.json") as docread:
            doc = json.load(docread)
    elif key in get_columns(user_query_list):
        chunk_path = "./" + user_query_list[0] + "_chunks"
        if not os.path.exists(chunk_path + "/agg"):
            os.mkdir(chunk_path + "/agg")
        for chunk in os.listdir(chunk_path):
            if os.path.isfile(chunk_path + "/" + chunk) and chunk[0] != ".":
                with open(chunk_path+"/"+chunk) as curr_chunk:
                    curr_doc = json.load(curr_chunk)
                for v in curr_doc.values():
                    if key in v and v[key] < curr_min:
                        curr_min = v[key]
        for chunk in os.listdir(chunk_path):
            if os.path.isfile(chunk_path + "/" + chunk) and chunk[0] != ".":
                with open(chunk_path+"/"+chunk) as curr_chunk:
                    curr_doc = json.load(curr_chunk)
                for v in curr_doc.values():
                    v[minkey] = curr_min
                outdoc = json.dumps(curr_doc, indent=1)
                with open(chunk_path + "/agg/" + chunk[:-5] + "_min.json", "w") as outfile:
                    outfile.write(outdoc)
        with open(chunk_path + "/agg/" + user_query_list[0] + "_chunk1_min.json") as docread:
            doc = json.load(docread)
    return doc

def docmax(user_query_list, doc):
    uqlupper = list(map(str.upper, user_query_list))
    key = user_query_list[uqlupper.index("MAX") + 1]
    maxkey = "max_" + key
    curr_max = float("-inf")
    if "nodes" in user_query_list and key in get_columns(user_query_list):
        chunk_path = "./" + user_query_list[0] + "_chunks"
        if not os.path.exists(chunk_path + "/col_agg"):
            os.mkdir(chunk_path + "/col_agg")
        for chunk in os.listdir(chunk_path):
            if os.path.isfile(chunk_path + "/" + chunk) and chunk[0] != ".":
                with open(chunk_path+"/"+chunk) as curr_chunk:
                    curr_doc = json.load(curr_chunk)
                for v in curr_doc.values():
                    if key in v and v[key] > curr_max:
                        curr_max = v[key]
        for chunk in os.listdir(chunk_path):
            if os.path.isfile(chunk_path + "/" + chunk) and chunk[0] != ".":
                with open(chunk_path+"/"+chunk) as curr_chunk:
                    curr_doc = json.load(curr_chunk)
                for v in curr_doc.values():
                    v[maxkey] = curr_max
                outdoc = json.dumps(curr_doc, indent=1)
                with open(chunk_path + "/col_agg/" + chunk[:-5] + "_col_max.json", "w") as outfile:
                    outfile.write(outdoc)
        with open(chunk_path + "/col_agg/" + user_query_list[0] + "_chunk1_col_max.json") as docread:
            doc = json.load(docread)
    elif key in get_columns(user_query_list):
        chunk_path = "./" + user_query_list[0] + "_chunks"
        if not os.path.exists(chunk_path + "/agg"):
            os.mkdir(chunk_path + "/agg")
        for chunk in os.listdir(chunk_path):
            if os.path.isfile(chunk_path + "/" + chunk) and chunk[0] != ".":
                with open(chunk_path+"/"+chunk) as curr_chunk:
                    curr_doc = json.load(curr_chunk)
                for v in curr_doc.values():
                    if key in v and v[key] > curr_max:
                        curr_max = v[key]
        for chunk in os.listdir(chunk_path):
            if os.path.isfile(chunk_path + "/" + chunk) and chunk[0] != ".":
                with open(chunk_path+"/"+chunk) as curr_chunk:
                    curr_doc = json.load(curr_chunk)
                for v in curr_doc.values():
                    v[maxkey] = curr_max
                outdoc = json.dumps(curr_doc, indent=1)
                with open(chunk_path + "/agg/" + chunk[:-5] + "_max.json", "w") as outfile:
                    outfile.write(outdoc)
        with open(chunk_path + "/agg/" + user_query_list[0] + "_chunk1_max.json") as docread:
            doc = json.load(docread)
    return doc

def bunch_agg(user_query_list, doc):
    uqlupper = list(map(str.upper, user_query_list))
    agg_list = ["SUM", "TOTALNUM", "MEAN", "MIN", "MAX"]
    agg_func = (set(agg_list).intersection(set(uqlupper)).pop()).lower()
    bunchkey = user_query_list[uqlupper.index("BUNCH") + 1]
    unique_bunchkey_list = []
    nodes_list = get_columns(user_query_list)
    if not os.path.exists("./" + user_query_list[0] + "_chunks"):
        print("Chunks folder does not exist")
        return
    else:
        bunch_agg_chunk_path=os.path.join("./" + user_query_list[0] + "_chunks", "bunch_agg_chunks")
        if not os.path.exists(bunch_agg_chunk_path):
            os.mkdir(bunch_agg_chunk_path)
    keys_list = get_columns(user_query_list)
    if bunchkey.upper() not in list(map(str.upper, keys_list)):
        print("Node to bunch must be selected in NODES")
        return
    else:
        chunk_path = "./" + user_query_list[0] + "_chunks"
        for chunk in os.listdir(chunk_path):
            with open(chunk_path+"/"+chunk) as curr_chunk:
                curr_doc = json.load(curr_chunk)
            for app in curr_doc.values():
                if app[bunchkey] not in unique_bunchkey_list:
                    unique_bunchkey_list.append(app[bunchkey])
        bunched_dict = {key: {} for key in unique_bunchkey_list}
        for chunk in os.listdir(chunk_path):
            with open(chunk_path+"/"+chunk) as curr_chunk:
                curr_doc = json.load(curr_chunk)
            for appname, appdata in curr_doc.items():
                bunched_dict[appdata[bunchkey]][appname] = appdata
            bunched = json.dumps(bunched_dict, indent=1)
            with open(bunch_agg_chunk_path + "/" + chunk[:-5] + "_bunch.json", "w") as outfile:
                outfile.write(bunched)
            bunched_dict = {key: {} for key in unique_bunchkey_list}
        if "SUM" in list(map(str.upper, user_query_list)):
            sumcol = user_query_list[uqlupper.index("SUM") + 1]
            sumkey = 'sum_' + sumcol
            if sumcol.upper() not in list(map(str.upper, nodes_list)):
                print("Node to aggregate must be selected in NODES")
                return
            sumdict = {key: 0 for key in unique_bunchkey_list}
            for filename in os.listdir(bunch_agg_chunk_path) and filename.endswith("_bunch.json"):
                with open(bunch_agg_chunk_path + "/" + filename) as infile:
                    curr_dict = json.load(infile)
                nodesum = 0 
                for k, v in curr_dict.items(): 
                    for k1, v2 in v.items(): 
                        if k1 == sumcol:
                            nodesum += v2
                    sumdict[k] += nodesum
                    nodesum = 0
            for filename in os.listdir(bunch_agg_chunk_path) and filename.endswith("_bunch.json"):
                with open(bunch_agg_chunk_path + "/" + filename) as infile:
                    curr_dict = json.load(infile)
                for group, app in curr_dict.items():
                    for appname, appdata in app.items(): 
                        appdata[sumkey] = sumdict[group]
                curr_dict = json.dumps(curr_dict, indent=1)
                with open(bunch_agg_chunk_path + "/" + chunk[:-5] + "_bunch_" + agg_func + ".json", "w") as outfile:
                    outfile.write(curr_dict)
            return curr_dict
        elif "TOTALNUM" in list(map(str.upper, user_query_list)):
            totalcol = user_query_list[uqlupper.index("TOTALNUM") + 1]
            totalkey = 'totalnum_' + totalcol
            if totalcol.upper() not in list(map(str.upper, nodes_list)):
                print("Node to aggregate must be selected in NODES")
                return
            totaldict = {key: 0 for key in unique_bunchkey_list}
            for filename in os.listdir(bunch_agg_chunk_path) and filename.endswith("_bunch.json"):
                with open(bunch_agg_chunk_path + "/" + filename) as infile:
                    curr_dict = json.load(infile)
                nodecount = 0 
                for k, v in curr_dict.items(): 
                    for k1, v2 in v.items(): 
                        if k1 == totalcol:
                            nodecount += 1
                    totaldict[k] += nodecount
                    nodecount = 0
            for filename in os.listdir(bunch_agg_chunk_path) and filename.endswith("_bunch.json"):
                with open(bunch_agg_chunk_path + "/" + filename) as infile:
                    curr_dict = json.load(infile)
                for group, app in curr_dict.items():
                    for appname, appdata in app.items(): 
                        appdata[totalkey] = totaldict[group]
                curr_dict = json.dumps(curr_dict, indent=1)
                with open(bunch_agg_chunk_path + "/" + chunk[:-5] + "_bunch_" + agg_func + ".json", "w") as outfile:
                    outfile.write(curr_dict)
            return curr_dict
        elif "MEAN" in list(map(str.upper, user_query_list)):
            meancol = user_query_list[uqlupper.index("MEAN") + 1]
            meankey = 'mean_' + meancol
            if meancol.upper() not in list(map(str.upper, nodes_list)):
                print("Node to aggregate must be selected in NODES")
                return
            meandict = {key: 0 for key in unique_bunchkey_list}
            for filename in os.listdir(bunch_agg_chunk_path) and filename.endswith("_bunch.json"):
                with open(bunch_agg_chunk_path + "/" + filename) as infile:
                    curr_dict = json.load(infile)
                nodesum = 0 
                nodecount = 0
                for k, v in curr_dict.items(): 
                    for k1, v2 in v.items(): 
                        if k1 == meancol:
                            nodesum += v2
                            nodecount += 1
                    meandict[k] += nodesum/nodecount
                    nodesum = 0
                    nodecount = 0
            for filename in os.listdir(bunch_agg_chunk_path) and filename.endswith("_bunch.json"):
                with open(bunch_agg_chunk_path + "/" + filename) as infile:
                    curr_dict = json.load(infile)
                for group, app in curr_dict.items():
                    for appname, appdata in app.items(): 
                        appdata[meankey] = meandict[group]
                curr_dict = json.dumps(curr_dict, indent=1)
                with open(bunch_agg_chunk_path + "/" + chunk[:-5] + "_bunch_" + agg_func + ".json", "w") as outfile:
                    outfile.write(curr_dict)
            return curr_dict
        elif "MIN" in list(map(str.upper, user_query_list)):
            mincol = user_query_list[uqlupper.index("MIN") + 1]
            minkey = 'min_' + mincol
            if mincol.upper() not in list(map(str.upper, nodes_list)):
                print("Node to aggregate must be selected in NODES")
                return
            mindict = {key: 0 for key in unique_bunchkey_list}
            for filename in os.listdir(bunch_agg_chunk_path) and filename.endswith("_bunch.json"):
                with open(bunch_agg_chunk_path + "/" + filename) as infile:
                    curr_dict = json.load(infile)
                nodemin = float("inf")
                for k, v in curr_dict.items(): 
                    for k1, v2 in v.items(): 
                        if k1 == mincol and v2 < nodemin:
                            nodemin = v2
                    nodemin = float("inf")
            for filename in os.listdir(bunch_agg_chunk_path) and filename.endswith("_bunch.json"):
                with open(bunch_agg_chunk_path + "/" + filename) as infile:
                    curr_dict = json.load(infile)
                for group, app in curr_dict.items():
                    for appname, appdata in app.items(): 
                        appdata[minkey] = mindict[group]
                curr_dict = json.dumps(curr_dict, indent=1)
                with open(bunch_agg_chunk_path + "/" + chunk[:-5] + "_bunch_" + agg_func + ".json", "w") as outfile:
                    outfile.write(curr_dict)
            return curr_dict
        elif "MAX" in list(map(str.upper, user_query_list)):
            maxcol = user_query_list[uqlupper.index("MAX") + 1]
            maxkey = 'max_' + maxcol
            if maxcol.upper() not in list(map(str.upper, nodes_list)):
                print("Node to aggregate must be selected in NODES")
                return
            maxdict = {key: 0 for key in unique_bunchkey_list}
            for filename in os.listdir(bunch_agg_chunk_path) and filename.endswith("_bunch.json"):
                with open(bunch_agg_chunk_path + "/" + filename) as infile:
                    curr_dict = json.load(infile)
                nodemax = float("-inf")
                for k, v in curr_dict.items(): 
                    for k1, v2 in v.items(): 
                        if k1 == maxcol and v2 > nodemax:
                            nodemax = v2
                    nodemax = float("-inf")
            for filename in os.listdir(bunch_agg_chunk_path) and filename.endswith("_bunch.json"):
                with open(bunch_agg_chunk_path + "/" + filename) as infile:
                    curr_dict = json.load(infile)
                for group, app in curr_dict.items():
                    for appname, appdata in app.items(): 
                        appdata[maxkey] = maxdict[group]
                curr_dict = json.dumps(curr_dict, indent=1)
                with open(bunch_agg_chunk_path + "/" + chunk[:-5] + "_bunch_" + agg_func + ".json", "w") as outfile:
                    outfile.write(curr_dict)
            return curr_dict

def bunch(user_query_list, doc):
    bunchidx = list(map(str.upper, user_query_list)).index("BUNCH") + 1
    bunchkey = user_query_list[bunchidx]
    unique_bunchkey_list = []
    if not os.path.exists("./" + user_query_list[0] + "_chunks"):
        print("Chunks folder does not exist")
        return
    else:
        bunched_chunk_path=os.path.join("./" + user_query_list[0] + "_chunks", "bunched_chunks")
        if not os.path.exists(bunched_chunk_path):
            os.mkdir(bunched_chunk_path)
    keys_list = get_columns(user_query_list)
    if bunchkey.upper() not in list(map(str.upper, keys_list)):
        print("Node to bunch must be selected in NODES")
        return
    else:
        chunk_path = "./" + user_query_list[0] + "_chunks"
        for chunk in os.listdir(chunk_path):
            with open(chunk_path+"/"+chunk) as curr_chunk:
                curr_doc = json.load(curr_chunk)
            for app in curr_doc.values():
                if app[bunchkey] not in unique_bunchkey_list:
                    unique_bunchkey_list.append(app[bunchkey])
        bunched_dict = {key: {} for key in unique_bunchkey_list}
        for chunk in os.listdir(chunk_path):
            with open(chunk_path+"/"+chunk) as curr_chunk:
                curr_doc = json.load(curr_chunk)
            for appname, appdata in curr_doc.items():
                bunched_dict[appdata[bunchkey]][appname] = appdata
            bunched = json.dumps(bunched_dict, indent=1)
            with open(bunched_chunk_path + "/" + chunk[:-5] + "_bunch.json", "w") as outfile:
                outfile.write(bunched)
            bunched_dict = {key: {} for key in unique_bunchkey_list}
        with open(bunched_chunk_path + "/" + user_query_list[0] + "_chunk1" + "_bunch.json") as docread:
            doc = json.load(docread)
    return doc

def merge(user_query_list):
    sortcol = user_query_list[list(map(str.upper, user_query_list)).index("INCOMMON") + 1]
    final_doc = sort_merge(user_query_list, sortcol)
    return final_doc
    
def sort(user_query_list):
    sorted_dict = {}
    uqlupper = list(map(str.upper, user_query_list))
    sortnode = user_query_list[uqlupper.index("SORT") + 1]
    direction = None
    if "ASC" in uqlupper:
        direction = "ASC"
    elif "DESC" in uqlupper:
        direction = "DESC"
    elif direction is None:
        print("Please specify direction to sort by as ASC or DESC!")
        return
    if "BUNCH" in uqlupper:
        sorted_dict = sort_bunch(user_query_list, sortnode)
    if "MERGE" in uqlupper:
        common_node = user_query_list[uqlupper.index("INCOMMON") + 1]
        sorted_dict = sort_merge(user_query_list, common_node)
    elif "BUNCH" not in uqlupper and "MERGE" not in uqlupper:
        directory = find_directory(user_query_list)
        directory = sort_within_chunks(user_query_list, sortnode, directory)
        sorted_dict = sort_between_chunks(user_query_list, sortnode, directory)
    if direction == "ASC":
        return sorted_dict
    elif direction == "DESC":
        return dict(list(sorted_dict).reverse())

def simple_sort(sortcol, doc):
    doc_list = [[k, v] for k, v in doc]
    sorted_doc = merge_sort(doc_list, sortcol)
    return sorted_doc

def sort_bunch(user_query_list, sortcol):
    uqlupper = list(map(str.upper, user_query_list))
    agglist = ["TOTALNUM", "SUM", "MEAN", "MIN", "MAX"]
    agg_present = not set(agglist).isdisjoint(set(uqlupper))
    which_agg = tuple(set(agglist).intersection(set(uqlupper)))[0]
    bunchcol = user_query_list[uqlupper.index("BUNCH") + 1] + "_bunched"
    if agg_present:
        file_path = os.path.join("./"+ user_query_list[0] + "_chunks", "bunch_agg_chunks")
    else:
        file_path = os.path.join("./"+ user_query_list[0] + "_chunks", "bunched_chunks")
    sorted_chunk_directory = "./"+ user_query_list[0] + "_chunks/sorted_chunks"
    for chunk in os.listdir(file_path):
        if os.path.isfile(file_path + "/" + chunk) and agg_present and chunk[0] != "." and chunk.endswith(which_agg.lower() + ".json"):      # CHANGE THIS IN RELATIONAL
            with open(file_path + "/" + chunk) as docread:
                doc = json.load(docread)
            doc_keys = list(doc.keys())
            if not os.path.exists(sorted_chunk_directory):
                os.mkdir(sorted_chunk_directory)
            if bunchcol[:-8] == sortcol:
                doc_keys.sort()
                sorted_doc = {i: doc[i] for i in doc_keys}
                sorted_doc = json.dumps(sorted_doc, indent=1)
                with open(sorted_chunk_directory + "/" + chunk[:-5] + "_sorted_on_bunch.json", "w") as outfile:
                    outfile.write(sorted_doc)
            else:
                doc_num = 1
                for key in doc_keys:
                    newdoc = doc[key]
                    newdoc = merge_sort([[k, v] for k, v in newdoc], sortcol) # Check if this is correct
                    newdoc = json.dumps(newdoc, indent=1)
                    with open(sorted_chunk_directory + "/" + chunk[:-5] + "_sorted_level_" + str(doc_num) + ".json") as outfile:
                        outfile.write(newdoc)
                    doc_num += 1
        elif os.path.isfile(file_path + "/" + chunk) and not agg_present and chunk[0] != "." and chunk.endswith("_bunch.json"):      # ADD THIS IN RELATIONAL
            with open(file_path + "/" + chunk) as docread:
                doc = json.load(docread)
            doc_keys = list(doc.keys())
            if not os.path.exists(sorted_chunk_directory):
                os.mkdir(sorted_chunk_directory)
            if bunchcol[:-8] == sortcol:
                doc_keys.sort()
                sorted_doc = {i: doc[i] for i in doc_keys}
                sorted_doc = json.dumps(sorted_doc, indent=1)
                with open(sorted_chunk_directory + "/" + chunk[:-5] + "_sorted_on_bunch.json", "w") as outfile:
                    outfile.write(sorted_doc)
            else:
                doc_num = 1
                for key in doc_keys:
                    newdoc = doc[key]
                    sorted_doc = merge_sort([[k, v] for k, v in newdoc], sortcol) # Check if this is correct
                    sorted_doc = json.dumps(newdoc, indent=1)
                    with open(sorted_chunk_directory + "/" + chunk[:-5] + "_sorted_level_" + str(doc_num) + ".json") as outfile:
                        outfile.write(sorted_doc)
                    doc_num += 1
    final_sorted_doc = {}
    for filename in sorted(os.listdir(sorted_chunk_directory)):
        if bunchcol[:-8] != sortcol:
            if "level" in filename:
                with open(sorted_chunk_directory + "/" + filename) as sortread:
                    sorted_chunk = json.load(sortread)
                for k, v in sorted_chunk.items():
                    final_sorted_doc[k] = v
        else:
            if ("sorted_on_bunch" in filename) and (which_agg.lower() in filename):
                with open(sorted_chunk_directory + "/" + filename) as sortread:
                    sorted_chunk = json.load(sortread)
                for k, v in sorted_chunk.items():
                    final_sorted_doc[k] = v
    return final_sorted_doc

def sort_merge(user_query_list, sortcol):
    doc1 = user_query_list[0]
    doc2 = user_query_list[list(map(str.upper, user_query_list)).index("MERGE") + 1]
    directory1 = "./" + doc1 + "_chunks"
    directory2 = "./" + doc2 + "_chunks"
    if not os.path.exists(directory1 + "/merged_docs"):
        os.mkdir(directory1 + "/merged_docs")
    merged_directory1 = directory1 + "/merged_docs"
    if not os.path.exists(directory2 + "/merged_docs"):
        os.mkdir(directory2 + "/merged_docs")
    merged_directory2 = directory2 + "/merged_docs"
    for filename in os.listdir(directory1):
        if os.path.isfile(directory1 + "/" + filename) and filename[0] != ".":
            with open(directory1 + "/" + filename) as newread:
                newdoc = json.load(newread)
            newdoc = simple_sort(sortcol, newdoc)
            newdoc = json.dumps(newdoc, indent=1)
            with open(merged_directory1 + "/" + filename, "w") as outfile:
                outfile.write(newdoc)
            newdoc.to_csv(merged_directory1 + "/" + filename)
    for filename in os.listdir(directory2):
        if os.path.isfile(directory2 + "/" + filename) and filename[0] != ".":
            with open(directory2 + "/" + filename) as newread2:
                newdoc2 = json.load(newread2)
            newdoc2 = simple_sort(sortcol, newdoc2)
            newdoc2 = json.dumps(newdoc2, indent=1)
            with open(merged_directory2 + "/" + filename, "w") as outfile:
                outfile.write(newdoc2)
    match_dict = {}
    count = 0
    for left_chunk in os.listdir(merged_directory1):
        for right_chunk in os.listdir(merged_directory2):
            if os.path.isfile(merged_directory1 + "/" + left_chunk) and os.path.isfile(merged_directory2 + "/" + right_chunk) and left_chunk[0] != "." and right_chunk[0] != ".":
                if "merged" not in left_chunk and "merged" not in right_chunk:
                    with open(directory1 + "/" + left_chunk) as leftread:
                        left = json.load(leftread)
                    with open(directory2 + "/" + right_chunk) as rightread:
                        right = json.load(rightread)
                    for i1 in range(len(left)):
                        appdata1 = left[i1]
                        if appdata1 is None:
                            continue
                        matchval = appdata1[sortcol]
                        for i2 in range(len(right)):
                            appdata2 = right[i2]
                            if appdata2 is None:
                                continue
                            if appdata2[sortcol] == matchval:
                                match_dict[count] = {i1: appdata1, i2: appdata2}
                                count += 1
                                right[i2] = None
                                break
                        left[i1] = None
    out = json.dumps(match_dict, indent=1)
    with open(merged_directory1 + "/" + doc1 + "_merged.csv") as outfile1:
        outfile1.write(out)
    with open(merged_directory2 + "/" + doc2 + "_merged.csv") as outfile2:
        outfile2.write(out)
    return out

def sort_within_chunks(user_query_list, sortcol, directory):
    sorted_chunk_directory = directory + "/sorted_chunks"
    if not os.path.exists(sorted_chunk_directory):
        os.mkdir(sorted_chunk_directory)
    for chunk in os.listdir(directory):
        with open(directory + "/" + chunk) as docread:
            doc = json.load(docread)
        doc_keys = list(doc.keys())
        doc_keys.sort()
        sorted_doc = {i: doc[i] for i in doc_keys}
        sorted_doc = json.dumps(sorted_doc, indent=1)
        with open(sorted_chunk_directory + "/" + chunk[:-5] + "_sorted.json", "w") as outfile:
            outfile.write(sorted_doc)
    return directory + "/sorted_chunks"

def sort_between_chunks(user_query_list, sortcol, directory):
    docname = user_query_list[0]
    if not os.path.exists("./" + docname + "_chunks/chunk_subsets"):
        os.mkdir("./" + docname + "_chunks/chunk_subsets")
    for filename in os.listdir(directory):
        if os.path.isfile(directory + "/" + filename) and filename[0] != ".":
            with open(directory + "/" + filename) as fileread:
                file_subset = json.load(fileread)
            file_subset_name = f"{filename.split('.')[0]}_subset.json"
            file_subset_path = os.path.join("./"+ docname + "_chunks/chunk_subsets", file_subset_name)
            out = json.dumps(file_subset)
            with open(file_subset_path, "w") as outfile:
                outfile.write(out)
    subset_files = os.listdir("./"+ docname + "_chunks/chunk_subsets")
    while len(subset_files) > 1:
        with open("./"+ docname + "_chunks/chunk_subsets" + subset_files.pop(0)) as chunk1read:
            chunk1 = json.load(chunk1read)
        with open("./"+ docname + "_chunks/chunk_subsets" + subset_files.pop(0)) as chunk2read:
            chunk2 = json.load(chunk2read)
        sorted_merged = merge_asc(chunk1.values.tolist(), chunk2.values.tolist(), sortcol)
        sorted_merged = json.dumps(sorted_merged, indent=1)
        with open("./"+ docname + "_chunks/chunk_subsets", f"merged_subset_{len(subset_files) + 1 }.json") as outfile:
            outfile.write(sorted_merged)
        subset_files.append(f"merged_subset_{len(subset_files) + 1 }.json")
    with open(os.path.join("./"+ docname + "_chunks/chunk_subsets", subset_files[0])) as finalread:
        final_merged_doc = json.load(finalread)
    return final_merged_doc

def merge_sort(doc_list, sortcol): 
    if len(doc_list) == 1:
        return doc_list
    mid = len(doc_list)//2
    left = merge_sort(doc_list[:mid], sortcol)
    right = merge_sort(doc_list[mid:], sortcol)
    sorted_doc = merge_asc(left, right, sortcol)
    return sorted_doc

def merge_asc(left, right, sortcol): 
    sorted_doc = []
    i = 0 
    j = 0 
    while i < len(left) and j < len(right):
        if left[i][1][sortcol] < right[j][1][sortcol]:
            sorted_doc.append(left[i])
            i = i + 1 
        else: 
            sorted_doc.append(right[j])
            j = j + 1 
    while i < len(left): 
        sorted_doc.append(left[i])
        i = i + 1 
    while j < len(right): 
        sorted_doc.append(right[j])
        j = j + 1
    sorted_doc = {i[0]: i[1] for i in sorted_doc}
    return sorted_doc

def has(user_query_list):
    uqlupper = list(map(str.upper, user_query_list))
    agglist = ["TOTALNUM", "SUM", "MEAN", "MIN", "MAX"]
    if "MERGE" in uqlupper:
       doc = has_logic(user_query_list, "merged_docs")
    elif "SORT" in uqlupper:
        doc = has_logic(user_query_list, "chunk_subsets")
    elif "BUNCH" in uqlupper:
        if not set(agglist).isdisjoint(set(list(map(str.upper, user_query_list)))):
            doc = has_logic(user_query_list, "bunch_agg_chunks")
        else:
            doc = has_logic(user_query_list, "bunched_chunks")
    elif not set(agglist).isdisjoint(set(list(map(str.upper, user_query_list)))):
        if "COLUMNS" in uqlupper:
            doc = has_logic(user_query_list, "col_agg")
        else:
            doc = has_logic(user_query_list, "agg")
    else:
        doc = has_logic(user_query_list, "")
    return doc

def has_logic(user_query_list, directory):
    uqlupper = list(map(str.upper, user_query_list))
    condidx = uqlupper.index("HAS") + 1
    full_condition = ""
    if condidx != len(uqlupper) - 1:
        conds = user_query_list[condidx:]
        for c in conds:
            full_condition += " "
            full_condition += c
    else:
        conds = user_query_list[condidx]
        full_condition = conds
    condition = full_condition.strip().replace("\"", "").replace("\'", "")
    for chunk in os.listdir("./" + user_query_list[0] + "_chunks/" + directory):
        if os.path.isfile("./" + user_query_list[0] + "_chunks/" + directory + "/" + chunk) and chunk[0] != ".":
            if "MERGE" in uqlupper and "merged" in chunk:
                with open("./" + user_query_list[0] + "_chunks/" + directory + "/" + chunk) as docread:
                    doc = json.load(docread)
            elif "MERGE" not in uqlupper:
                with open("./" + user_query_list[0] + "_chunks/" + directory + "/" + chunk) as docread:
                    doc = json.load(docread)
            if "<" in condition:
                cond = condition.split("<")
                col1 = cond[0]
                cond2 = cond[1]
                if col1 not in list(doc.values())[0]:
                    return 
                if cond2 not in list(doc.values())[0]:
                    type1 = type(list(doc.items())[0][1][col1])
                    if '.' in cond2:
                        cond2 = float(cond2)
                    elif cond2.isdigit():
                        cond2 = int(cond2)
                    type2 = type(cond2)
                    if type1 == type2: 
                        doc = {k: v for k, v in doc.items() if v[col1] < cond2}
                    else:   
                        if isinstance(type1, str) or isinstance(type2, str):
                            print("Incompatible type comparison")
                            return
                        else:
                            doc = {k: v for k, v in doc.items() if v[col1] < cond2}
                else:
                    doc = {k: v for k, v in doc.items() if v[col1] < v[cond2]}
            elif ">" in condition:
                cond = condition.split(">")
                col1 = cond[0]
                cond2 = cond[1]
                if col1 not in list(doc.values())[0]:
                    return 
                if cond2 not in list(doc.values())[0]:
                    type1 = type(list(doc.items())[0][1][col1])
                    if '.' in cond2:
                        cond2 = float(cond2)
                    elif cond2.isdigit():
                        cond2 = int(cond2)
                    type2 = type(cond2)
                    if type1 == type2: 
                        doc = {k: v for k, v in doc.items() if v[col1] > cond2}
                    else:   
                        if isinstance(type1, str) or isinstance(type2, str) or isinstance(type1, date) or isinstance(type2, date):
                            print("Incompatible type comparison")
                            return
                        else:
                            doc = {k: v for k, v in doc.items() if v[col1] > cond2}
                else:
                    doc = {k: v for k, v in doc.items() if v[col1] > v[cond2]}
            elif "=" in condition:
                cond = condition.split("=")
                col1 = cond[0]
                cond2 = cond[1]
                if col1 not in list(doc.values())[0]:
                    return 
                if cond2 not in list(doc.values())[0]:
                    type1 = type(list(doc.items())[0][1][col1])
                    if '.' in cond2:
                        cond2 = float(cond2)
                    elif cond2.isdigit():
                        cond2 = int(cond2)
                    type2 = type(cond2)
                    if type1 == type2: 
                        doc = {k: v for k, v in doc.items() if v[col1] == cond2}
                    else:   
                        if isinstance(type1, str) or isinstance(type2, str) or isinstance(type1, date) or isinstance(type2, date):
                            print("Incompatible type comparison")
                            return
                        else:
                            doc = {k: v for k, v in doc.items() if v[col1] == cond2}
                else:
                    doc = {k: v for k, v in doc.items() if v[col1] == v[cond2]}
        else:
            continue
        if not os.path.exists("./" + user_query_list[0] + "_chunks/" + directory + "/has_chunks"):
            os.mkdir("./" + user_query_list[0] + "_chunks/" + directory + "/has_chunks")
        out = json.dumps(doc, indent=1)
        with open("./" + user_query_list[0] + "_chunks/" + directory + "/has_chunks/"+ user_query_list[0] + "_has.json", "w") as outfile:
            outfile.write(out)
        return doc