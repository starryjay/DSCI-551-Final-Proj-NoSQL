import pandas as pd
import os
from datetime import date

# Datatypes to account for: int, str, float, datetime64
global nodenames
global dtypes 

def make(user_query_list):
    if user_query_list[0].upper() == "COPY":
        return make_copy(user_query_list[1:])
    elif user_query_list[1].upper() == "NODES":
        tablename = user_query_list[0]
        for char in tablename:
            if char.isdigit():
                print("Please use a tablename without any numbers in it.")
                return
        if tablename in os.listdir(os.getcwd()):
            print("Table with name", tablename, "already exists! Please use a different name.")
            return
        nodestuples = [tuple(data.split('=')) for data in user_query_list[2:]]
        global nodenames
        nodenames = [nodestuples[i][0] for i in range(len(nodestuples))]
        global dtypes 
        dtypes = [nodestuples[i][1].lower() for i in range(len(nodestuples))]
        doc = {nodename: 0 for nodename in nodenames}
        for nodename, datatype in zip(nodenames, dtypes):
            # iterate thru, convert all values to appropriate dtypes
            continue # placeholder
        if not os.path.exists("./table"):
            os.mkdir("./table")
        print("Successfully created document", tablename, "with nodes", nodenames, "and datatypes", dtypes)
    else:
        print("Please use keyword COPY or COLUMNS!")
        return
def make_copy(user_query_list):
    existingtable = user_query_list[0]
    copytable = user_query_list[1]
    if copytable in os.listdir(os.getcwd()):
        print("Table with name", copytable, "already exists! Please use a different name.")
        return
    curr_table = pd.read_pickle("./table/" + existingtable + '.pkl')
    copy = curr_table.copy(deep=False)
    copy.to_pickle("./table/" + copytable + '.pkl')
    print("Successfully created copy of table", existingtable, "called", copytable, "with columns", nodenames, "and datatypes", dtypes)


    """
{
    "Photo Editor & Candy Camera & Grid & ScrapBook": {
        "Category": "ART_AND_DESIGN",
        "Rating": 4.1,
        "Reviews": "159",
        "Size": "19M",
        "Installs": "10,000+",
        "Type": "Free",
        "Price": "0",
        "Content Rating": "Everyone",
        "Genres": "Art & Design",
        "Last Updated": "January 7, 2018",
        "Current Ver": "1.0.0",
        "Android Ver": "4.0.3 and up"
    },
    "Coloring book moana": {
        "Category": "ART_AND_DESIGN",
        "Rating": 3.9,
        "Reviews": "967",
        "Size": "14M",
        "Installs": "500,000+",
        "Type": "Free",
        "Price": "0",
        "Content Rating": "Everyone",
        "Genres": "Art & Design;Pretend Play",
        "Last Updated": "January 15, 2018",
        "Current Ver": "2.0.0",
        "Android Ver": "4.0.3 and up"
    }
}   
    """