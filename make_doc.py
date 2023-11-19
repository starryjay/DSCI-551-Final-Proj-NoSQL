import os
import json

# Datatypes to account for: int, str, float, datetime64
global nodenames
global dtypes 

def make(user_query_list):
    if user_query_list[0].upper() == "COPY":
        return make_copy(user_query_list[1:])
    elif user_query_list[1].upper() == "NODES":
        docname = user_query_list[0]
        for char in docname:
            if char.isdigit():
                print("Please use a docname without any numbers in it.")
                return
        if docname in os.listdir(os.getcwd()):
            print("Document with name", docname, "already exists! Please use a different name.")
            return
        nodestuples = [tuple(data.split('=')) for data in user_query_list[2:]]
        global nodenames
        nodenames = [nodestuples[i][0] for i in range(len(nodestuples))]
        global dtypes 
        dtypes = [nodestuples[i][1].lower() for i in range(len(nodestuples))]
        doc = {nodename: 0 for nodename in nodenames}
        for nodename, datatype in zip(nodenames, dtypes):
            doc[nodename] = datatype
        if not os.path.exists("./document"):
            os.mkdir("./document")
        json_object = json.dumps(doc, indent=1)
        with open("./document/" + docname + ".json", "w") as outfile:
            outfile.write(json_object)
        print("Successfully created document", docname, "with nodes", nodenames, "and datatypes", dtypes)
    else:
        print("Please use keyword COPY or NODES!")
        return
def make_copy(user_query_list):
    existingdoc = user_query_list[0]
    copydoc = user_query_list[1]
    if copydoc in os.listdir(os.getcwd()):
        print("Document with name", copydoc, "already exists! Please use a different name.")
        return
    with open("./document/" + existingdoc + ".json") as infile:
        curr_doc = json.load(infile)
    copy = json.dumps(curr_doc, indent=1)
    with open("./document/" + copydoc + ".json", "w") as outfile:
        outfile.write(copy)
    print("Successfully created copy of table", existingdoc, "called", copydoc, "with nodes", list(curr_doc.keys()), "and datatypes", list(curr_doc.values()))


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