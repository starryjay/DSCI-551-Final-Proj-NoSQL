import os

def drop(user_input_query):
    tablename = user_input_query[0]
    filename = tablename + ".json"
    if os.path.exists("./document"):
        if filename in os.listdir("./document"):
            os.remove("./document/" + filename)
            print("Dropped table", tablename)
    if os.path.exists("./" + tablename + "_chunks"):
        if os.listdir("./" + tablename + "_chunks"):
            for fn in os.listdir("./" + tablename + "_chunks"):
                os.remove("./" + tablename + "_chunks/" + fn)
        os.rmdir("./" + tablename + "_chunks")