from make_doc import make
from edit_doc import edit
from fetch_doc import fetch
from drop_doc import drop

def parse_query(user_input_string, current_db):
    user_input_list = user_input_string.replace(',', '').split()
    first_keyword = user_input_list[0] 
    if first_keyword.upper() == "MAKE": 
        return make(user_input_list[1:])
    elif first_keyword.upper() == "EDIT": 
        return edit(user_input_list[1:], current_db)
    elif first_keyword.upper() == "FETCH": 
        return fetch(user_input_list[1:])
    elif first_keyword.upper() == "DROP":
        return drop(user_input_list[1:])
    else:
        print("Invalid query. If modifying databases, use MAKEDB, USEDB, or DROPDB. If modifying tables, use MAKE, EDIT, FETCH, or DROP.")