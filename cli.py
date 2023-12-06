import cmd
import os
from queryparse import parse_query

class CLI(cmd.Cmd):
    intro = """\nWelcome to Roma Jay Data Base (RJDB) :D\n\nMake sure to USEDB before you try MAKE, EDIT, FETCH, or DROP!\nType \'help\' or \'help [command]\' without quotes to learn more about the query language.\n"""
    prompt = 'RJDB > '
    
    def __init__(self, current_db=None):
        super(CLI, self).__init__()
        self.current_db = current_db

    def onecmd(self, line):
            """Interpret the argument as though it had been typed in response
            to the prompt.

            This may be overridden, but should not normally need to be;
            see the precmd() and postcmd() methods for useful execution hooks.
            The return value is a flag indicating whether interpretation of
            commands by the interpreter should stop.

            """
            cmd, arg, line = self.parseline(line)
            if not line:
                return self.emptyline()
            if cmd is None:
                return self.default(line)
            self.lastcmd = line
            if line == 'EOF' :
                self.lastcmd = ''
            if cmd == '':
                return self.default(line)
            else:
                try:
                    func = getattr(self, 'do_' + cmd)
                except AttributeError:
                    try:
                        func = getattr(self, 'do_' + cmd.lower())
                    except AttributeError:
                        return self.default(line)
                return func(arg)

    def do_makedb(self, user_input_query):
        '''
        Use this keyword at the beginning of a query to
        create a new database.

        Syntax: MAKEDB REL/NOSQL dbname
        ''' 
        user_input_query = user_input_query.strip().split(" ")
        if self.current_db is not None:
            if user_input_query[0].upper() == "REL":
                os.chdir("..")
                os.chdir("../DSCI-551-Final-Proj-Rel")
            elif user_input_query[0].upper() == "NOSQL":
                os.chdir("..")
                os.chdir("../DSCI-551-Final-Proj-NoSQL")
        else:
            if user_input_query[0].upper() == "REL":
                os.chdir("../DSCI-551-Final-Proj-Rel")
            elif user_input_query[0].upper() == "NOSQL":
                os.chdir("../DSCI-551-Final-Proj-NoSQL")
        if os.path.exists(user_input_query[1]):
            print("DB name already exists! Please use the existing DB or make a DB with a different name.")
        else:
            os.mkdir("./" + user_input_query[1])
            if user_input_query[0].upper() == "REL":
                print("Created relational DB ", user_input_query[1])
            elif user_input_query[0].upper() == "NOSQL":
                print("Created non-relational DB ", user_input_query[1])
        
    def do_usedb(self, user_input_query):
        """
        Use this keyword at the beginning of a query to
        use an existing database.

        Syntax: USEDB REL/NOSQL dbname
        """
        user_input_query = user_input_query.strip().split(" ")
        if self.current_db is not None:
            if user_input_query[0].upper() == "REL":
                os.chdir("..")
                os.chdir("./DSCI-551-Final-Proj-Rel")
            elif user_input_query[0].upper() == "NOSQL":
                os.chdir("..")
                os.chdir("./DSCI-551-Final-Proj-NoSQL")
            if not os.path.exists(user_input_query[1]):
                print("DB does not exist!")
        else:
            os.chdir("./" + user_input_query[1])
        self.current_db = user_input_query[1]
        print("Using DB " + user_input_query[1])

    def do_dropdb(self, user_input_query):
        """
        Use this keyword at the beginning of a query to
        drop an existing database.

        Syntax: DROPDB REL/NOSQL dbname
        """
        user_input_query = user_input_query.strip().split(" ")
        if user_input_query[1] == self.current_db:
            self.current_db = None
            if user_input_query[0].upper() == "REL":
                os.chdir("..")
                os.chdir("./DSCI-551-Final-Proj-Rel")
            elif user_input_query[0].upper() == "NOSQL":
                os.chdir("..")
                os.chdir("./DSCI-551-Final-Proj-NoSQL")
        elif self.current_db is not None:
            if user_input_query[0].upper() == "REL":
                os.chdir("..")
                os.chdir("./DSCI-551-Final-Proj-Rel")
            elif user_input_query[0].upper() == "NOSQL":
                os.chdir("..")
                os.chdir("./DSCI-551-Final-Proj-NoSQL")
        else:
            if user_input_query[0].upper() == "REL":
                os.chdir("..")
                os.chdir("./DSCI-551-Final-Proj-Rel")
            elif user_input_query[0].upper() == "NOSQL":
                os.chdir("..")
                os.chdir("./DSCI-551-Final-Proj-NoSQL")
        if user_input_query[1] not in os.listdir('.'):
            print("DB does not exist!")
            return
        else:
            for filename in os.listdir("./" + user_input_query[1]):
                os.remove("./" + user_input_query[1] + filename)
            os.rmdir("./" + user_input_query[1])
            if user_input_query[0].upper() == "REL":
                print("Dropped relational DB", user_input_query[1])
            elif user_input_query[0].upper() == "NOSQL":
                print("Dropped non-relational DB", user_input_query[1])


    def do_showdb(self, user_input_query):
        """
        Use this keyword at the beginning of a query to
        show all databases in the DBMS. Do this BEFORE using
        the USEDB command.

        Syntax: SHOWDB REL/NOSQL
        """
        if user_input_query.upper() == "REL":
            os.chdir("..")
            os.chdir("./DSCI-551-Final-Proj-Rel")
        elif user_input_query.upper() == "NOSQL":
            os.chdir("..")
            os.chdir("./DSCI-551-Final-Proj-NoSQL")
        for filename in os.listdir("."):
            if os.path.isdir("./" + filename) and not filename.startswith("_") and not filename.startswith(".") and not filename=="table":
                print(filename)

    def do_make(self, user_input_query):
        """
        Use this keyword at the beginning of a query
        to make a new table/document within the current database.

        Relational:
            Syntax for MAKE (new table): 
                MAKE tablename COLUMNS col1=dtype1, col2=dtype2, col3=dtype3...
            Syntax for MAKE COPY (existing table):
                MAKE COPY tablename1 tablename2

        NoSQL:
            Syntax for MAKE (new document): 
                MAKE docname NODES node1=dtype1, node2=dtype2, node3=dtype3...
            Syntax for MAKE COPY (existing document):
                MAKE COPY docname jsonname2

        * Supported datatypes are int, float, and str.
        * Relational db also supports datetime64.
        * You MUST type datatypes exactly as written here. No quotes.
        * If using MAKE COPY, do not use COLUMNS/NODES. 
        * MAKE COPY will only copy the table/document's schema, not its contents.

        """
        user_input_query = "MAKE " + user_input_query
        return parse_query(user_input_query, self.current_db)
        
    def do_edit(self, user_input_query):
        """
        Use this keyword at the beginning of a query
        to edit an existing table within the current database,
        including insert, update, and delete operations.

        General syntax: 
            EDIT tablename INSERT/UPDATE/DELETE record

        Syntax for INSERT (one record at a time): 
            EDIT tablename/docname INSERT col1/node1=x, col2/node2=y, col3/node3=z...
        Syntax for INSERT (whole file): 
            EDIT tablename/docname INSERT FILE filepath
        Syntax for UPDATE: 
            EDIT tablename/docname UPDATE id=rownum, col3=abc, col5=xyz... 
        Syntax for DELETE: 
            EDIT tablename/docname DELETE id=rownum...

        * When using INSERT FILE, do not wrap the filepath in quotes.
        """
        user_input_query = "EDIT " + user_input_query
        return parse_query(user_input_query, self.current_db)
        
    
    def do_fetch(self, user_input_query): 
        """
        Use this keyword at the beginning of a query to 
        fetch existing table and perform aggregation, bunching, 
        filtering, sorting, or merging operations on specified columns.

        Relational:
            Syntax: FETCH tablename [COLUMNS] [column(s)] [AGGREGATION FUNCTION] [column] [BUNCH/SORT] [column] [MERGE] [tablename2] [INCOMMON] [mergingcol] [HAS] [column(s)]

        NoSQL:
            Syntax: FETCH docname [NODES] [node(s)] [AGGREGATION FUNCTION] [node] [BUNCH/SORT] [node] [MERGE] [docname2] [INCOMMON] [mergingnode] [HAS] [node(s)]

        * Brackets indicate optional query parameters.
        * Possible aggregation functions are TOTALNUM, SUM, MEAN, MIN, and MAX.
        * Keywords should specifically be in the order of 
          COLUMNS/NODES --> TOTALNUM/SUM/MEAN/MIN/MAX --> BUNCH --> SORT --> MERGE --> HAS
        """
        user_input_query = "FETCH " + user_input_query
        return parse_query(user_input_query, self.current_db)
        
    def do_drop(self, user_input_query): 
        """
        Use this keyword at the beginning of a query to
        drop a table from the current database.

        Syntax: DROP tablename
        """
        user_input_query = "DROP " + user_input_query
        return parse_query(user_input_query, self.current_db)
    
    def do_show(self, user_input_query):
        """
        Use this keyword to show all tables in the current DB.
        
        * Make sure you're in a DB first, using USEDB. 
        """
        if "Rel" in os.getcwd():
            for filename in os.listdir("./table"):
                print("       ", filename[:-4])
        elif "NoSQL" in os.getcwd():
            for filename in os.listdir("./document"):
                print("       ", filename[:-5])

    def do_exit(self, *args):
        """
        Use this keyword by itself to exit the command line interface.
        """
        print("\nBye\n")
        return True

if __name__ == "__main__": 
    CLI().cmdloop()