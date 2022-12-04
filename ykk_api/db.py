import json
import sqlite3

DB_TYPE = "sqlite3"
DB_NAME = "ykk.sqlite3"
# DLI singleton is set at the bottom

# models must register the data input sources
registered_inputs = dict()
def register_inputs(inputs) -> None:
    # TODO: add ability for input sources to be remote
    for item in inputs:
        registered_inputs[item["table"]] = item["fpath"]

# models must register any table structure they will need
registered_tables = dict()
def register_tables(tables) -> None:
    for k in tables:
        registered_tables[k] = tables[k]

# returns if the database has all the given table names
def has_tables(tables = registered_tables) -> bool:
    for k in tables:
        if not _has_table(k):
            return False
    return True

# returns if the database has the given table name
def _has_table(table_name) -> bool:
    cx = sqlite3.connect(DB_NAME, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cu = cx.cursor()
    cu.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='"+table_name+"'")
    # if the count is 1, then table exists
    exists = True if (cu.fetchone())[0] == 1 else False
    cu.close()
    cx.close()
    return exists

# create tables using the given table definitions
def create_tables(tables = registered_tables) -> None:
    for table in tables:
        _create_table(tables[table])

# create a table in the database
def _create_table(table) -> None:
    statement = _make_create_table(table)
    cx = sqlite3.connect(DB_NAME, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cu = cx.cursor()
    cu.execute(statement)
    cu.close()
    cx.close()

# returns a sql command to create a table
def _make_create_table(table, if_not_exists = True, without_rowid = False) -> str:
    columns = []
    statement = "CREATE TABLE "
    statement += "IF NOT EXISTS " if if_not_exists else ""
    statement += table["name"] + " ("
    for i in range(len(table["fields"])):
        columns.append(table["fields"][i] + " " + table["types"][i] + " " + table["constraints"][i])
    statement += ",".join(columns)
    statement += ") "
    statement += "WITHOUT ROWID" if without_rowid else ""
    statement += ";"
    return statement

# drop each table from the list of names
def drop_tables(tables = registered_tables) -> None:
    for table_name in tables:
        _drop_table(table_name)

# drop a table from the database
def _drop_table(table_name) -> None:
    statement = _make_drop_table(table_name)
    cx = sqlite3.connect(DB_NAME, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cu = cx.cursor()
    cu.execute(statement)
    cu.close()
    cx.close()

# returns a sql command to drop a table
def _make_drop_table(table_name) -> str:
    statement = "DROP TABLE IF EXISTS "+table_name+";"
    return statement

# returns a dict of JSON data found in each given input
def read_inputs(inputs = registered_inputs) -> dict:
    data = dict()
    
    for table_name in inputs:
        # use .update() to overwrite any existing table_name:values
        data.update(_read_input(inputs[table_name])) # fpath
    return data

# returns a dict of a JSON file
def _read_input(fpath) -> dict:
    with open(fpath, 'r') as f:
        data = json.load(f)
    return data

# returns a sql values string that combines all rows to be inserted
def _parse_entries(entries, columns) -> str:
    # hold all the values used in the later sql insert
    values = []
    for entry in entries:
        
        # hold the values for the given entry
        temp_values = []
        
        # string of the values for the given entry
        entry_values = ""
        
        # loop through the columns to ensure the proper order of the entry values
        for c in columns:
            if c != 'id': # avoid using any old id and avoid any errors if none are present
                # in the read/parse processes quotation escaping gets out of hand,
                # so there is a replace to help
                temp_values.append("\'" + entry[c].replace("\'", "") + "\'")
        
        # parse the entry values
        entry_values = ",".join(temp_values)
        entry_values = wraps(entry_values, "(", ")", 1)
        
        # add the entry to the list of values
        values.append(entry_values)
    
    # add a newline between entry values to avoid a line that is too long
    val_str = ",\n".join(values)
    return val_str

# returns the given string with the characters added 
# to the beginning and end of a string from 1 to n times
def wraps(s, open, close, n = 1) -> str:
    if n > 1:
        s = wraps(s,open,close,n-1)
    return open + s + close

# returns a JSON string representation of the given object
def toJson(o) -> str:
    try: # attempt the ideal approach
        val = json.dumps(o)
        return val
    except: # ignore any errors
        pass
    
    # common object method names used to get a string representation
    stringify_methods   = ["__str__", "__repr__", "toJson", "to_Json"]
    iterable_methods = ["to_dict"]

    # holder for the handler method
    handler = None

    # loop through the different method names
    for method in stringify_methods+iterable_methods:
        # if the object has the method
        if hasattr(o, method):
            # store the method
            handler = getattr(o, method)
            # stop looking
            break
    
    # if a handler was found
    if handler:
        # get the string representation
        val = handler()
        # return an attempt at JSON.dumps() which may error
        return json.dumps(val)
    
    # raise an error that the object cannot be serialized to JSON
    else:
        raise TypeError("Object of type "+ type(o) + " cannot be serialized to JSON.")

# returns a bytes object of the encoded JSON string
def toJsonBytes(o) -> bytes:
    s = toJson(o)
    return bytes(s, 'utf-8')

# data_layer helps keep the logic used by models consistent
# by managing database interactions
class data_layer(object):

    def __init__(self) -> None:
        # use a singleton pattern as there is no
        # need for more than one instance
        DLI = self

    def bootstrap(self) -> None:
        # start with clean tables
        drop_tables(registered_tables)
        create_tables()
        
        # get the input data
        data = read_inputs(registered_inputs)
        
        for table_name in data:
            # use the registered table fields for the given table
            # to get values the JSON input data
            values = _parse_entries(data[table_name], registered_tables[table_name]["fields"])

            # use the registered table fields except for the
            # first one (which is id), and insert values 
            self.insert(table_name, ",".join(registered_tables[table_name]['fields'][1:]), values)
        
    # executes the sql statement and optionally returns a list of the results
    def execute(self, query, ret = True) -> (list | None):
        conn = sqlite3.connect(DB_NAME, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        cur = conn.cursor()
        cur = cur.execute(query)
        
        if ret == True: # results are requested
            results = cur.fetchall()
        
        else: # changes are requested
            conn.commit()
        
        cur.close()
        conn.close()
        if ret == True:
            return results
    
    # construct and execute an insert statement
    def insert(self, table_name, columns, values) -> None:
        statement = "INSERT INTO " + table_name + " (" + columns + ") VALUES "
        statement += values + ";"
        self.execute(statement, False)

    # returns the record with the given id or None
    def fetch(self, table_name, id, col = "id") -> (list | None):
        query = "SELECT * FROM " + table_name + " WHERE " + col + " = " + str(id) + ";"
        o = self.execute(query)
        
        if len(o) != 1:
            return None
        return o[0]
    
    # returns a list of query results or an empty list if none found
    def fetchall(self, table_name, where = None) -> (list):
        query = "SELECT * FROM " + table_name
        if where:
            query += " WHERE " + where + ";"
        results = self.execute(query)
        return results

# store the class as a singleton
DLI = data_layer()
