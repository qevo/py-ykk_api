from json import dumps
import sqlite3
import cherrypy
from db import DLI

DB_INPUTS = [
    dict(table= "deaths", fpath= "../data/deaths.json")
]
DB_FIELDS = ('id', 'medium', 'title','description', 'image')

# returns the tables used by the model
def db_tables() -> dict:
    tables = dict()
    tables["deaths"] = dict()
    tables["deaths"]["name"] = "deaths"
    tables["deaths"]["fields"] = DB_FIELDS
    tables["deaths"]["types"] = ('INTEGER', 'TEXT', 'TEXT', 'TEXT', 'TEXT')
    tables["deaths"]["constraints"] = ("PRIMARY KEY AUTOINCREMENT", "NOT NULL", "NOT NULL", "NOT NULL", "NOT NULL")
    return tables

# returns the inputs used by the model
def db_inputs() -> list:
    return DB_INPUTS

class Death:
    '''Class Death defines instances when Kenny has died'''
    
    # holder for the data layer
    dli = None

    # table used by the model
    table_name = "deaths"

    def __init__(self, dli = None, id = -1):
        self.id = id
        self.medium = ""
        self.title = ""
        self.description = ""
        self.image = ""
        self.isStored = False
        if dli:
            self.setDLI(dli)
        if id > 0:
            self.fetch(id)            
    
    # set the data layer
    def setDLI(self, dli) -> None:
        self.dli = dli

    # returns a dict of the object
    def to_dict(self) -> dict:
        d = dict()
        for f in DB_FIELDS:
            d[f] = getattr(self, f)
        return d

    # returns a JSON string
    def toJson(self) -> str:
        return dumps(self.__str__())

    # returns a bytes object of the encoded JSON string
    def toJsonBytes(self) -> bytes:
        return bytes(self.toJson())

    # returns a string representation of the object
    # for sqlite3
    def __conform__(self, protocol) -> (str | None):
        if protocol is sqlite3.PrepareProtocol:
            return str(self.to_dict())

    # returns a string representation of the object
    def __repr__(self) -> str:
        return str(self.to_dict())

    # returns a string representation of the object
    def __str__(self) -> str:
        return str(self.to_dict())

    # update the object attributes from the data passed
    def setData(self, data: dict):
        self.id = data.get("id", -1)
        self.medium = data.get("medium")
        self.title = data.get("title")
        self.description = data.get("description")
        self.image = data.get("image")
        return self
    
    # returns the object with the given id or None
    def fetch(self, id):
        data = self.dli.fetch(self.table_name, id, "id")
        
        # no results, or too many results
        if data == None:
            return None

        # using map and dict type casting
        # to convert lists to dictionary
        res = dict(map(lambda i,j : (i,j) , DB_FIELDS, data))
        
        # get the appropriate death object for the record
        fresh = _get_death_obj(self.dli, res)
        return fresh

    # returns a list of objects or an empty list if none
    def fetchall(self, where = None) -> list:
        res = self.dli.fetchall(self.table_name, where)

        if len(res) == 0:
            return []

        death_list = []
        for death in res:
            # using map and dict type casting
            # to convert lists to dictionary
            dd = dict(map(lambda i,j : (i,j) , DB_FIELDS, death))
            # get the appropriate death object for the record
            do = _get_death_obj(self.dli, dd)
            death_list.append(do)
        return death_list

class DeathTV(Death):
    '''Extend Death to add some TV-specific attributes'''
    
    def __init__(self, dli = None, id = -1) -> None:
        self.season = ""
        super().__init__()
        
    def setData(self, data):
        self.season = data.get("season", "")
        return super().setData(data)

class DeathREST:
    '''DeathREST defines the routes used for Death'''

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def index(self):
        json_body = []
        o = Death(dli=DLI)
        all = o.fetchall()
        for death in all:
            json_body.append(death.toJson())
        return json_body

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def default(self, id):
        id = int(id)
        o = Death(dli=DLI)
        result = []
        death = o.fetch(id)
        result.append(death.toJson())
        return result

# returns the appropriate death object for the data
def _get_death_obj(dli, data: dict) -> (Death | DeathTV):
    if data["medium"] == "Television":
        do = DeathTV(dli=dli)
    else:
        do = Death(dli=dli)
            
    do.setData(data)
    do.isStored = True
    return do