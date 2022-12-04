import cherrypy
from models.death import DeathREST


class Root(object):
    @cherrypy.expose
    def index(self):
        return "Hello World!"
    def status(self):
        pass

def bootstrap():
    cherrypy.tree.mount(Root(), '/') # server.conf was already passed
    cherrypy.tree.mount(DeathREST(), '/deaths', "app.conf")
