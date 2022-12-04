import sys
import cherrypy
import db
import routes
from models import death

def bootstrap():
    # register tables used by models
    db.register_tables(death.db_tables())
    
    # register data sources used by models
    db.register_inputs(death.db_inputs())

    # start the data layer used by models
    db.DLI.bootstrap()
    
    # config http server
    cherrypy.config.update("server.conf")
    
    # set the http routes
    routes.bootstrap()
    
    # start the http server
    cherrypy.engine.start()
    cherrypy.engine.block()

def main():
    # TODO: parse cli values
    bootstrap()

# app is called directly
if __name__ == '__main__':
    # TODO: add cli options
    sys.exit(main())
