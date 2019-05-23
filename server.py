import time, sys, cherrypy, os
import findspark
import logging
findspark.init()
from paste.translogger import TransLogger
from app import create_app
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


# Create Spark Session
spark = SparkSession.builder.appName("Recommendation Engine").getOrCreate()

def run_server(app):
    # Enable WSGI access logging via Paste
    app_logged = TransLogger(app)

    # Mount the WSGI callable object (app) on the root directory
    cherrypy.tree.graft(app_logged, '/')

    # Set the configuration of the web server
    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 5432,
        'server.socket_host': '0.0.0.0'
    })

    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    print("Recomender Engine is running on localhost port 5432!")
    cherrypy.engine.block()


if __name__ == "__main__":
    # IMPORTANT: pass aditional Python modules to each worker
    enginepyloc = os.path.join(os.getcwd(), "engine.py")
    print(enginepyloc)
    apppyloc = os.path.join(os.getcwd(), "app.py")
    print(apppyloc)
    spark.sparkContext.addPyFile(enginepyloc)
    spark.sparkContext.addPyFile(apppyloc)

    #set dataset location
    dataset_path = os.path.join(os.path.dirname(os.getcwd()), 'ml-1m')
    print (dataset_path)
    app = create_app(spark, dataset_path)

    # start web server
    run_server(app)

