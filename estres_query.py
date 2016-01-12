#!/usr/bin/env python
#-*- coding: UTF-8 -*-

# autor: Carlos Rueda
# fecha: 2013-11-29
# mail: carlos.rueda@deimos-space.com

import os
import sys
import math
import haversine
import csv
import threading
import time
import datetime
import MySQLdb as mdb

import logging, logging.handlers

print ('Empezamos !')


s = None


########################################################################
# configuracion y variables globales
from configobj import ConfigObj
config = ConfigObj('./estres_query.properties')

LOG_FILE = config['directory_logs'] + "/estres_query.log"
LOG_FOR_ROTATE = 10

DB_FRONTEND_IP = config['mysql_host']
DB_FRONTEND_PORT = config['mysql_port']
DB_FRONTEND_NAME = config['mysql_db_name']
DB_FRONTEND_USER = config['mysql_user']
DB_FRONTEND_PASSWORD = config['mysql_passwd']

PID = "/var/run/estres_query/estres_query"

########################################################################

# definimos los logs internos que usaremos para comprobar errores
try:
    logger = logging.getLogger('estres_query')
    loggerHandler = logging.handlers.TimedRotatingFileHandler(LOG_FILE, 'midnight', 1, backupCount=10)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    loggerHandler.setFormatter(formatter)
    logger.addHandler(loggerHandler)
    logger.setLevel(logging.DEBUG)
except:
    print '------------------------------------------------------------------'
    print '[ERROR] Error writing log at %s' % LOG_FILE 
    print '[ERROR] Please verify path folder exits and write permissions'
    print '------------------------------------------------------------------'
    exit()


########################################################################
if os.access(os.path.expanduser(PID), os.F_OK):
        print "Checking if estres_query process is already running..."
        pidfile = open(os.path.expanduser(PID), "r")
        pidfile.seek(0)
        old_pd = pidfile.readline()
        # process PID
        if os.path.exists("/proc/%s" % old_pd) and old_pd!="":
            print "You already have an instance of the estres_query process running"
            print "It is running as process %s" % old_pd
            sys.exit(1)
        else:
            print "Trying to start estres_query process..."
            os.remove(os.path.expanduser(PID))

#This is part of code where we put a PID file in the lock file
pidfile = open(os.path.expanduser(PID), 'a')
print "estres_query process started with PID: %s" % os.getpid()
pidfile.write(str(os.getpid()))
pidfile.close()


########################################################################

class MiThread(threading.Thread):
    def __init__(self, num):  
        threading.Thread.__init__(self)  
        self.num = num  
  
    def run(self):  
        global s
        car = self.num
        print "Arrancando hilo", car
        
        con = None

        # calcular coordenadas


        # consulta
        try:

            con = mdb.connect(DB_FRONTEND_IP, DB_FRONTEND_USER, DB_FRONTEND_PASSWORD, DB_FRONTEND_NAME)
     
            cur = con.cursor()
            cur.execute("SELECT OGR_FID, (st_distance_sphere(SHAPE, POINT(-4.28, 41.02))) as distance FROM routes ORDER BY distance")
            numrows = int(cur.rowcount)
            for i in range(numrows):
                row = cur.fetchone()
                row_id = row[0]
                distancia = row[1]
                logger.debug("Distancia: " + distancia)

        except mdb.Error, e:
            logger.error ("Error %d: %s" % (e.args[0], e.args[1]))
            #sys.exit(1)

        finally:
            if con:
                con.close()

########################################################################


if len(sys.argv) == 1:
    print "--------------------------------------------------------"
    print "Este programa necesita parametros:"
    print " --> numero de consultas"
    print "Ejemplo: estres_query.sh 1000"
    exit()

if len(sys.argv) < 2:
    print "ERROR: Numero de parámetros incorrecto"
    exit()

consultas = sys.argv[1]

# connect to server
print ('Lanzando consultas')

#for i in range(1, 151):  
for i in range(1, int(consultas)): 
    time.sleep(0.1) 
    t = MiThread(i)  
    t.start()  
    #t.join() 

