#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 25 19:45:29 2020

@author: lvardges
"""


import psycopg2
from db.config import config

def updateRequestedFix(transcriptid, num):
    """ UPDATE transcript request info in transcriptsinfo table  """

    
    sql_query = f"""update transcriptsinfo
    set requestedfix={num}, requestedfixtime = NOW()
    where transcriptid = {transcriptid}"""

    
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the SELECT statement
        cur.execute(sql_query)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
