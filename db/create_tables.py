#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 13:41:19 2020

@author: revist
"""


import psycopg2
from db.config import config
 
 
def createTables():
    """Create tables in the PostgreSQL database"""
    commands = (      
        """
        CREATE TABLE sources (
                sourceid VARCHAR(20) PRIMARY KEY,
                sourcename VARCHAR(50) NOT NULL,
                databasename VARCHAR(20) NOT NULL
        )
        """,
        """
        CREATE TABLE transcriptsinfo (
            transcriptid SERIAL PRIMARY KEY,
            query VARCHAR(30) NOT NULL,
            sourceid VARCHAR(20) NOT NULL,
            FOREIGN KEY (sourceid)
                REFERENCES sources (sourceid)
                ON UPDATE CASCADE ON DELETE CASCADE,
            date DATE NOT NULL,
            time TIME,
            duration INTERVAL,
            transcripttitle TEXT,
            iscopy bool NOT NULL default FALSE,
            transcriptgroup BIGINT NOT NULL,
            hastranscript bool NOT NULL default TRUE,
            unavailablelink TEXT default NULL,
            unavailablereason TEXT default NULL
        )
        """,
        """
        CREATE TABLE transcriptspreprocessed (
            transcriptid BIGINT NOT NULL,
            FOREIGN KEY (transcriptid)
                REFERENCES transcriptsinfo (transcriptid)
                ON UPDATE CASCADE ON DELETE CASCADE,
            preprocessedtranscript TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE transcriptsraw (
            transcriptid BIGINT NOT NULL,
            FOREIGN KEY (transcriptid)
                REFERENCES transcriptsinfo (transcriptid)
                ON UPDATE CASCADE ON DELETE CASCADE,
            rawtranscript TEXT NOT NULL
        )
        """,     
        """ CREATE TABLE topics (
        transcriptid BIGINT NOT NULL,
        FOREIGN KEY (transcriptid)
            REFERENCES transcriptsinfo (transcriptid)
            ON UPDATE CASCADE ON DELETE CASCADE,
        topic SMALLINT,
        cossim2 NUMERIC,
        cossim3 NUMERIC,
        cossim4 NUMERIC,
        cossim5 NUMERIC,
        cossim6 NUMERIC,
        cossim7 NUMERIC,
        cossim8 NUMERIC,
        cossim9 NUMERIC,
        cossim10 NUMERIC,
        cossim11 NUMERIC,
        cossim12 NUMERIC,
        cossim13 NUMERIC,
        cossim14 NUMERIC,
        cossim15 NUMERIC,
        cossim16 NUMERIC,
        cossim17 NUMERIC,
        cossim18 NUMERIC,
        cossim19 NUMERIC,
        cossim20 NUMERIC,
        cossim21 NUMERIC
        )
        """          
  )
    conn = None
    try:
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
 



