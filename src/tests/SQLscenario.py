'''
Created on 13/01/2015

@author: achim
'''

import uuid
import unittest
from ABM.scenario import scenario, SQLscenario, SQLBase
import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker 
from sqlalchemy.engine.url import URL

class fromSQL(unittest.TestCase):

    def setUp(self):
        self.theUUID=uuid.uuid1()
        engine=create_engine(URL('mysql.mysqlconnector',
                                 username='simoneABM',
                                 password='awe5ome',
                                 database="ABMsimulations"))
        #engine.connect()
        SQLBase.metadata.create_all(engine)
        
        self.sessionmaker=sessionmaker(bind=engine)

        s=self.sessionmaker()
        s.query(SQLscenario).filter(SQLscenario.experimentID==self.theUUID.hex).delete()
        s.commit()
        s.close()
        
    def tearDown(self):
        s=self.sessionmaker()
        s.query(SQLscenario).filter(SQLscenario.experimentID==self.theUUID.hex).delete()
        s.commit()
        s.close()
        del s
        del self.sessionmaker
    
    def testRemovedData(self):
        session=self.sessionmaker()
        self.assertEqual(session.query(SQLscenario).filter(SQLscenario.experimentID==self.theUUID.hex).count(), 0)

    def testWriteOne(self):
        session=self.sessionmaker()
        p=SQLscenario(experimentID=self.theUUID.hex, name="bla", date=datetime.datetime.now(), value=8)
        session.add(p)
        session.commit()
        session.close()
        del p, session
        
        # check whether written
        session=self.sessionmaker()
        self.assertEqual(session.query(SQLscenario).filter(SQLscenario.experimentID==self.theUUID.hex).count(),1)
        self.assertEqual(session.query(SQLscenario).filter(SQLscenario.experimentID==self.theUUID.hex and SQLscenario.name=="bla").count(),1)
        session.close()
        
    def testReadOne(self):
        session=self.sessionmaker()
        p=SQLscenario(experimentID=self.theUUID.hex,name="bla", date=None, type_="Int", value=8)
        q=SQLscenario(experimentID=self.theUUID.hex,name="bla", date=datetime.datetime.now(), type_="Int", value=8)
        session.add_all([p,q])
        session.commit()

        s=scenario()
        s.readFromMySQL(session, self.theUUID)
        self.assertIn("bla", s.parameters)
        self.assertIs(type(s.parameters["bla"][1][0]), int)
        self.assertIs(type(s.parameters["bla"][1][1]), int)
        
    def testReadWrite(self):
        session=self.sessionmaker()

        s=scenario()
        s.parameters["bla"]=([None],[1])
        s.writeToMySQL(session, self.theUUID)
        self.assertEqual(session.query(SQLscenario).filter(SQLscenario.experimentID==self.theUUID.hex and SQLscenario.name=="bla").count(),1)