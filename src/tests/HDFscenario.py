'''
Created on 24/03/2015

@author: achim
'''

'''
Created on 13/01/2015

@author: achim
'''

import unittest
import tables
import os
import datetime

from ABM.scenario import scenario


class fromSQL(unittest.TestCase):
    
    testFileName='test.h5'

    def setUp(self):
        self.theTableFile=tables.open_file(self.testFileName, 'w')
        
    def tearDown(self):
        self.theTableFile.close()
        os.unlink(self.testFileName)
            
    def testRemovedData(self):
        s=scenario()
        s.writeToHDF(self.theTableFile.root, 'scenario')
        
        s.readFromHDF(self.theTableFile.root, 'scenario')
        
        self.assertEqual(len(s.parameters),0)

    def testReadWriteSimple(self):

        s=scenario()
        s.parameters["bla"]=([None],[1])
        s.writeToHDF(self.theTableFile.root, 'scenario')
        
        s2=scenario()
        s2.readFromHDF(self.theTableFile.root.scenario)
        self.assertTupleEqual(s.parameters["bla"], s2.parameters["bla"])

    def testReadWriteSimple2(self):

        s=scenario()
        s.parameters["bla"]=([None, datetime.datetime(2014,7,1)],[1,2])
        s.writeToHDF(self.theTableFile.root, 'scenario')
        
        s2=scenario()
        s2.readFromHDF(self.theTableFile.root.scenario)
        self.assertTupleEqual(s.parameters["bla"], s2.parameters["bla"])

    def testFailedName(self):
        s=scenario()
        s.parameters["♂"*43]=([None],[2])
        self.assertRaises(ValueError, s.writeToHDF, self.theTableFile.root, 'scenario')

        s.parameters={}
        s.parameters[" "*129]=([None],[2])
        self.assertRaises(ValueError, s.writeToHDF, self.theTableFile.root, 'scenario')

    def testReadWriteNames(self):
        
        s=scenario()
        s.parameters["blä"]=([None],[True])
        s.parameters["bl              d"]=([None],[False])
        s.parameters["blä"]=([None],[True])
        s.parameters["bl°"]=([None],[2])
        s.parameters["bl°a"]=([None],[2])
        s.parameters["♂♀"]=([None],[2])
        s.parameters["♂"*42]=([None],[2])
        s.parameters[" "*128]=([None],[2])
        s.writeToHDF(self.theTableFile.root, 'scenario')

        s2=scenario()
        s2.readFromHDF(self.theTableFile.root.scenario)
        
        self.assertSetEqual(set(s.parameters.keys()),
                            set(s2.parameters.keys()))