'''
Created on 28/11/2014

@author: achim
'''
import unittest
import os
import subprocess
import tables

from ABM import fsmAgent, world, hdfReporting
from ABM.hdfReporting import offloadedHdfLogger

class flipFlopAgent(fsmAgent):
    
    def __init__(self, theWorld):
        self.transitionEvents=[]
        super().__init__(theWorld, "flip")
        
    def activity_flip(self):
        self.scheduleTransition("flop", self.wallClock()+30)
        
    def activity_flop(self):
        self.scheduleTransition("flip", self.wallClock()+30)

    def reportTransition(self, s1, s2, t1, t2):
        self.transitionEvents.append((t2, s2))
        #fsmAgent.reportTransition(self, s1, s2, t1, t2)

class TestPrameters(unittest.TestCase):
    
    fileName="test.hdf"
    
    def setUp(self):
        if os.path.isfile(self.fileName):
            os.unlink(self.fileName)
    
    def testCreationTermination(self):
        o=offloadedHdfLogger(self.fileName)
        del o
        self.assertTrue(tables.isPyTablesFile(self.fileName), "expecting hdf file {:s}".format(self.fileName))
    
    def testParameters(self):
        parameterSet={"bla":"bla", "blub":3}
        o=offloadedHdfLogger(self.fileName)
        o.writeParameters(parameterSet)
        del o
        self.assertTrue(tables.isPyTablesFile(self.fileName), "expecting hdf file {:s}".format(self.fileName))

        logFile=tables.openFile(self.fileName)
        self.assertTrue("/parameters" in logFile)
        
        parameterTable=logFile.root.parameters.read()
        self.assertTrue(len(parameterTable)==len(parameterSet))
        # todo: check parameters
        # todo: runParams
        
        logFile.close()

    def testParametersRepeated(self):
        parameterSet={"bla":"bla", "blub":3}
        o=offloadedHdfLogger(self.fileName)
        repeatNo=2
        for _ in range(repeatNo):
            o.writeParameters(parameterSet)
        del o
        self.assertTrue(tables.isPyTablesFile(self.fileName), "expecting hdf file {:s}".format(self.fileName))

        logFile=tables.openFile(self.fileName)
        self.assertTrue("/parameters" in logFile)
        
        parameterTable=logFile.root.parameters.read()
        self.assertTrue(len(parameterTable)==len(parameterSet)*repeatNo)        
        logFile.close()

    def testProgress(self):
        progressIter=10
        o=offloadedHdfLogger(self.fileName)
        for _ in range(progressIter):
            o.logProgress()
        del o
        self.assertTrue(tables.isPyTablesFile(self.fileName), "expecting hdf file {:s}".format(self.fileName))
        logFile=tables.openFile(self.fileName)
        self.assertTrue("/progress" in logFile, "expecting progress log table")
        progressTable=logFile.root.progress.read()
        self.assertTrue(len(progressTable)==progressIter)
        logFile.close()

class TestFull(unittest.TestCase):

    fileName="test.hdf"

    def setUp(self):
        self.agentWorld=world()
        a=flipFlopAgent(self.agentWorld)
        if os.path.isfile(self.fileName):
            os.unlink(self.fileName)

    def tearDown(self):
        del self.agentWorld

    def testReporting(self):
        # add reporting
        o=offloadedHdfLogger(self.fileName)

        # log start
        o.logProgress(self.agentWorld)

        schedulerIter=10
        # start scheduler
        for i in range(schedulerIter):
            self.agentWorld.theScheduler.eventLoop(10.0*(i+1))
            o.logProgress(self.agentWorld)
        
        # quit reporter
        del o
        
        # find out whether report is there
        self.assertTrue(tables.isPyTablesFile(self.fileName), "expecting hdf file {:s}".format(self.fileName))
        logFile=tables.openFile(self.fileName)
        self.assertTrue("/progress" in logFile, "expecting progress log table")

        # read report
        progressTable=logFile.root.progress.read()
        self.assertTrue(len(progressTable)==(schedulerIter+1))
        logFile.close()

    def testPeriodicReporting(self):
        
        reportCalls=[]
        
        r=hdfReporting.progressMonitor(None, lambda x:reportCalls.append(x))
        
        r.start()
        r.join(1.0)
        r.quitFlag.set()
        r.join()
        
        self.assertGreaterEqual(len(reportCalls), 10)

    def testPeriodicReportingSafeQuit(self):
        
        reportCalls=[0]*5
        
        r=hdfReporting.progressMonitor(None, lambda x:reportCalls.pop())
        
        r.start()
        self.assertTrue(r.isAlive())
        print("expecting error message:\n====")
        r.join(1.0)
        print("====")
        self.assertFalse(r.isAlive())
        self.assertTrue(r.quitFlag.isSet(), "expect quit flag set due to error")
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()