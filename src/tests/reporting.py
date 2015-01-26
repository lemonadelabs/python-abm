'''
Created on 28/11/2014

@author: achim
'''
import unittest
import os
import sys
import time
import tables

from ABM import fsmAgent, world
from ABM.hdfReporting import hdfLogger, offloadedHdfLogger
from ABM.reporting import progressMonitor

class TestPrameters(unittest.TestCase):
    
    fileName="test.hdf"
    
    def setUp(self):
        if os.path.isfile(self.fileName):
            os.unlink(self.fileName)
    
    def testCreationTermination(self):
        o=offloadedHdfLogger(self.fileName)
        del o
        self.assertTrue(tables.is_pytables_file(self.fileName), "expecting hdf file {:s}".format(self.fileName))
    
    def testParameters(self):
        parameterSet={"bla":"bla", "blub":3}
        o=offloadedHdfLogger(self.fileName)
        o.writeParameters(parameterSet)
        del o
        self.assertTrue(tables.is_pytables_file(self.fileName), "expecting hdf file {:s}".format(self.fileName))

        logFile=tables.open_file(self.fileName)
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
        self.assertTrue(tables.is_pytables_file(self.fileName), "expecting hdf file {:s}".format(self.fileName))

        logFile=tables.open_file(self.fileName)
        self.assertTrue("/parameters" in logFile)
        
        parameterTable=logFile.root.parameters.read()
        self.assertTrue(len(parameterTable)==len(parameterSet)*repeatNo)        
        logFile.close()

class testProgressReporting(unittest.TestCase):

    fileName="test.hdf"
    
    def setUp(self):
        if os.path.isfile(self.fileName):
            os.unlink(self.fileName)

    def testProgressReporting(self):
        
        reportCalls=[]
        
        r=progressMonitor(None, lambda x:reportCalls.append(x))
        
        r.start()
        self.assertTrue(r.isAlive())
        r.join(1.0) # must be 1.0, otherwise won't be >=10 calls 
        self.assertTrue(r.isAlive())
        r.quitFlag.set()
        r.join()
        self.assertFalse(r.isAlive())
        
        self.assertGreaterEqual(len(reportCalls), 10)

    def testProgressReportingSafeQuit(self):
        
        reportCalls=[0]*5
        
        r=progressMonitor(None, lambda x:reportCalls.pop())
        
        r.start()
        self.assertTrue(r.isAlive())
        print("expecting error message:\n====")
        r.join(1.0)
        time.sleep(0.05)
        sys.stderr.flush()
        print("====")
        self.assertFalse(r.isAlive())
        self.assertTrue(r.quitFlag.isSet(), "expect quit flag set due to error")

    def testProgress(self):
        progressIter=10
        o=offloadedHdfLogger(self.fileName)
        for _ in range(progressIter):
            o.logProgress()
        del o
        self.assertTrue(tables.is_pytables_file(self.fileName), "expecting hdf file {:s}".format(self.fileName))
        logFile=tables.open_file(self.fileName, 'r')
        self.assertTrue("/progress" in logFile, "expecting progress log table")
        progressTable=logFile.root.progress.read()
        self.assertTrue(len(progressTable)==progressIter)
        logFile.close()

class testMessageLogging(unittest.TestCase):

    fileName="test.hdf"
    
    def setUp(self):
        if os.path.isfile(self.fileName):
            os.unlink(self.fileName)

    def testSimpleMessageLogging(self):    
        progressIter=10
        o=offloadedHdfLogger(self.fileName)
        for i in range(progressIter):
            o.logMessage("message {:d}".format(i))
        del o
        self.assertTrue(tables.is_pytables_file(self.fileName), "expecting hdf file {:s}".format(self.fileName))
        logFile=tables.open_file(self.fileName, 'r')
        self.assertTrue("/log" in logFile, "expecting message log table")
        logTable=logFile.root.log.read()
        self.assertTrue(len(logTable)==progressIter)
        logFile.close()

class flipFlopAgent(fsmAgent):
    
    def __init__(self, theWorld):
        self.transitionEvents=[]
        super().__init__(theWorld, "flip")
        
    def activity_flip(self):
        self.scheduleTransition("flop", self.wallClock()+30)
        
    def activity_flop(self):
        self.scheduleTransition("flip", self.wallClock()+30)

    def reportTransition(self, s1, s2, t1, t2):
        if self.myWorld.theLogger is not None:
            self.myWorld.theLogger.reportTransition(self, s1, s2, t1, t2)
        else:
            self.transitionEvents.append((t2, s2))

class flipFlopAgentWCustomizedLogging(fsmAgent):
        
    def __init__(self, theWorld):
        super().__init__(theWorld, "flip")
        
    def activity_flip(self):
        self.scheduleTransition("flop", self.wallClock()+30)
        
    def activity_flop(self):
        self.scheduleTransition("flip", self.wallClock()+30)

    def reportTransition(self, s1, s2, t1, t2):
        if self.myWorld.theLogger is not None:
            #self.myWorld.theLogger.reportTransition(self, s1, s2, t1, t2)
            self.myWorld.theLogger.reportTransition(self, s1, s2, t1, t2, blabla=s1)

class TestFull(unittest.TestCase):

    fileName="test.hdf"

    def setUp(self):
        self.agentWorld=world()
        flipFlopAgent(self.agentWorld)
        if os.path.isfile(self.fileName):
            os.unlink(self.fileName)

    def tearDown(self):
        del self.agentWorld

    def testReporting(self):
        flipFlopAgentWCustomizedLogging(self.agentWorld)
        # add reporting
        o=self.agentWorld.theLogger=offloadedHdfLogger(self.fileName)
        # log start
        o.registerTransitionTable(self.agentWorld.theAgents[flipFlopAgentWCustomizedLogging][0], {"blabla": "tables.StringCol(itemsize=15)"})

        schedulerIter=10
        # start scheduler
        for i in range(schedulerIter):
            self.agentWorld.theScheduler.eventLoop(10.0*(i+1))
        
        self.agentWorld.theLogger=None
        del o
        
        # find out whether report is there
        self.assertTrue(tables.is_pytables_file(self.fileName), "expecting hdf file {:s}".format(self.fileName))

        logFile=tables.open_file(self.fileName, "r")
        self.assertTrue("/transitionLogs/flipFlopAgent" in logFile, "expecting transition table")
        # test the table structure
        self.assertSetEqual(set(logFile.root.transitionLogs.flipFlopAgent.colnames),
                            set(["agentId", "timeStamp", "fromState", "toState", "effort", "dwellTime"]))
        
        self.assertTrue("/transitionLogs/flipFlopAgentWCustomizedLogging" in logFile, "expecting transition table")

        # test the table structure
        self.assertSetEqual(set(logFile.root.transitionLogs.flipFlopAgentWCustomizedLogging.colnames),
                            set(["agentId", "timeStamp", "fromState", "toState", "effort", "dwellTime", "blabla"]))
        logFile.close()

    def testReportingAndLogging(self):
        # add reporting
        o=self.agentWorld.theLogger=offloadedHdfLogger(self.fileName)
        # log start

        r=progressMonitor(self.agentWorld, o.logProgress)
        
        r.start()
        schedulerIter=10
        # start scheduler
        for i in range(schedulerIter):
            self.agentWorld.theScheduler.eventLoop(10.0*(i+1))

        r.quitFlag.set()
        r.join()
        self.agentWorld.theLogger=None
        del r
        del o

        # find out whether report is there
        self.assertTrue(tables.is_pytables_file(self.fileName), "expecting hdf file {:s}".format(self.fileName))

        logFile=tables.open_file(self.fileName, "r")
        self.assertTrue("/transitionLogs/flipFlopAgent" in logFile, "expecting transition table")

        # test the table structure
        self.assertSetEqual(set(logFile.root.transitionLogs.flipFlopAgent.colnames),
                            set(["agentId", "timeStamp", "fromState", "toState", "effort", "dwellTime"]))

        self.assertTrue("/progress" in logFile, "expecting progress log table")

        logFile.close()

class testBenchmark(unittest.TestCase):
    
    def testSpeed(self):
        fileNameOffload="test1.hdf"
        if os.path.isfile(fileNameOffload):
            os.unlink(fileNameOffload)
        fileName="test2.hdf"
        if os.path.isfile(fileName):
            os.unlink(fileName)

        simTime=1000000

        self.agentWorld=world()
        flipFlopAgent(self.agentWorld)
        self.agentWorld.theLogger=offloadedHdfLogger(fileNameOffload)
        #r=hdfReporting.progressMonitor(self.agentWorld, self.agentWorld.theLogger.logProgress)
        #r.start()
        t=time.time()
        self.agentWorld.theScheduler.eventLoop(simTime)
        #r.quitFlag.set()
        #r.join()
        #del r
        self.agentWorld.theLogger=None
        print("offLoad", time.time()-t)

        # the old hdf thingy
        self.agentWorld=world()
        flipFlopAgent(self.agentWorld)
        self.agentWorld.theLogger=hdfLogger(fileName)
        t=time.time()
        self.agentWorld.theScheduler.eventLoop(simTime)
        self.agentWorld.theLogger=None
        print("same process", time.time()-t)

    def testProfiling(self):
        fileNameOffload="test1.hdf"
        if os.path.isfile(fileNameOffload):
            os.unlink(fileNameOffload)
        simTime=1000000
        import cProfile
        pr=cProfile.Profile()

        self.agentWorld=world()
        flipFlopAgent(self.agentWorld)
        self.agentWorld.theLogger=offloadedHdfLogger(fileNameOffload)
        pr.enable()
        self.agentWorld.theScheduler.eventLoop(simTime)
        pr.disable()
        self.agentWorld.theLogger=None
        pr.dump_stats('offLoad.profile')
        del pr
