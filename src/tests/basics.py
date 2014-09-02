import unittest

from ABM import *

class testFSM(fsmAgent):
    
    def __init__(self, theWorld):
        self.transitionEvents=[]
        fsmAgent.__init__(self, theWorld)
        
    
    def enter_start(self):
        self.scheduleTransition("flip")
    
    def activity_flip(self):
        self.scheduleTransition("flop", self.wallClock()+30)
        
    def activity_flop(self):
        self.scheduleTransition("flip", self.wallClock()+30)

    def reportTransition(self, s1, s2, t1, t2):
        self.transitionEvents.append((t2, s2))
        fsmAgent.reportTransition(self, s1, s2, t1, t2)

class basicsTest(unittest.TestCase):
    
    def testWorld(self):
        w=world()
        self.assertEqual(w.weekDay, w.worldStart.weekday())

    def testAgentBase(self):        
        w=world()
        a=agentBase(w)        

        # check on wall clock 
        self.assertEqual(a.wallClock(), 0)
        # check on agent list
        self.assertListEqual(list(w.iterAgents(type(a))), [a])
        self.assertListEqual(list(w.iterAgents(fsmAgent)), [])

        # remove from list
        a.endLife()
        self.assertListEqual(list(w.iterAgents(type(a))), [])
        
    def testFSM(self):
        w=world()
        a=testFSM(w)
        w.theScheduler.eventLoop(120)
        self.assertListEqual(a.transitionEvents, [(i*30.0, ('flip', 'flop')[i%2]) for i in range(4)])
        w.theScheduler.eventLoop(240)
        self.assertListEqual(a.transitionEvents, [(i*30.0, ('flip', 'flop')[i%2]) for i in range(8)])
        