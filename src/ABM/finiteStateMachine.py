'''
Created on 2/09/2014

@author: achim
'''

import sys
from .agentBase import agentBase

class fsmAgent(agentBase):
    # a finite state machine species
    # the base class provides:
    # transition counter

    # find state names, make enumeration
    # enter, activity, leave
    # and state name

    def __init__(self, theWorld, startState="start"):
        agentBase.__init__(self, theWorld)
        self.state="start"
        self.nextState=startState
        self.nextActivity=self.lastTransition=self.wallClock()
        self.effort=0.0
        self.__doFSMActions()

    def __doFSMActions(self):
        wallClock=self.wallClock()
        if self.state!=self.nextState:
            if wallClock>=self.nextActivity:
                leaveMethod=getattr(self, "leave_"+self.state, None)
                if leaveMethod is not None:
                    leaveMethod()
                enterMethod=getattr(self, "enter_"+self.nextState, None)
                if enterMethod is not None:
                    enterMethod()

                # report transition
                self.reportTransition(self.state, self.nextState, self.lastTransition, wallClock)
                self.effort=0.0
                
                self.lastTransition=wallClock
                self.state=self.nextState

        if self.state=="end":
            leaveMethod=None
            enterMethod=None
            # make sure nothing happens anymore!
            self.endLife()
            #print(sys.getrefcount(self)) # <- this should be 3
            return

        # now do the activity!
        activityMethod=getattr(self, "activity_"+self.state, None)
        
        if activityMethod is not None:
            activityMethod()
            # and do some re-scheduling if required
            self.schedule(self.nextActivity, self.__doFSMActions)
        else:
            print("no state activity '{:s}' for {:s} no {:d}".format(self.state, type(self).__name__, self.agentId))
        
    def scheduleTransition(self, nextState, transitionTime=0.0):
        # report time and effort
        self.nextActivity=transitionTime
        self.nextState=nextState

    def scheduleActivity(self, activityTime=0.0):
        #self.nextState=self.state
        self.nextActivity=activityTime

    def reportTransition(self, s1, s2, t1, t2):
        pass

    def addEffort(self, effort=0.0):
        self.effort+=effort

    def _endLife(self):
        self.mailbox=[]
        # also modify the worldremoveAgent method
        self.myWorld.theAgents[type(self)].remove(self)

        # override endLife method of agentBase
        self.myWorld=None
        #print(sys.getrefcount(self))