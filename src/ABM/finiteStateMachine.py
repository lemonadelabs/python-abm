'''
Created on 2/09/2014

@author: achim
'''

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
        theWorld.theScheduler.addEvent(0.0, self.__doFSMActions)

    def __doFSMActions(self):
        wallClock=self.myWorld.wallClock
        if wallClock<self.nextActivity:
            return self.nextActivity
        nextState=self.nextState
        state=self.state
        if state!=nextState:
            leaveMethod=getattr(self, "leave_"+state, None)
            if leaveMethod is not None:
                leaveMethod()

            # report transition
            reportTransition=getattr(self, "reportTransition", None)
            if reportTransition:
                reportTransition(state, nextState, self.lastTransition, wallClock)
            self.effort=0.0
            
            enterMethod=getattr(self, "enter_"+nextState, None)
            if enterMethod is not None:
                enterMethod()

            self.lastTransition=wallClock
            self.state=nextState

            if nextState=="end":
                leaveMethod=None
                enterMethod=None
                # make sure nothing happens anymore!
                self.endLife()
                #print(sys.getrefcount(self)) # <- this should be 3
                return

        # now do the activity!
        activityMethod=getattr(self, "activity_"+nextState, None)
        
        if activityMethod is not None:
            activityMethod()
            # and do some re-scheduling if required
            return self.nextActivity
        else:
            print("no activity '{:s}' for {:s} no {:d}".format(nextState, type(self).__name__, self.agentId))
        
    def scheduleTransition(self, nextState, transitionTime=0.0):
        # report time and effort
        self.nextActivity=transitionTime
        self.nextState=nextState

    def scheduleActivity(self, activityTime=0.0):
        #self.nextState=self.state
        self.nextActivity=activityTime

    def addEffort(self, effort=0.0):
        self.effort+=effort

    def endLife(self):
        self.mailbox=[]
        # also modify the worldremoveAgent method
        self.myWorld.theAgents[type(self)].remove(self)

        # override endLife method of agentBase
        self.myWorld=None
        #print(sys.getrefcount(self))