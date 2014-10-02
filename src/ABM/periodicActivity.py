'''
Created on 2/09/2014

@author: achim
'''

from .agentBase import agentBase

class periodicActivity(agentBase):
    
    def __init__(self, theWorld, period):
        agentBase.__init__(self, theWorld)
        self.setPeriod(period)
        theWorld.theScheduler.addEvent(self.__doActivity(), self.__doActivity)

    def __doActivity(self):
        self.activity()
        return self.wallClock()+self.period

    def setPeriod(self, newPeriod):
        # todo: doesn't reschedule immediately
        if newPeriod<=0:
            raise ValueError("period must be positive")
        self.period=float(newPeriod)
        
    def activity(self):
        pass
