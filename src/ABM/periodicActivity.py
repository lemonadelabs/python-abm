'''
Created on 2/09/2014

@author: achim
'''

from .agentBase import agentBase

class periodicActivity(agentBase):
    
    def __init__(self, period):
        agentBase.__init__(self)
        self.setPeriod(period)
        self.__doActivity()

    def __doActivity(self):
        self.activity()
        self.schedule(self.wallClock()+self.period, self.__doActivity)

    def setPeriod(self, newPeriod):
        # todo: doesn't reschedule immediately
        if newPeriod<=0:
            raise ValueError("period must be positive")
        self.period=float(newPeriod)
        
    def activity(self):
        pass
