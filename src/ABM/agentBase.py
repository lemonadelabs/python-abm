'''
Created on 2/09/2014

@author: achim
'''

import collections

class agentBase:    
    
    message=collections.namedtuple("message", ["timestamp", "agent", "message"])

    def __init__(self, theWorld):
        self.myWorld=theWorld
        theWorld.addToWorld(self)
        self.mailbox=[]
    
    def endLife(self):
        # does not guarantee to delete the species
        self.myWorld.removeFromWorld(self)
        self.myWorld=None

    # todo: mailbox methods
    
    def schedule(self, newTime, target):
        self.myWorld.theScheduler.addEvent(newTime, target)

    def removeFromScheduler(self):
        self.myWorld.theScheduler.removeAgentFromScheduler(self)

    def wallClock(self):
        return self.myWorld.wallClock
