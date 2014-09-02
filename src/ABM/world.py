'''
Created on 2/09/2014

@author: achim
'''

import datetime
import sys
import itertools
from .scheduler import scheduler

class world:
    
    # contains the essentials of a agent based model simulation
    
    theScheduler=None
    theAgents={}
    agentIdCounter=0

    wallClock=0.0
    worldStart=None
    theTopography=None # future
    theLogger=None # near future
    
    def __init__(self):

        # time (real time)
        self.worldStart=datetime.datetime.now()
        self.wallClock=-1.0
        self.updateWallClock(0.0)
        
        self.theScheduler=scheduler(self)

        # topography:
        # some object making/handling coordinates and put them into relation to area, to each other
        
        # the logger

    def updateWallClock(self, newClock):
        """ only used by scheduler!
        advances world clock and updates information on daytime, weekday and so on...
        """
        if self.wallClock > newClock:
            raise ValueError("wall clock can't be set backwards")

        if self.wallClock<newClock:
            self.wallClock=newClock
            # update daytime, week day, month...
            #make a daytime object out of it
            if self.worldStart is not None:
                c=self.worldClock=self.worldStart+datetime.timedelta(seconds=newClock)
                # update weekday, time
                self.daytime=c.time()
                self.date=c.date()
                self.weekDay=self.date.weekday()

    def iterAgents(self, species):
        yield from itertools.chain.from_iterable([agentList for agentType, agentList in self.theAgents.items()
                                                  if issubclass(agentType, species)])

    def addToWorld(self, theAgent):
        c=theAgent.agentId=self.agentIdCounter
        self.agentIdCounter=c+1
        agentDict=self.theAgents
        theSpecies=type(theAgent)
        if theSpecies not in agentDict:
            agentDict[theSpecies]=[theAgent]
        else:
            agentDict[theSpecies].append(theAgent)

    def removeFromWorld(self, theAgent):
        # remove from theAgents
        self.theAgents[type(theAgent)].remove(theAgent)
        
        # remove from scheduler
        self.theScheduler.removeAgentFromScheduler(theAgent)
        
        # check reference count
#         remainingRefs=sys.getrefcount(theAgent)
#         if remainingRefs>2:
#             print("{:s} ref count is {:d}".format(str(theAgent), remainingRefs-2))