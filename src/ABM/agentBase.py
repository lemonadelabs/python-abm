'''
Created on 2/09/2014

@author: achim
'''

import collections
import itertools

class agentBase:

    # how long does an agent live? It will live for ever if not removed from this dictionary
    agentDictionary={}
    
    agentIdCounter=0
    
    message=collections.namedtuple("message", ["timestamp", "agent", "message"])

    def __init__(self):
        self.agentId=type(self).agentIdCounter
        type(self).agentIdCounter=self.agentId+1
        if type(self) in agentBase.agentDictionary:
            agentBase.agentDictionary[type(self)].append(self)
        else:
            agentBase.agentDictionary[type(self)]=[self]

        self.mailbox=[]
    
    @classmethod
    def iterAgents(species):
        yield from itertools.chain.from_iterable([agentList for agentType, agentList in agentBase.agentDictionary.items() if issubclass(agentType, species)])
    
    def endLife(self):
        # does not guarantee to delete the species
        agentBase.agentDictionary[type(self)].remove(self)
        self.removeFromScheduler()

    # todo: mailbox methods
    
    def schedule(self, newTime, target):
        global theScheduler
        theScheduler.addEvent(newTime, target)
        
    def removeFromScheduler(self):
        global theScheduler
        theScheduler.removeAgentFromScheduler(self)

    @staticmethod
    def wallClock():
        global theScheduler
        return theScheduler.wallClock
