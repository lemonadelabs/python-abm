'''
Created on 2/09/2014

@author: achim
'''

import collections

class agentBase:    
    
    message=collections.namedtuple("message", ["timestamp", "sender", "content"])

    def __init__(self, theWorld):
        self.myWorld=theWorld
        theWorld.addToWorld(self)
        self.mailbox=[]
    
    def endLife(self):
        self.mailbox=[]
        # does not guarantee to delete the species
        self.myWorld.removeFromWorld(self)
        self.myWorld=None

    # todo: mailbox methods
    def sendMessage(self, recipient, content):
        # many recipients?
        recipient.mailbox.append(agentBase.message(timestamp=self.wallClock(),
                                                   sender=self,
                                                   content=content))

    def getNextMessage(self, senderType=None):
        if not self.mailbox:
            return None
        if senderType is None:
            return self.mailbox.pop(0)
        # get next message, but sender specific
        theMessage=next((m for m in self.mailbox if isinstance(m.sender, senderType)), None)
        if theMessage is not None:
            self.mailbox.remove(theMessage)
        return theMessage
    
    def schedule(self, newTime, target):
        self.myWorld.theScheduler.addEvent(newTime, target)

    def removeFromScheduler(self):
        self.myWorld.theScheduler.removeAgentFromScheduler(self)

    def wallClock(self):
        return self.myWorld.wallClock
