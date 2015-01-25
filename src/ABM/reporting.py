'''
Created on 25/01/2015

@author: achim
'''
import types
import resource
import time
import os
from multiprocessing.reduction import ForkingPickler

from . import fsmAgent

class offloadedReporting:
    
    def __init__(self):
        self.speciesTables={}
        self.outputPipes=[]        
        
    def __del__(self):

        if hasattr(self, "outputPipes"):
            for p in self.outputPipes:
                p.send(["end"])
                p.close()
                del p

        if hasattr(self, "loggingProcess"):
            self.loggingProcess.join()
            del self.loggingProcess

    def send(self, message):
        msgSerialized=ForkingPickler.dumps(message)
        for p in self.outputPipes:
            p._send_bytes(msgSerialized)

    def registerTransitionTable(self, agent, extraParameterTypes={}, stateNames=None):
        # see whether this is already registered
        if not isinstance(agent, fsmAgent):
            raise TypeError("{:s} is not an fsmAgent".format(str(agent)))
        
        agentType=type(agent)
        # initialize the enum list
        if stateNames is None:
            stateNames=set(["start", "end"]) # default, must be there
            for attr in dir(agent):
                nameSplit=attr.split("_")
                if len(nameSplit)>=2 and nameSplit[0] in ["enter", "leave", "activity"]:
                    attr_object=getattr(agent, attr)
                    attr_name="_".join(nameSplit[1:])
                    if type(attr_object) is types.MethodType:
                        stateNames.add(attr_name)

        stateNames=list(stateNames)
        stateNames.sort()
        tableDef={ "timeStamp": "tables.Float64Col()", # @UndefinedVariable
                  "fromState": stateNames,
                  "toState": stateNames,
                  "dwellTime":"tables.Float32Col()", # @UndefinedVariable
                  "effort": "tables.Float32Col()",   # @UndefinedVariable
                  "agentId": "tables.UInt64Col()"}  # @UndefinedVariable
        
        tableDef.update(extraParameterTypes)
        # and then handle the extra parameters!
        # how to get the extra parameters?
        
        self.send(["registerTransitionType", str(agentType.__name__), tableDef])
        #self.msgQueue.put_nowait(["registerTransitionType", str(agentType.__name__), tableDef])
        self.speciesTables[agentType]=tableDef

    def reportTransition(self, agent, s1, s2, t1, t2, **extraParams):

        # see whether this is already registered
        if not isinstance(agent, fsmAgent):
            raise TypeError("{:s} is not an fsmAgent".format(str(agent)))

        agentType=type(agent)
        if agentType not in self.speciesTables:
            # create agent table on the fly
            self.registerTransitionTable(agent)
        
        # that's the order expected for msg[2]: agentId, t1, t2, fromState, toState, effort
        self.send(["logTransition", str(agentType.__name__), [agent.agentId, t1, t2, s1, s2, agent.effort], extraParams])
        #self.msgQueue.put_nowait(["logTransition", str(agentType.__name__), [agent.agentId, t1, t2, s1, s2, agent.effort], extraParams])
        
    def logMessage(self, message):
        self.send(["message", message])
        #self.msgQueue.put_nowait(["message", message])
        
    def writeParameters(self, parameters, runParameters={}):
        self.send(["parameters", parameters, runParameters])
        #self.msgQueue.put_nowait(["parameters", parameters, runParameters])
                                
    def logProgress(self, theWorld=None):
        if not hasattr(self, "startTime"):
            self.startTime=time.time()
                    
        statsFile=open("/proc/{:d}/statm".format(os.getpid()))
        stats=statsFile.read().split(" ")
        statsFile.close()
        memSize=int(stats[5])*resource.getpagesize()
        if theWorld is not None:
            theData=(float(theWorld.wallClock),
                     time.time()-self.startTime,
                     sum([len(a) for a in theWorld.theAgents.values()]),
                     len(theWorld.theScheduler.schedule_heap),
                     memSize)
        else:
            theData=(0.0, time.time()-self.startTime, 0, 0, memSize)
        self.send(["progress", theData])
        #self.msgQueue.put_nowait(["progress", theData])
