'''
Created on 25/01/2015

@author: achim
'''
import types
import resource
import time
import os
import sys
import threading

from multiprocessing.reduction import ForkingPickler

from . import fsmAgent


class progressMonitor(threading.Thread):

    schedule = [(200, 100), (20, 10), (2, 1), (0.2, 0.1)]
    """
    the schedule contains tuples (a,b)
    meaning: till a seconds log in an interval of b seconds
    """
    minInterval = 0.05

    def __init__(self, theWorld, logOutput):
        self.theWorld = theWorld
        self.logAction = logOutput
        self.quitFlag = threading.Event()
        self.quitFlag.clear()
        threading.Thread.__init__(self)

    def run(self):
        self.startTime = time.time()

        while not self.quitFlag.is_set():
            try:
                self.logAction(self.theWorld)
            except Exception as e:
                print("Error in progress logging: {}".format(e),
                      file=sys.stderr)
                self.quitFlag.set()
                return
            # determine next tick
            theRunTime = time.time()-self.startTime
            timeInterval = 0.0
            for runTime, timeInterval in self.schedule:
                if theRunTime > runTime:
                    break
            self.quitFlag.wait(max(timeInterval, self.minInterval))

        try:
            self.logAction(self.theWorld)
        except Exception:
            self.quitFlag.set()

        del self.theWorld
        del self.logAction


class offloadedReporting:

    def __init__(self):
        self.speciesTables = {}
        self.stateDicts = {}
        self.outputPipes = []

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
        brokenPipes = []
        msgSerialized = ForkingPickler.dumps(message)
        for p in self.outputPipes:
            try:
                p._send_bytes(msgSerialized)
            except BrokenPipeError:
                print("pipe", p, "is broken, removing from output pipes")
                brokenPipes.append(p)
        for p in brokenPipes:
            if p in self.outputPipes:
                self.outputPipes.remove(p)

    def registerTransitionTable(self,
                                agent,
                                extraParameterTypes={},
                                stateNames=None):
        # see whether this is already registered
        if not isinstance(agent, fsmAgent):
            raise TypeError("{:s} is not an fsmAgent".format(str(agent)))

        agentType = type(agent)
        # initialize the enum list
        if stateNames is None:
            stateNames = set(["start", "end"])  # default, must be there
            for attr in dir(agent):
                nameSplit = attr.split("_")
                if len(nameSplit) >= 2 and nameSplit[0] in ["enter",
                                                            "leave",
                                                            "activity"]:
                    attr_object = getattr(agent, attr)
                    attr_name = "_".join(nameSplit[1:])
                    if isinstance(attr_object, types.MethodType):
                        stateNames.add(attr_name)

        stateDict = dict((n, i) for i, n in enumerate(sorted(stateNames)))

        tableDef = {"timeStamp": "tables.Float64Col()",
                    "fromState": stateDict,
                    "toState": stateDict,
                    "dwellTime": "tables.Float32Col()",
                    "effort": "tables.Float32Col()",
                    "agentId": "tables.UInt64Col()"}

        tableDef.update(extraParameterTypes)
        # and then handle the extra parameters!
        # how to get the extra parameters?

        self.send(["registerTransitionType",
                   str(agentType.__name__),
                   tableDef])
        # self.msgQueue.put_nowait(["registerTransitionType",
        #                           str(agentType.__name__), tableDef])
        self.speciesTables[agentType] = tableDef
        self.stateDicts[agentType] = stateDict

    def reportTransition(self, agent, s1, s2, t1, t2, **extraParams):

        # see whether this is already registered
        # if not isinstance(agent, fsmAgent):
        #     raise TypeError("{:s} is not an fsmAgent".format(str(agent)))

        agentType = type(agent)
        if agentType not in self.speciesTables:
            # create agent table on the fly
            self.registerTransitionTable(agent)

        stateDict = self.stateDicts[agentType]
        # that's the order expected for msg[2]: agentId, t1, t2,
        #                                       fromState, toState, effort
        self.send(["logTransition",
                   agentType.__name__,
                   [agent.agentId, t1, t2, stateDict[s1],
                    stateDict[s2], agent.effort],
                   extraParams])

    def logMessage(self, message):
        self.send(["message", message])

    def writeParameters(self, parameters, runParameters={}):
        self.send(["parameters", parameters, runParameters])

    def logProgress(self, theWorld=None):
        if not hasattr(self, "startTime"):
            self.startTime = time.time()

        statsFile = open("/proc/{:d}/statm".format(os.getpid()))
        stats = statsFile.read().split(" ")
        statsFile.close()
        memSize = int(stats[5])*resource.getpagesize()
        if theWorld is not None:
            theData = (float(theWorld.wallClock),
                       time.time()-self.startTime,
                       sum([len(a) for a in theWorld.theAgents.values()]),
                       len(theWorld.theScheduler.schedule_heap),
                       memSize)
        else:
            theData = (0.0, time.time()-self.startTime, 0, 0, memSize)
        self.send(["progress", theData])
