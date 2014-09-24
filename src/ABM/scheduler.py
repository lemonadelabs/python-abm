'''
Created on 2/09/2014

@author: achim
'''

import heapq
import collections
import math
import random

class scheduler:
    # loosely follows the sched.scheduler implementation (well, it is anyway the obvious thing to do)

    # what does a time step do?
    # governs the (re) evaluation of triggered activities
    # repetition time for generating new agents
    # events can be scheduled with higher precision (i.e. do not have to stick to multiples of time step), but their repetition time is limited to timeStep 

    event=collections.namedtuple("event", ["timestamp", "method"])
    # do compare time stamps, but not methods
    event.__lt__=lambda x,y: x[0]<y[0]
    
    def __init__(self, theWorld):
        self.theWorld=theWorld

        self.schedule_heap=[]
        heapq.heapify(self.schedule_heap)

        self.timeStep=30.0
        theWorld.updateWallClock(0.0)
    
    def addEvent(self, timestamp, target=None):
        wallClock=self.theWorld.wallClock
        if timestamp<wallClock:
            raise ValueError("timestamp<wallClock")
        if not math.isinf(timestamp):
            heapq.heappush(self.schedule_heap, self.event(max(wallClock+self.timeStep, timestamp), target))
        
    def removeAgentFromScheduler(self, theAgent):
        # be prepared for non class methods as well?
        newHeap=[e for e in self.schedule_heap if e.method.__self__ is not theAgent]
        heapq.heapify(newHeap)
        self.schedule_heap=newHeap
        
    # create functions like:
    # nextDay
    # nextBusinessDay
    
    def eventLoop(self, stopTime=float("inf")):
        theWorld=self.theWorld
        # two ways of doing all this:
        # get one after the other event from the heap
        # or
        # get all events concerning a certain time interval from the heap and execute them in a random order
        
        nextWallClockTick=wallClock=theWorld.wallClock
        while stopTime>wallClock:

            # schedule non-timing events like interrupts (e.g messages, evaluate conditions)
            # todo loop over all agents
            
            # collect all events within this time step
            nextWallClockTick=nextWallClockTick+self.timeStep
            while self.schedule_heap and self.schedule_heap[0][0] < nextWallClockTick:

                theWorld.updateWallClock(self.schedule_heap[0][0])
                wallClock=theWorld.wallClock

                exec_next=[heapq.heappop(self.schedule_heap)[1]]
                while self.schedule_heap and self.schedule_heap[0][0]==wallClock:
                    exec_next.append(heapq.heappop(self.schedule_heap)[1])

                # ok, execute them now (no threads... by now)
                # randomize order
                random.shuffle(exec_next)
                for e in exec_next:
                    #print(wallClock, e)
                    e()
            
            theWorld.updateWallClock(nextWallClockTick)
            wallClock=theWorld.wallClock
