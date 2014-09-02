'''
Created on 2/09/2014

@author: achim
'''

import heapq
import collections
import math
import datetime
import random

global theScheduler

class scheduler:
    # loosely follows the sched.scheduler implementation (well, it is anyway the obvious thing to do)

    # this is a singleton, right now...
    singleton=None
    
    wallClock=0.0 # in seconds from an arbitrary starting date
    worldStart=None
    
    timeStep=30.0 # in seconds

    # what does a time step do?
    # governs the (re) evaluation of triggered activities
    # repetition time for generating new agents
    # events can be scheduled with higher precision (i.e. do not have to stick to multiples of time step), but their repetition time is limited to timeStep 

    schedule_heap=[]
    heapq.heapify(schedule_heap)

    event=collections.namedtuple("event", ["timestamp", "method"])
    # do compare time stamps, but not methods
    event.__lt__=lambda x,y: x[0]<y[0]
    
    def __init__(self):
        if scheduler.singleton is not None:
            raise Exception("only one scheduler allowed")
        scheduler.singleton=self
        global theScheduler
        theScheduler=self
        self.worldStart=datetime.datetime.now()
        self.updateWallClock(0.0)
    
    def addEvent(self, timestamp, target=None):
        if timestamp<self.wallClock:
            raise ValueError("timestamp<wallClock")
        if not math.isinf(timestamp):
            heapq.heappush(self.schedule_heap, self.event(max(self.wallClock+self.timeStep, timestamp), target))
        
    def removeAgentFromScheduler(self, theAgent):
        # be prepared for non class methods as well?
        newHeap=[e for e in self.schedule_heap if e.method.__self__ is not theAgent]
        heapq.heapify(newHeap)
        self.schedule_heap=newHeap
    
    def updateWallClock(self, newClock):
        assert self.wallClock <= newClock
        if self.wallClock<newClock:
            self.wallClock=newClock
            # update daytime, week day, month...
            #make a daytime object out of it
            if self.worldStart is not None:
                self.worldClock=self.worldStart+datetime.timedelta(newClock)
    
    # create functions like:
    # nextDay
    # nextBusinessDay
    
    def eventLoop(self, stopTime=float("inf")):
        
        # two ways of doing all this:
        # get one after the other event from the heap
        # or
        # get all events concerning a certain time interval from the heap and execute them in a random order
        
        while stopTime>self.wallClock:

            # schedule non-timing events like interrupts (e.g messages, evaluate conditions)
            # todo loop over all agents
            
            # collect all events within this time step
            thisWallClockTick=self.wallClock
            nextWallClockTick=thisWallClockTick+self.timeStep
            
            while self.schedule_heap and self.schedule_heap[0][0] < nextWallClockTick:

                self.updateWallClock(self.schedule_heap[0][0])

                exec_next=[heapq.heappop(self.schedule_heap)[1]]
                while self.schedule_heap and self.schedule_heap[0][0]==self.wallClock:
                    exec_next.append(heapq.heappop(self.schedule_heap)[1])

                # ok, execute them now (no threads... by now)
                # randomize order
                random.shuffle(exec_next)
                for e in exec_next:
                    e()
            
            self.updateWallClock(nextWallClockTick)
    
