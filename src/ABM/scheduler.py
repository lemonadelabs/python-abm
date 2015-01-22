'''
Created on 2/09/2014

@author: achim
'''

import heapq
import math
import threading

class scheduler:
    # loosely follows the sched.scheduler implementation (well, it is anyway the obvious thing to do)

    # what does a time step do?
    # governs the (re) evaluation of triggered activities
    # repetition time for generating new agents
    # events can be scheduled with higher precision (i.e. do not have to stick to multiples of time step), but their repetition time is limited to timeStep 

    def __init__(self, theWorld):
        self.theWorld=theWorld

        self.schedule_heap=[]
        heapq.heapify(self.schedule_heap)

        self.timeStep=30.0
        assert(self.timeStep>0)
        theWorld.updateWallClock(0.0)
        self.abortSignal=threading.Event()
    
    def addEvent(self, timestamp, target):
        if not (math.isinf(timestamp) and timestamp>0):
            heapq.heappush(self.schedule_heap, (max(self.theWorld.wallClock+self.timeStep, timestamp), id(target), target))
        
    def removeAgentFromScheduler(self, theAgent):
        # be prepared for non class methods as well?
        newHeap=[e for e in self.schedule_heap if e[2].__self__ is not theAgent]
        heapq.heapify(newHeap)
        self.schedule_heap=newHeap
        
    # create functions like:
    # nextDay
    # nextBusinessDay
    
    def eventLoop(self, stopTime=float("inf")):
        theWorld=self.theWorld
        theHeap=self.schedule_heap
        self.abortSignal.clear()
        # two ways of doing all this:
        # get one after the other event from the heap
        # or
        # get all events concerning a certain time interval from the heap and execute them in a random order
        
        wallClock=theWorld.wallClock
        nextWallClockTick=wallClock+self.timeStep

        if theHeap:
            timeStamp, _, exec_next=heapq.heappop(theHeap)
        else:
            timeStamp=None
            exec_next=None
        
        while stopTime>wallClock and not self.abortSignal.is_set():

            if (timeStamp is None) or timeStamp>nextWallClockTick:
                theWorld.updateWallClock(nextWallClockTick)
                wallClock=nextWallClockTick
                # this is the slot where all triggers should run (e.g. messages in mail box)
                # do fill up the heap...

                nextWallClockTick=wallClock+self.timeStep

            if timeStamp is None:
                if theHeap:
                    # if the heap is still empty
                    timeStamp, _, exec_next=heapq.heappop(theHeap)
                continue
            
            if theHeap and timeStamp>theHeap[0][0]:
                # get a new one, which might be inserted from the last time step fill up
                timeStamp, _, exec_next=heapq.heappushpop(theHeap, (timeStamp, id(exec_next), exec_next))
                continue

            if timeStamp>nextWallClockTick:
                # start over with next wall clock tick
                continue

            if timeStamp>wallClock:
                theWorld.updateWallClock(timeStamp)
                wallClock=theWorld.wallClock

            try:
                # do the work!
                retval=exec_next()
            except Exception as e:
                raise
                print(str(e))
                exec_next=None
                retval=None

            if retval is not None:
                if type(retval) in [float, int]:
                    timeStamp=max(wallClock+self.timeStep, float(retval))
                    if math.isfinite(timeStamp):
                        timeStamp, _, exec_next=heapq.heappushpop(theHeap, (timeStamp, id(exec_next), exec_next))
                    else:
                        # ignore this event
                        if theHeap:
                            timeStamp, _, exec_next=heapq.heappop(theHeap)
                        else:
                            timeStamp=None
                            exec_next=None
                elif type(retval) is tuple:
                    timeStamp=max(wallClock+self.timeStep, float(retval[0]))
                    if math.isfinite(timeStamp):
                        timeStamp, _, exec_next=heapq.heappushpop(theHeap, (timeStamp, id(exec_next), retval[1]))
                    else:
                        # ignore this event
                        if theHeap:
                            timeStamp, _, exec_next=heapq.heappop(theHeap)
                        else:
                            timeStamp=None
                            exec_next=None
                else:
                    raise ValueError("unexpected return value from event function")
            else:
                if theHeap:
                    timeStamp, _, exec_next=heapq.heappop(theHeap)
                else:
                    timeStamp=None
                    exec_next=None

        # end of while loop!
        
        if exec_next is not None:
            # push that element back
            heapq.heappush(theHeap, (timeStamp, id(exec_next), exec_next))