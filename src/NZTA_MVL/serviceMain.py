'''
Created on 3/09/2014

@author: achim
'''

import random
import ABM 

class vehicleOwner(ABM.fsmAgent):

    def activity_start(self):
        if hasattr(self, "licenseEnds"):
            licenseDue=min(self.licenseEnds)
            wallClock=self.wallClock()
            if licenseDue<wallClock+7*24*3600:
                # take action!
                self.scheduleTransition("gatherRequiredInformation", 0.0)
            else:
                # check back next week or so
                self.scheduleActivity(self.wallClock()+random.uniform(3.0, 7.0)*24*3600)

    def activity_gatherRequiredInformation(self):
        self.effort+=180.0 # add 3 minutes...
        
        if random.random()<0.5:
            self.scheduleTransition("getToAgency", 0.0)
        else:
            self.scheduleTransition("haveAComputerAvailable", 0.0)

    def activity_getToAgency(self):
        if random.random()<0.5:
            self.scheduleTransition("fillOutForm", 0.0)
        else:
            self.scheduleTransition("queue", 0.0)

    def activity_fillOutForm(self):
        self.scheduleTransition("queue", 0.0)

    def activity_queue(self):
        self.scheduleTransition("fileFormOrReminder", 0.0)

    def activity_fileFormOrReminder(self):
        self.scheduleTransition("payFeeAtAgent", 0.0)
        
    def activity_payFeeAtAgent(self):
        self.scheduleTransition("returnToBaseWithLabel", 0.0)
        
    def activity_returnToBaseWithLabel(self):
        self.scheduleTransition("displayNewLicenseLabel", 0.0)

    def activity_haveAComputerAvailable(self):
        self.scheduleTransition("fillFormOnline", 0.0)

    def activity_fillFormOnline(self):
        if random.random()<0.5:
            self.scheduleTransition("getHelp", 0.0)
        else:
            self.scheduleTransition("payOnline", 0.0)

    def activity_getHelp(self):
        self.scheduleTransition("fillFormOnline", 0.0)

    def activity_payOnline(self):
        self.scheduleTransition("waitForMailWithLicenseLabel", 0.0)

    def activity_waitForMailWithLicenseLabel(self):
        self.scheduleTransition("displayNewLicenseLabel", 0.0)

    def activity_displayNewLicenseLabel(self):
        self.scheduleTransition("end", 0.0)

    def enter_end(self):
        pass

    def reportTransition(self, s1, s2, t1, t2):
        logger=self.myWorld.theLogger
        if logger is not None:
            logger.reportTransition(self, s1, s2, t1, t2)
        #print(t2, s2)
        #ABM.fsmAgent.reportTransition(self, s1, s2, t1, t2)

def generatePopulation(mvlWorld):
    
    # some probability distributions to sample from
    
    for i in range(10000):
        # todo: supply customer type and vehicle number
        # schedule licenses
        
        v=vehicleOwner(mvlWorld)
        v.licenseEnds=[random.uniform(4,30)*24*3600]

def main():
    
    mvlWorld=ABM.world()
    mvlWorld.theLogger=ABM.hdfLogger("test.h5")
    generatePopulation(mvlWorld)

    mvlWorld.theScheduler.eventLoop(30*24*3600)
    
    mvlWorld.theLogger=None

if __name__=="__main__":
    main()