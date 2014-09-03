'''
Created on 2/09/2014

@author: achim
'''
import tables
import types

from ABM import fsmAgent

class hdfLogger:
    
    def __init__(self, logFileName):
        
        self.speciesTables={}
        
        # needing another place to flush the tables and close the file
        self.theFile=tables.open_file(logFileName, "w")
        self.theFile.create_group("/", "transitionLogs")
    
        # create a table for messages
    
    def __del__(self):
        if hasattr(self, "theFile"):
            self.theFile.close()
    
    def reportTransition(self, agent, s1, s2, t1, t2):
        if not isinstance(agent, fsmAgent):
            raise TypeError("{:s} is not an fsmAgent".format(str(agent)))

        agentType=type(agent)
        if agentType not in self.speciesTables:
            # create agent table on the fly
            # initialize the enum list
            stateNames=set(["start", "end"]) # default, must be there
            for attr in dir(agent):
                nameSplit=attr.split("_")
                if len(nameSplit)>=2 and nameSplit[0] in ["enter", "leave", "activity"]:
                    attr_object=getattr(agent, attr)
                    attr_name="_".join(nameSplit[1:])
                    if type(attr_object) is types.MethodType:
                        stateNames.add(attr_name)

            type(self).stateEnum=stateEnum=tables.Enum(list(stateNames))
            transitions=type("transitions", (tables.IsDescription,), {"timeStamp": tables.Float64Col(), # @UndefinedVariable
                                                                      "fromState": tables.EnumCol(stateEnum, "start", "uint16"), # @UndefinedVariable
                                                                      "toState": tables.EnumCol(stateEnum, "start", "uint16"), # @UndefinedVariable
                                                                      "dwellTime":tables.Float32Col(), # @UndefinedVariable
                                                                      "effort": tables.Float32Col(),   # @UndefinedVariable
                                                                      "agentId": tables.UInt64Col()})  # @UndefinedVariable

            # needing another place to flush the tables and close the file
            
            self.speciesTables[agentType]=self.theFile.createTable("/transitionLogs", str(agentType.__name__), transitions)

        theTransition=self.speciesTables[agentType].row
        theTransition["agentId"]=agent.agentId
        theTransition["timeStamp"]=t2
        theTransition["fromState"]=self.stateEnum[s1 if s1 else "start"]
        theTransition["toState"]=self.stateEnum[s2 if s2 else "start"]
        theTransition["dwellTime"]=t2-t1
        theTransition["effort"]=agent.effort
        # fill in the values
        theTransition.append()
    
    def logMessage(self):
        pass
