'''
Created on 2/09/2014

@author: achim
'''
import tables
import types

class testFSM:

    myTables=None

    def __init__(self):

        if type(self).myTables is None:
            # initialize the enum list
            stateNames=set(["start", "end"]) # default, must be there
            for attr in dir(self):
                nameSplit=attr.split("_")
                if len(nameSplit)>=2 and nameSplit[0] in ["enter", "leave", "activity"]:
                    attr_object=getattr(self, attr)
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
            theFile=tables.open_file("testFSM.h5", "w")
            type(self).myTables=theFile.createTable("/", "testFSM", transitions)
        
    def __del__(self):
        # needing another place to flush the tables and close the file            
        if type(self).myTables is not None:
            #theFile=type(self).myTables._v_parent._v_file
            try:
                type(self).myTables.flush()
                #type(self).myTables.close()
            except Exception:
                pass
            #theFile.close()

    def reportTransitionPyTables(self, s1, s2, t1, t2):
        theTransition=type(self).myTables.row
        # make this assignment more efficient
        theTransition["agentId"]=self.agentId
        theTransition["timeStamp"]=t2
        theTransition["fromState"]=self.stateEnum[s1 if s1 else "start"]
        theTransition["toState"]=self.stateEnum[s2 if s2 else "start"]
        theTransition["dwellTime"]=t2-t1
        theTransition["effort"]=self.effort
        # fill in the values
        theTransition.append()
        #type(self).myTables.flush()
