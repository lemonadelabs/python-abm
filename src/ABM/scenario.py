'''
Created on 8/12/2014

this module provides class and functions to handle parameter sets/scenarios

@author: achim
'''

import json
import datetime
import bisect
from argparse import ArgumentError

class scenario:
    
    def __init__(self):
        self.parameters={}
        
    def readFromJSON(self, jsonString):
        # JSON doesn't distinguish reliably between integers and floats
        
        newParameters={}
        
        data=json.loads(jsonString)
        for key, value in data.items():
            
            theValues=[]
            
            if type(value) is dict:
                # time series
                if "initial" not in value:
                    raise KeyError("initial value missing for {:s}".format(key))

                if type(value["initial"]) not in [int, float, bool]:
                    raise TypeError("unexpected value for {:s}".format(key))

                theValues.append((None, value["initial"]))
                
                times=[t for t in value.keys() if t!="initial"]
                #times.sort()
                # todo: don't rely on lexicographic order reflecting time order...
                
                for t in times:
                    timeDate=self.parseTime(t, key)

                    if any(timeDate==tt for tt,_ in theValues[1:]):
                        raise ValueError("duplicate time found in {:s}".format(key))

                    if value[t][0] not in ["set"]:
                        raise ValueError("unknown update command in {:s}".format(key))

                    if type(theValues[0][1]) is bool and type(value[t][1]) is not bool:
                        raise TypeError("expecting consistent types")

                    theValues.append((timeDate, value[t][1]))
            else:
                if type(value) not in [int, float, bool]:
                    raise TypeError("unexpected value for {:s}".format(key))
                theValues.append((None, value))

            # clean up float/int mixture -> convert everything to float if one float found
            if any(type(v) is float for _,v in theValues):
                theValues=[(t,float(v)) for t,v in theValues]

            sortedValues=theValues[1:]
            sortedValues.sort()
            sortedValues.insert(0, theValues[0])
            newParameters[key]=list(zip(*sortedValues))
    
        # success
        self.parameters=newParameters
    
    def parseTime(self, t, key="?"):
        if type(t) is not str or len(t)<8:
            raise ValueError("unexpected/invalid time definition in {:s}".format(key))

        # collect date
        timeDate=datetime.datetime(year=int(t[:4]), month=int(t[4:6]), day=int(t[6:8]))
       
        if len(t)>8:
            if len(t)<17 or t[8]!=" " or t[11]!=":" or t[14]!=":":
                raise ValueError("invalid time definition in {:s}".format(key))
            timeDate=timeDate.replace(hour=int(t[9:11]), minute=int(t[12:14]), second=int(t[15:17]))

        if len(t)>17:
            if t[17]!=".":
                raise ValueError("invalid time definition in {:s}".format(key))
            timeDate=timeDate.replace(microsecond=int(float("0"+t[17:])*1e6))

        return timeDate
    
    def readFromCmdLine(self, args):
#        for k,v in self.experimentParameters.items():
#            t={str:"str", float:"float", int:"int", bool: "bool"}[type(v)]
#            all_parameters.append("--param={:s},{:s},{:s}".format(k,t,str(v)))
        newParameters={}

        for a in args:
            if a.startswith("--param="):
                sections=a[8:].split(",")
                # todo
                if len(sections)!=3:
                    raise ValueError("expecting key,type,value")
                key,valueType,value=sections
                # else: shouldn't be here!
                if valueType in ["int", "bool", "float"]:
                    if valueType=="int":
                        value=int(value)
                    elif valueType=="float":
                        value=float(value)
                    elif valueType=="bool":
                        value=bool(value) # todo: more fancy conversion
                    # must be default
                    if key in newParameters:
                        if any(t is None for t,_ in newParameters[key]):
                            raise KeyError("duplicate default value")
                        newParameters[key].insert(0, (None, value))
                    else:
                        newParameters[key]=[(None, value)]
                else:
                    # expect date and time
                    timeDate=self.parseTime(valueType, key)
                    if key in newParameters:
                        if any(t is not None and t==timeDate for t,_ in newParameters[key]):
                            raise ValueError("duplicate time spec")
                        newParameters[key].append((timeDate, value))
                    else:
                        newParameters[key]=[(timeDate, value)]

        for key in list(newParameters.keys()):
            if newParameters[key][0][0] is not None:
                raise ValueError("no default value for {:s}".format(key))

            if type(newParameters[key][0][1]) is int:
                sortedValues=[(k,int(v)) for k,v in newParameters[key][1:]]
            elif type(newParameters[key][0][1]) is float:
                sortedValues=[(k,float(v)) for k,v in newParameters[key][1:]]
            elif type(newParameters[key][0][1]) is bool:
                sortedValues=[(k,bool(v)) for k,v in newParameters[key][1:]]

            sortedValues.sort()
            sortedValues.insert(0, newParameters[key][0])
            newParameters[key]=list(zip(*sortedValues))

        self.parameters=newParameters
    
    def getValue(self, name, time):
        param=self.parameters[name]
        paramLen=len(param[0])
        if paramLen<2:
            return param[1][0]
        idx=bisect.bisect_right(param[0], time, 1, paramLen)
        return param[1][idx-1]
