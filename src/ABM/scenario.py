'''
Created on 8/12/2014

this module provides class and functions to handle parameter sets/scenarios

@author: achim
'''

import json
import datetime
import bisect

from sqlalchemy import Column, String, DateTime, Integer, Enum
from sqlalchemy.ext.declarative import declarative_base

SQLBase=declarative_base()

class SQLscenario(SQLBase):
    # todo: this is not a scenario, the collection of rows with the same UUID are a scenario
    __tablename__="scenario"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    experimentID=Column(String(32))
    name=Column(String(128))
    type_=Column(Enum("Int", "Float", "Bool"))
    date = Column(DateTime(timezone=False), nullable=True)
    value = Column(String(128))

class scenario:
    
    def __init__(self):
        self.parameters={}

    def readFromMySQL(self, session, experimentID=None):
        # get a cursor, get a uuid or so...
        
        if experimentID is None:
            # do not filter
            scenarios=session.query(SQLscenario.name, SQLscenario.date, SQLscenario.type_, SQLscenario.value).all()
        elif type(experimentID) is str:
            scenarios=session.query(SQLscenario.name, SQLscenario.date, SQLscenario.type_,  SQLscenario.value).filter(SQLscenario.experimentID==experimentID).all()
        else:
            scenarios=session.query(SQLscenario.name, SQLscenario.date, SQLscenario.type_,  SQLscenario.value).filter(SQLscenario.experimentID==experimentID.hex).all()

        newParameters={}
        for name, date, type_, value in scenarios:
            
            if type_.lower()=="float":
                value=float(value)
            elif type_.lower()=="int":
                value=int(value)
            elif type_.lower()=="bool":
                value=(value in ["True", "1", "true"])
            else:
                raise ValueError("unknown type specification '{}'".format(type_))
            
            # oh what about the default parameter?!
            if date is None:
                if name in newParameters:
                    newParameters[name].insert(0,(None, value))
                else:
                    newParameters[name]=[(None, value)]

            else:
                if name in newParameters:
                    newParameters[name].append((date, value))
                else:
                    newParameters[name]=[(date, value),]
        
        for key in list(newParameters.keys()):
            theValues=newParameters[key]

            # check whether there is a default value
            if theValues[0][0] is not None:
                raise ValueError("need one default value for '{}'".format(key))
            if len(theValues)>1 and theValues[1][0] is None:
                raise ValueError("need only one default value for '{}'".format(key))

            # todo: check types of values are consistent!
            
            sortedValues=theValues[1:]
            sortedValues.sort()
            sortedValues.insert(0, theValues[0])
            newParameters[key]=list(zip(*sortedValues))
        
            # success
            self.parameters=newParameters
        
    def writeToMySQL(self, session, experimentID):

        expID=experimentID.hex
        # todo: clear values before writing new ones
        session.query(SQLscenario).filter(SQLscenario.experimentID==expID).delete()
        
        for name, values in self.parameters.items():
            theType={int: "int", float:"float", bool:"bool"}[type(values[1][0])]
            session.add_all([SQLscenario(name=name, type_=theType,
                                         experimentID=expID,
                                         date=d,
                                         value=str(v)) for d,v in zip(*values)])
        session.commit()
        
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
    
    def writeToCmdLine(self):
        args=[]
        
        for name, (times, values) in self.parameters.items():
            for time,value in zip(times,values):
                if time is None:
                    valueType={int:"int", float: "float", bool:"bool"}[type(value)]
                    args.append("--param={:s},{:s},{:s}".format(name, valueType, str(value)))
                else:
                    dateStr="{:04d}{:02d}{:02d}".format(time.year, time.month, time.day)
                    if time.time()!=datetime.time():
                        dateStr+=" {:02d}:{:02d}:{:02d}".format(time.hour, time.minute, time.second)
                        if time.microsecond!=0:
                            dateStr+=".{:06d}".format(int(time.microsecond*1e6))
                    args.append("--param={:s},{:s},{:s}".format(name, dateStr, str(value)))
        
        return args
    
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
