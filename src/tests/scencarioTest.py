'''
Created on 8/12/2014

@author: achim
'''

import unittest
from ABM import scenario
import datetime
import json

class fromCmdLine(unittest.TestCase):
    
    def testSimple(self):
        s=scenario()
        s.readFromCmdLine(("--param=a,int,1", "--param=b,float,2.3", "--param=c,bool,1"))
        self.assertDictEqual(s.parameters, {"a": [(None,),(1,)], "b":[(None,), (2.3,)], "c": [(None,), (True,)]})

    def testWithDate(self):
        s=scenario()
        s.readFromCmdLine(("--param=a,int,1", "--param=a,20130101,2", "--param=a,20130102,3"))
        self.assertDictEqual(s.parameters,
                             {"a": [(None, datetime.datetime(2013,1,1), datetime.datetime(2013,1,2)),
                                    (1,2,3)]})

        self.assertEqual(s.getValue("a", datetime.datetime(2012,1,1)), 1)
        self.assertEqual(s.getValue("a", datetime.datetime(2013,1,1)), 2)
        self.assertEqual(s.getValue("a", datetime.datetime(2013,1,1, 0, 30)), 2)
        self.assertEqual(s.getValue("a", datetime.datetime(2013,1,2)),3)
        self.assertEqual(s.getValue("a", datetime.datetime(2014,1,1)), 3)

class timeConversion(unittest.TestCase):
    
    def testConv1(self):
        
        theDate=datetime.datetime(2014, 7, 1, 0,0,0,0)
        
        self.assertEqual(scenario.stringToDate('20140701'), theDate)
        self.assertEqual(scenario.stringToDate('20140701 00:00:00'), theDate)
        self.assertEqual(scenario.stringToDate('20140701 00:00:00.000'), theDate)

    def testConv2(self):
        
        theDate=datetime.datetime(2014, 7, 1, 1,2,3,4000)
        
        self.assertEqual(scenario.stringToDate('20140701 01:02:03.004'), theDate)
        self.assertEqual(scenario.stringToDate('20140701 01:02:03.0040'), theDate)
        self.assertEqual(scenario.stringToDate('20140701 01:02:03.00400'), theDate)
        self.assertEqual(scenario.stringToDate('20140701 01:02:03.004000'), theDate)
        self.assertEqual(scenario.stringToDate('20140701 01:02:03.0040001'), theDate)
        # here: truncate to microseconds precision
        self.assertEqual(scenario.stringToDate('20140701 01:02:03.00399999'), theDate)

        self.assertNotEqual(scenario.stringToDate('20140701 01:02:03.009'), theDate)
        self.assertNotEqual(scenario.stringToDate('20140701 01:02:03.009'), theDate)
        self.assertNotEqual(scenario.stringToDate('20140701 01:02:03.003999'), theDate)

    def testConv3(self):

        self.assertEqual(scenario.dateToString(datetime.datetime(2014, 7, 1, 1, 2, 3, 4000)),
                         '20140701 01:02:03.004')
        
        self.assertEqual(scenario.dateToString(datetime.datetime(2014, 7, 1, 1, 2, 3, 4)),
                         '20140701 01:02:03.000004')

        self.assertEqual(scenario.dateToString(datetime.datetime(2014, 7, 1, 1, 2, 3, 0)),
                         '20140701 01:02:03')

    def testFailedConvDate(self):
        self.assertRaises(ValueError, scenario.stringToDate, '2014070')
        self.assertRaises(ValueError, scenario.stringToDate, '201407010')
        self.assertRaises(ValueError, scenario.stringToDate, '20140e01')
        self.assertRaises(ValueError, scenario.stringToDate, '201d07010')

    def testFailedConvTime(self):
        self.assertRaises(ValueError, scenario.stringToDate, '20140701 ')
        self.assertRaises(ValueError, scenario.stringToDate, '20140701 12:30:00d')
        self.assertRaises(ValueError, scenario.stringToDate, '20140701 12.30.00')
        self.assertRaises(ValueError, scenario.stringToDate, '20140701_12:30:00')

    def testFailedConvMicrosec(self):
        self.assertRaises(ValueError, scenario.stringToDate, '20140701 0.000')
        self.assertRaises(ValueError, scenario.stringToDate, '20140701 12:30:00.0e')
        self.assertRaises(ValueError, scenario.stringToDate, '20140701 12:30:00.3e6')
        self.assertRaises(ValueError, scenario.stringToDate, '20140701 12:30:00.3e4')
        self.assertRaises(ValueError, scenario.stringToDate, '20140701 12:30:00.0000ee')

class loadJSON(unittest.TestCase):
    
    def testEmpty(self):
        
        s=scenario()
        self.assertDictEqual(s.parameters, {}, "expecting empty initial set of parameters")
        
        s=scenario()
        s.readFromJSON("{}")
        self.assertDictEqual(s.parameters, {}, "expecting empty initial set of parameters")

    def testSimpleData(self):
        
        s=scenario()
        s.readFromJSON("""{"a":1, "b":1.0, "c":true }""")
        
        self.assertDictEqual(s.parameters, {"a": [(None,),(1,)], "b": [(None,), (1.0,)], "c": [(None,), (True,)]})

    def testSimpleDataRW(self):
        s=scenario()
        s.parameters={"a": ([None,], [1,]),
                      "b": ([None,], [1.2,]),
                      "c": ([None,], [True,])}
        theJSON=s.writeToJSON()
        
        jsonStruct=json.loads(theJSON)
        
        self.assertSetEqual(set(jsonStruct.keys()), set(["a", "b", "c"]))
        self.assertIs(type(jsonStruct["c"]), bool)
        self.assertIn(type(jsonStruct["a"]), [float, int])
        self.assertIs(type(jsonStruct["b"]), float)

    def testTimeFormats(self):
        s=scenario()
        
        s.readFromJSON("""{"a": {"initial":1}}""")
        self.assertDictEqual(s.parameters, {"a": [(None,),(1,)]})

        s.readFromJSON("""{"a": {"initial":1, "20140101": ["set", 2]}}""")
        self.assertDictEqual(s.parameters, {"a": [(None,datetime.datetime(2014,1,1)),(1,2)]})

        s.readFromJSON("""{"a": {"initial":1, "20140101 00:30:00": ["set", 2]}}""")
        self.assertDictEqual(s.parameters, {"a": [(None,datetime.datetime(2014,1,1,0,30, 0)),(1,2)]})

        s.readFromJSON("""{"a": {"initial":1, "20140101 00:30:00.004": ["set", 2]}}""")
        self.assertDictEqual(s.parameters, {"a": [(None,datetime.datetime(2014,1,1,0,30, 0, 4000)),(1,2)]})

    def testIntFloat(self):
        s=scenario()
        s.readFromJSON("""{"a": {"initial":1.0, "20140101 00:30:00.004": ["set", 2]}}""")
        self.assertDictEqual(s.parameters, {"a": [(None,datetime.datetime(2014,1,1,0,30, 0, 4000)),(1.0,2.0)]})
        self.assertIs(type(s.parameters["a"][1][0]), float)
        self.assertIs(type(s.parameters["a"][1][1]), float)

    def testTimeOrder(self):
        s=scenario()
        s.readFromJSON("""{"a": {"initial":1.0, "20140101 00:30:00.0004": ["set", 2], "20140101 00:30:00": ["set", 3]}}""")
        self.assertDictEqual(s.parameters,
                             {"a": [(None, datetime.datetime(2014,1,1,0,30, 0), datetime.datetime(2014,1,1,0,30, 0, 400)),
                                    (1,3,2)]})
        
    def testErrorMsgs(self):
        s=scenario()
        
        # no exception 
        s.readFromJSON("""{"a": {"initial":1}}""")
        self.assertDictEqual(s.parameters, {"a": [(None,),(1,)]})
        
        # string is not acceptable value
        self.assertRaises(TypeError, s.readFromJSON, """{"a": "string"}""")
        self.assertDictEqual(s.parameters, {"a": [(None,),(1,)]})
        
        # initial value is missing
        self.assertRaises(KeyError, s.readFromJSON, """{"a": {"":1}}""")
        self.assertDictEqual(s.parameters, {"a": [(None,),(1,)]})

        # test invalid date format        
        self.assertRaises(ValueError, s.readFromJSON, """{"a": {"initial":1, "2":2}}""")
        self.assertRaises(ValueError, s.readFromJSON, """{"a": {"initial":1, "2014":2}}""")
        self.assertRaises(ValueError, s.readFromJSON, """{"a": {"initial":1, "20140101 ":2}}""")
        self.assertRaises(ValueError, s.readFromJSON, """{"a": {"initial":1, "20140101 12:20":2}}""")
        self.assertRaises(ValueError, s.readFromJSON, """{"a": {"initial":1, "20140101 12:20:0":2}}""")
        self.assertRaises(ValueError, s.readFromJSON, """{"a": {"initial":1, "2014011  12:20:0":2}}""")
        self.assertRaises(ValueError, s.readFromJSON, """{"a": {"initial":1, "20140101 12:20:00.3d":2}}""")

        self.assertRaises(ValueError,
                          s.readFromJSON,
                          """{"a": {"initial":1.0, "20140101 00:30:00.000": ["set", 2], "20140101 00:30:00": ["set", 3]}}""")


    def testGetValue(self):
        s=scenario()
        s.readFromJSON("""{"a": {"initial": 1.0}}""")
        self.assertEqual(s.getValue("a", datetime.datetime.now()), 1.0)
                
        s.readFromJSON("""{"a": {"initial":1.0, "20140101 00:30:00.0004": ["set", 2], "20140101 00:30:00": ["set", 3]}}""")
        self.assertEqual(s.getValue("a", datetime.datetime(2013,1,1)), 1.0)
        self.assertEqual(s.getValue("a", datetime.datetime(2014,1,2)), 2.0)
        self.assertEqual(s.getValue("a", datetime.datetime(2014,1,1, 0, 30)), 3.0)
        self.assertEqual(s.getValue("a", datetime.datetime(2014,1,1, 0, 30, 0, 400)), 2.0)
                