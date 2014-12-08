'''
Created on 8/12/2014

@author: achim
'''

import unittest
from ABM import scenario
import datetime

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
                