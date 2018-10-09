# -*- coding: utf-8 -*-
"""
Created on Tue Jul 17 09:26:58 2018

@author: mohsheik
"""

import re

#res = re.search('^\s*[0-9]',"123. Starts with one")
res_num = re.findall('^\s*[0-9]*', "667890 Starts with one 345 45435")

print (res_num)
