import pandas as pd
import os

testDirect = os.getcwd()
testDirect = os.path.join(testDirect, 'db2/tbl_3.json')
lastPath = os.path.basename(testDirect)
output = os.path.join(os.getcwd(), 'db2')
output = os.path.basename(output)

print (lastPath)
print (output)
# df = pd.read_json(testDirect, orient = 'table')
df = pd.read_json(testDirect, orient = 'table')

print (df)
print (df.dtypes)