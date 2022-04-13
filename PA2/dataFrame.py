from numpy import False_, diff
import pandas as pd
import re

exmplData = 'CREATE TABLE tbl_1 (a1 int, a2 varchar(20));'
splitCommands = list(filter(None, re.split(', |\s|;+|\((.+)\)', exmplData)))
splitToken = list(filter(None, re.split(', |\s|;+|\(()\)', splitCommands[3])))
#print (splitToken) # data type token separated
colName = splitToken[0::2]
datatypes = splitToken[1::2]

#print (colName)
#print (datatypes)

for i,entry in enumerate(datatypes):
    if 'varchar' in entry:
        datatypes[i] = str
    if 'float' in entry:
        datatypes[i] = float
    if 'int' in entry:
        datatypes[i] = int



Types = dict(zip(colName, datatypes))
# print (Types)

df = pd.DataFrame(columns = colName)
df = df.astype(Types)

# probably do .json instead of .csv

#print(df)
#print(df.dtypes)

newEntry = pd.DataFrame([[1, 'hi'], [2, 'bye']], columns = ['a1', 'a2'])
newEntry2 = pd.DataFrame([[1, 'higuy'], [3, 'byeguy']], columns = ['a1', 'a2'])
df = pd.concat([df,newEntry], ignore_index = True)
df = pd.concat([df, newEntry2], ignore_index = True)

passedLabel = ['a1']
conditionCol = df[passedLabel]
deleteIndex = []

for row in conditionCol.itertuples():
    print (row[1])
    if row[1] > 1:
        # deleteIndex.append(row[0])
        deleteIndex.append(False)
    else:
        deleteIndex.append(True)

delData = df[[x == False for x in deleteIndex]]
print (delData)
# df.drop(1)
# print (df)
# print (deleteIndex)


# print (difference)
# df.to_json("pandasOut.json", orient = 'table', indent = 4)

# data4 = data[[x == "yes" for x in my_list]]         # Using list to remove rows
# print(data4)            

# data3 = data[(data["x3"] != 5) & (data["x1"] > 2)]  # Multiple logical conditions
# print(data3)  

#df.to_csv("pandasOut.csv")

'''
attributes = pd.read_csv("db_1/tbl_1Attribute.csv")
data = pd.read_csv("db_1/tbl_1.csv")

#print("attributes DF")
#print(attributes.dtypes)

#print (data.dtypes)
newTypes = {'a1': int,
        'a2': str,
        'a3': float}
data = data.astype(newTypes)
#print (data.dtypes)
newEntry = pd.DataFrame([[1, 'hi', 4.20], [2, 'bye', 3]], columns = ['a1', 'a2', 'a3'])
data = pd.concat([data,newEntry], ignore_index = True)


print (attributes)
print (data)
'''

# columns and dtypes attributes are useful