import os
import shutil
import errno
import re
import csv
import pandas as pd

def createDB(directory, name):
	"""
	createDB creates a database in the root folder if available

	:param directory: root directory
	:param name: desired name of the database
	:return: no value returned
	"""
	try:
		os.mkdir(os.path.join(os.getcwd(), name))
		print("Database % s created." % name)
	except OSError as e:
		if e.errno == errno.EEXIST:
			print("!Failed to create database %s because it already exists." % name)

def createTBL(directory, name, schema):
	"""
	createTBL creates a table in a given database if available

	:param directory: currently selected database
	:param name: desired name of the table
	:param schema: token representing the desired column names and primitives
	:return: no value returned
	"""

	name = name.lower()

	path = os.path.join(directory, name)
	if (directory == os.getcwd()):
			print ("No database selected")
	else:
		if (os.path.isfile(path+'.csv')):
			print ("!Failed to create table %s because it already exists." % name)
		else:
			# create JSON and save attributes in CSV
			splitToken = list(filter(None, re.split(', |\s|;+|\(()\)', schema)))
			colName = splitToken[0::2]
			datatypes = splitToken[1::2]
			datatypesCopy = datatypes.copy()

			# changes variable keywords into respective class
			for i,entry in enumerate(datatypes):
				datatypes[i] = keywordToClass(datatypes[i])
			
			# zips together the variable names and datatypes in a dictionary
			dataTypes = dict(zip(colName, datatypes))

			# assigns data types to columns of newTable DataFrame
			newTable = pd.DataFrame(columns = colName)
			newTable = newTable.astype(dataTypes)

			# saves the table with columns
			fileName = os.path.join(directory, name+'.json')
			newTable.to_json(fileName, orient = 'table', indent = 4)

			# saves attirubte names
			fileName = os.path.join(directory, name+'Attributes.csv')
			with open (fileName, 'w') as newAttributes:
				csvwriter = csv.writer(newAttributes)
				csvwriter.writerow(datatypesCopy)

			print ("Table %s created." % name)

def dropDB(directory, name):
	"""
	dropDB deletes a desired database if available

	:param: directory: root directory
	:param: name: name of database to delete
	:return: no value returned
	"""

	try:
		path = os.path.join(directory, name)
		shutil.rmtree(path)
		print("Database %s deleted." % name)
	except FileNotFoundError:
		print("!Failed to delete %s because it does not exist." % name)

def dropTBL(directory, name):
	"""
	dropTBL drops a desired table if available

	:param directory: currently selected database
	:param name: name of the table to drop
	:return: no value returned
	"""

	dataPath = os.path.join(directory, name+'.json')
	attributePath = os.path.join(directory, name+'Attributes.csv')

	if(os.path.isfile(dataPath)):
		os.remove(dataPath)
		os.remove(attributePath)
		print("Table %s deleted." % name)
	else:
		print("!Failed to delete %s because it does not exist." % name)

def use(directory, name):
	"""
	use selects a database to access if available

	:param directory: root directory
	:param name: name of database to access
	:return: returns the directory of the database
	"""
	if(os.path.isdir(name)):
		print ("Using database %s." % name)
		return os.path.join(directory, name)
	else:
		print ("!Failed to use database beacuse it does not exist")

def query(directory, cols, tableNames, condition):
	"""
	query reads the desired table and returns the desired column

	:param directory: directory of selected database
	:param col: desired attribute to select from
	:param tableName: name of the desired table to read
	:return: no value returned
	"""
	# print (cols)
	# print (tableNames)
	# print (condition)


	tableData = pd.DataFrame()
	attributeNames = []
	attributeTypes = []
	outputData = []

	if (len(tableNames) == 1):
		tableName = tableNames[0].lower()
		tableData, attributeNames, attributeTypes = readTable(directory, tableName)
		dataLine = [[] for i in range(len(tableData))]
		header = []
		conditionGiven = (len(condition) > 0)

		# finds the index of the column used for comparison
		if conditionGiven:
			conditionColumnIndex = tableData.columns.get_loc(condition[0]) + 1

			# removes any ' in value to compare against
			if "'" in condition[2]:
				condition[2] = condition[2].replace("'", "")

		# checks if table exists
		if (len(attributeNames) != 0):
			if (cols[0] == '*'):
				# creates 'header' for output
				for i in range(0,len(attributeTypes)):
					header.append(attributeNames[i] + ' ' + attributeTypes[i])
				for i, row in enumerate(tableData.itertuples()):
					for j in range(1, len(row)):
						dataLine[i].append(str(row[j]))
				
			else:
				dataIndicies = []

				# creates a dataframe with the desired columns
				subTable = tableData[cols]

				# iterates over desired columns and attributeNames to get the proper index
				for j, tableColumn in enumerate(attributeNames):
					for i, givenCol in enumerate(cols):
						if (givenCol == tableColumn): # if desired column in data
							# creates header
							header.append(attributeNames[j] + ' ' + attributeTypes[j])
							# adds the index of the column to retreive data later
							dataIndicies.append(i+1)

				# iterates over each row in the DataFrame
				for i, row in enumerate(subTable.itertuples()):
					for j in dataIndicies:
						if (conditionGiven):
							# pulls data if it fits the given condition
							conditionTrue = evalComparison(row[conditionColumnIndex], condition[1], condition[2])
							if (conditionTrue):
								dataLine[i].append(str(row[j]))
						# appens row for output if there is no condition given
						else:
							dataLine[i].append(str(row[j]))
				# iterrate over the rows and pull data from the proper column

			# filters out any empty rows
			dataLine = list(filter(None, dataLine))

			# formats each output row
			for value in dataLine:
				mergedData = ' \t| '.join(value)
				outputData.append(mergedData)

			# formats the header for output
			mergedOutput = ' | '.join(header)

			# prints output
			print (mergedOutput)
			for x in outputData:
				print(x)

		else:
			print ("!Failed to query table %s because it does not exist." % tableName)
	elif (len(tableNames) == 4):

		header = []
		outputData = []

		# reads in the two tables
		tblData1, attrNames1, attrTypes1 = readTable(directory, tableNames[0])
		tblData2, attrNames2, attrTypes2 = readTable(directory, tableNames[2])
		tblAbb1 = tableNames[1]
		tblAbb2 = tableNames[3]

		# gets variable table name to compare later
		condition1 = condition[0].split('.')
		condition2 = condition[2].split('.')

		# gets the two columns within the tables for comparison
		tblCond1 = tblData1.loc[:,(condition1[1])]
		tblCond2 = tblData2.loc[:,(condition2[1])]

		# gets column names from given tables
		combinedCols = attrNames1.values.tolist() + attrNames2.values.tolist()

		# creates new DataFrame with the columns and datatypes from given tables
		joinTable = pd.DataFrame(columns = combinedCols)
		joinTable = joinTable.astype(tblData1.dtypes)
		joinTable = joinTable.astype(tblData2.dtypes)

		tempDF = joinTable.copy()

		if (condition1[0] == tblAbb1 and condition2[0] == tblAbb2):

			i = 0
			j = 0
			for row1 in tblCond1.iteritems():
				j = 0
				for row2 in tblCond2.iteritems():
					if (row1[1] == row2[1]):
						# takes the data from the left and right tables and combines them
						tempDF.loc[0, tblData1.columns] = tblData1.iloc[i]
						tempDF.loc[0, tblData2.columns] = tblData2.iloc[j]
						tempDF = tempDF.astype(joinTable.dtypes)

						# adds the combined row into the joined table
						joinTable = pd.concat([joinTable, tempDF], ignore_index = True)
						
					j += 1
				i += 1

			dataLine = [[] for i in range(len(joinTable))]

			for i in range(0, len(attrNames1)):
				header.append(attrNames1[i] + ' ' + attrTypes1[i])
			for i in range(0, len(attrNames2)):
				header.append(attrNames1[i] + ' ' + attrTypes2[i])
			
			for i, row in enumerate(joinTable.itertuples()):
				for j in range(1, len(row)):
					dataLine[i].append(str(row[j]))

			# formats each output row
			for value in dataLine:
				mergedData = ' \t| '.join(value)
				outputData.append(mergedData)

			# formats the header for output
			mergedOutput = ' | '.join(header)

			# prints output
			print (mergedOutput)
			for x in outputData:
				print(x)

		else:
			
			print ("Table not found")
	else:
		printError()


def evalComparison(value1, operator, value2):
	'''
	evalComparison performs boolean comparisons from passed input
	
	:param value1: first value for comparison
	:param operator: boolean operation desired
	:param value2: second value for comparison
	:return: result of boolean comparison
	'''
	value2 = type(value1)(value2)
	if operator == '>':
		return value1 > value2
	elif operator == '>=':
		return value1 >= value2
	elif operator == '=':
		return value1 == value2
	elif operator == '<=':
		return value1 <= value2
	elif operator == '<':
		return value1 < value2
	elif operator == '!=':
		return value1 != value2

def alter(directory, tblName, action, varName, attribute):
	"""
	alter adds a column to an existing table if available

	:param directory: directory of currently-selected database
	:param tblName: name of desired table to alter
	:param action: 'ADD'
	:param varName: name of column to add
	:param attribute: type of primitive to add
	:return: no value returned
	"""

	path = os.path.join(directory, tblName)
	dataFileName = path + '.csv'
	attributeFileName = path + 'Attribute.csv'
	if (directory == os.getcwd()):
		print ("No database selected.")
	elif (os.path.isfile(dataFileName)):
		if (action == 'ADD'):
			tempData = path + 'temp.csv'
			tempAttributes = path + 'tempAttribute.csv'

			with open(dataFileName, 'r') as read_obj, open(tempData, 'w', newline='') as write_obj:
				csv_reader = csv.reader(read_obj)
				csv_writer = csv.writer(write_obj)
				for row in csv_reader:
					row.append(varName)
					csv_writer.writerow(row)
			with open(attributeFileName, 'r') as read_obj, open(tempAttributes, 'w', newline='') as write_obj:
				csv_reader = csv.reader(read_obj)
				csv_writer = csv.writer(write_obj)
				for row in csv_reader:
					row.append(attribute)
					csv_writer.writerow(row)
			os.rename(tempData, dataFileName)
			os.rename(tempAttributes, attributeFileName)
			print ("Table %s modified." %tblName)
		else:
			print ("Action unavailable")
	else:
		print ("Table %s not found" % tblName)


def insert(directory, commands):
	'''
	insert takes in data a user wants to add and appends it to the table

	:param directory: directory of selected database
	:param commands: raw input from the command line
	:return: none
	'''

	# removes specific characters and separates tokens into splitData list
	commands[4] = commands[4].replace('\'', '')
	commands[4] = commands[4].replace('\"', '')
	splitData = list(filter(None, re.split(',|\s|;+|\((.+)\)', commands[4])))

	# changes entered data from strings into proper data type
	for i, data in enumerate(splitData):
		splitData[i] = inputToObject(data)

	# reads in table
	tableData, attributeNames, attributeTypes = readTable(directory, commands[2])
	dataFileName = commands[2] + '.json'
	dataPath = os.path.join(directory, dataFileName)

	if (len(splitData) == len(attributeTypes)):
		splitData = [splitData] # converts inputted data into form for data frame

		addValueDF = pd.DataFrame(splitData, columns = attributeNames)
		tableData = pd.concat([tableData, addValueDF], ignore_index = True)
		tableData.to_json(dataPath, orient = 'table', indent = 4)
		
		print ("%d new record inserted." %1)
	else:
		print ("!Error column count does not match the table")


def inputToObject(data):
	"""
	inputToObject checks if a float or int exists in a string and changes it if it does

	:param data: given string to check
	:return: float or int class
	"""
	checkFloat = re.findall("\d+\.\d+", data)
	if (len(checkFloat) > 0):
		return float(checkFloat[0])
	if (data.isnumeric()):
		return int(data)

	return data


def readTable(directory, tableName):
	"""
	readTable reads in the data for a given table in possible

	:param directory: directory of selected database
	:param tableName: name of the table to read
	:return: DataFrame with data, attributeNames with the column names, attributeTypes with original variable names
	"""
	dataFileName = tableName + '.json'
	attributeFileName = tableName + 'Attributes.csv'
	tableData = pd.DataFrame()
	attributeNames = []
	attributeTypes = []

	dataPath = os.path.join(directory, dataFileName)
	attributePath = os.path.join(directory, attributeFileName)


	if (directory == os.getcwd()):
		print ("No database in use.")
	elif (os.path.isfile(dataPath)):
		tableData = pd.read_json(dataPath, orient = 'table')
		attributeNames = tableData.columns
		
		with open(attributePath, mode = 'r') as file:
			csvFile = csv.reader(file)
			for lines in csvFile:
				attributeTypes = lines
		# creates output "header" for the entries

	return tableData, attributeNames, attributeTypes


def writeTable(directory, tableName, table, attributeTypes):
	'''
	writes a given table to appropriate files

	:param directory: directory of current database in use
	:param tableName: table of the desired table
	:param table: DataFrame with the table's data
	:param attributeTypes: original names of given columns
	return: no value returned
	'''
	tableFileName = tableName + '.json'
	tablePath = os.path.join(directory, tableFileName)

	table.to_json(tablePath, orient = 'table', indent = 4)


def update(directory, tableName, updateCol, updateVal, compareOperator, conditionCol, conditionVal):
	'''
	update chnages values in a column based on a given condition

	:param directory: directory of current database in use
	:commands: user-inputted commands
	:return: no value returned
	'''

	tableData, attributeNames, attributeTypes = readTable(directory, tableName)
	numUpdated = 0

	# changes a numeric string argument into int
	if (updateVal.isnumeric()):
		updateVal = int(updateVal)

	# if the table exists, then check values to update
	if (len(attributeNames) > 0):
		conditionColData = tableData.loc[:,(conditionCol)]

		# iterates over the conditional column and updates desired column
		for i, value in enumerate(conditionColData):
			condition = evalComparison(value, compareOperator, conditionVal)

			if condition:
				tableData.at[i, updateCol] = updateVal
				numUpdated += 1

		# writes updated data to disc
		writeTable(directory, tableName, tableData, attributeTypes)

		if (numUpdated == 1):
			print ("%d record modified." % numUpdated)
		else:
			print ("%d records modified." % numUpdated)

	# table does not exist -> display error
	else:
		print ("!Error table %s doesn't exist." % tableName)


def delete(directory, commands):
	'''
	delete removes values from a given table based on given conditions

	:param directory: current database in use
	:param commands: user-entered input	
	:return: no return
	'''

	tableData, attributeNames, attributeTypes = readTable(directory, commands[2])
	numDeleted = 0

	# iterates over the user-passed command and removes ' from any strings
	for i, value in enumerate(commands):
		if '\'' in value:
			commands[i] = commands[i].replace('\'','')

	# checks if given table exists
	if (len(attributeNames) > 0):
		conditionCol = tableData[[commands[4]]]

		# changes a numeric string argument into int or float
		if (commands[6].isnumeric()):
			commands[6] = int(commands[6])
		if (isinstance(commands[6], str)):
			floatInStr = re.findall("\d+\.\d+", commands[6])
			if (len(floatInStr) > 0):
				commands[6] = float(commands[6])


		# iterates through the 'conditional' column
		# reads boolean operator and follows proper branch with corresponing operator
		# appends True if the checked entry should be removed and False otherwise
		deleteRows = []

		# iterates over the rows and appends true or false based on the condition
		for row in conditionCol.itertuples():
			compare = evalComparison(row[1], commands[5], commands[6])

			# remove any row that corresponds to false
			if compare:
				deleteRows.append(False)
			else:
				deleteRows.append(True)

		# constructs a new DataFrame based on True and False values from deleteRows
		newTable = tableData[[x == True for x in deleteRows]]

		# num deleted rows = original num rows - modified num rows
		numDeleted = tableData.shape[0] - newTable.shape[0]

		# writes updated data to disc
		writeTable(directory, commands[2], newTable, attributeTypes)

		if (numDeleted == 1):
			print ("%d record deleted." % numDeleted)
		else:
			print ("%d records deleted." % numDeleted)

	# table does not exists -> display error
	else:
		print("!Error table %s does not exist" % commands[2])


# def innerJoin(database, leftTableName, leftAttribute, rightTableName, rightAttribute):
# 	leftTable = (database, leftTableName)

# 	rightTable = (database, rightTableName)
	
	
# 	print()


def parser(inputCommand, direct):
	"""
	parser reads string input and calls appropriate function

	:param inputCommand: command entered by the user
	:param direct: is the current directory/database being accessed
	:return: returns the original directory if unchanged
	:return: returns False if "EXIT" is parsed from the inputCommand
	"""

	# parses the user input into tokens and groups anything within parenthesis as one token
	splitCommands = list(filter(None, re.split(', |\s|;+|\((.+)\)', inputCommand)))
	splitCommands[0] = splitCommands[0].upper()

	if (len(splitCommands) > 0):

		# following else-if statements call the correct function based on input
		if (splitCommands[0] == 'EXIT' or splitCommands[0] == '.EXIT'):
			return False

		# USE given database
		elif (splitCommands[0] == 'USE'):
			return use(os.getcwd(), splitCommands[1])

		# CREATE a table or database
		elif (splitCommands[0] == 'CREATE'):
			splitCommands[1] = splitCommands[1].upper()
			if (splitCommands[1] == 'DATABASE'):
				createDB(os.getcwd(), splitCommands[2])
			elif (splitCommands[1] == 'TABLE'):
				createTBL(direct, splitCommands[2], splitCommands[3])

		# DROP a table or database
		elif (splitCommands[0] == 'DROP'):
			splitCommands[1] = splitCommands[1].upper()
			if (splitCommands[1] == 'DATABASE'):
				direct = os.getcwd()
				dropDB(direct, splitCommands[2])
			elif (splitCommands[1] == 'TABLE'):
				dropTBL(direct, splitCommands[2])
			else:
				print("Invalid syntax.")

		# SELECT data from tables
		elif (splitCommands[0] == 'SELECT'):
			fromIndex = splitCommands.index('from')
			condition = []
			whereIndex = 0
			if 'where' in splitCommands:
				# creates a 'condition' list after 'where' keyword until the end
				whereIndex = splitCommands.index('where')
				condition = splitCommands[whereIndex+1: len(splitCommands)]
			# creates a list of desired columns between 'SELECT' keyword and 'from'
			columns = splitCommands[1:fromIndex]
			query(direct, columns, splitCommands[fromIndex+1:whereIndex], condition)

		# ALTER table data
		elif (splitCommands[0] == 'ALTER'):
			splitCommands[3] = splitCommands[3].upper()
			alter(direct, splitCommands[2], splitCommands[3], splitCommands[4], splitCommands[5])

		# INSERT new data into the table
		elif (splitCommands[0] == 'INSERT'):
			if (splitCommands[1].upper() == 'INTO'):
				insert(direct, splitCommands)
			else:
				printError()
		
		# UPDATE entries based on given condition
		elif (splitCommands[0] == 'UPDATE'):
			# iterates over the user-passed command and removes ' from any strings
			for i, value in enumerate(splitCommands):
				if '\'' in value:
					splitCommands[i] = splitCommands[i].replace('\'','')
			update(direct, splitCommands[1], splitCommands[3], splitCommands[5], splitCommands[8], splitCommands[7], splitCommands[9])
		# DELETE entries based on given condition
		elif (splitCommands[0] == 'DELETE' and splitCommands[1].upper() == 'FROM'):
				delete(direct, splitCommands)

		else:
			printError()

	#returns last-used directory
	return direct


def printError():
	print("Invalid syntax.")


def keywordToClass(keyword):
	"""
	keywordToClass converts a given keyword into a class

	:param keyword: string representing desired class
	:return: class represented by a string
	"""

	if 'varchar' in keyword:
		return str
	elif 'char' in keyword:
		return str
	elif 'float' in keyword:
		return float
	elif 'double' in keyword:
		return float
	elif 'int' in keyword:
		return int
	else:
		return keyword


def main():
	"""
	main handles taking in input from the user

	:return: null
	"""

	currDirect = os.getcwd()
	loop = True
	while (loop):

		# continues reading lines until ';' is reached
		select = ""
		while True:
			line = input("-->")
			if ';' in line or '.exit' in line:
				select += "%s\n" % line
				break
			select += "%s\n" % line
		
		returnOutput = parser (select, currDirect)
		if (isinstance(returnOutput, bool)):
			loop = False
			print ("All done.")
		else:
			currDirect = returnOutput


if __name__ == "__main__":
	main()