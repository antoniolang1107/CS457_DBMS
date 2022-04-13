import os
import shutil
import errno
import re
import csv

currDirect = rootDirect = os.getcwd()

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

	path = os.path.join(directory, name)
	if (directory == os.getcwd()):
			print ("No database selected")
	else:
		if (os.path.isfile(path+'.csv')):
			print ("!Failed to create table %s because it already exists." % name)
		else:
			# create CSV and save attributes
			splitToken = list(filter(None, re.split(', |\s|;+|\(()\)', schema)))
			colName = splitToken[0::2]
			datatypes = splitToken[1::2]

			fileName = os.path.join(directory, name+'.csv')
			with open (fileName, 'w') as newTable:
				csvwriter = csv.writer(newTable)
				csvwriter.writerow(colName)
			fileName = os.path.join(directory, name+'Attribute.csv')
			with open (fileName, 'w') as newAttributes:
				csvwriter = csv.writer(newAttributes)
				csvwriter.writerow(datatypes)

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
	:param name: 
	:return: no value returned
	"""

	try:
		path = os.path.join(directory, name+'.csv')
		os.remove(path)
		print("Table %s deleted." % name)
	except FileNotFoundError:
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

def query(directory, col, tableName):
	"""
	query reads the desired table and returns the desired column

	:param directory: directory of selected database
	:param col: desired attribute to select from
	:param tableName: name of the desired table to read
	:return: no value returned
	"""
	dataFileName = tableName + '.csv'
	data = []
	attributeFileName = tableName + 'Attribute.csv'
	attributes = []
	combined = []

	path = os.path.join(directory, dataFileName)
	path2= os.path.join(directory, attributeFileName)
	if (directory == os.getcwd()):
		print ("No database in use.")
	elif (os.path.isfile(path)):
		with open(path, mode = 'r') as file:
			csvFile = csv.reader(file)
			for lines in csvFile:
				data = lines
		with open(path2, mode = 'r') as file:
			csvFile = csv.reader(file)
			for lines in csvFile:
				attributes = lines

		for i in range(0,len(attributes)):
			combined.append(data[i] + ' ' + attributes[i])
		
		if (col == '*'):
			output = ' | '.join(combined)
			print (output)
		else:
			print (data)

	else:
		print ("!Failed to query table %s because it does not exist." % tableName)

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
			print ("Action unimplemented")
	else:
		print ("Table %s not found" % tblName)

def parser(inputCommand, direct):
	"""
	parser reads string input and calls appropriate function

	:param inputCommand: command entered by the user
	:param direct: is the current directory/database being accessed
	:return: returns the original directory if unchanged
	:return: returns False if "EXIT" is parsed from the inputCommand

	"""

	splitCommands = list(filter(None, re.split(', |\s|;+|\((.+)\)', inputCommand)))
	splitCommands[0] = splitCommands[0].upper()

	if (splitCommands[0] == 'EXIT' or splitCommands[0] == '.EXIT'):
		return False

	elif (splitCommands[0] == 'USE'):
		return use(os.getcwd(), splitCommands[1])

	elif (splitCommands[0] == 'CREATE'):
		splitCommands[1] = splitCommands[1].upper()
		if (splitCommands[1] == 'DATABASE'):
			createDB(os.getcwd(), splitCommands[2])
		elif (splitCommands[1] == 'TABLE'):
			createTBL(direct, splitCommands[2], splitCommands[3])

	elif (splitCommands[0] == 'DROP'):
		splitCommands[1] = splitCommands[1].upper()
		if (splitCommands[1] == 'DATABASE'):
			direct = os.getcwd()
			dropDB(direct, splitCommands[2])
		elif (splitCommands[1] == 'TABLE'):
			dropTBL(direct, splitCommands[2])
		else:
			print("Invalid syntax.")

	elif (splitCommands[0] == 'SELECT'):
		query(direct, splitCommands[1], splitCommands[3])

	elif (splitCommands[0] == 'ALTER'):
		splitCommands[3] = splitCommands[3].upper()
		alter(direct, splitCommands[2], splitCommands[3], splitCommands[4], splitCommands[5])

	else:
		print("Invalid syntax.")

	#returns last-used directory
	return direct



loop = True
while (loop):
	select = input()
	returnOutput = parser (select, currDirect)
	if (isinstance(returnOutput, bool)):
		loop = False;
		print ("All done.")
	else:
		currDirect = returnOutput
