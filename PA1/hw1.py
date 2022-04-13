import os
import errno


# while(True)
# wait for input
# parse input
# execute in proper order

# change path or directory when using database

while (True):
	command = input("")
	#command = command.split()
	command = command.upper()
	#command[0] = command[0].upper()
	#ommand[1] = command[1].upper()
	#print(command)

	if (command == "EXIT"):
		print("All done.")
		break;


directory = "testDir"
parent_dir = os.getcwd()

#parent_dir = "/Users/tonylslame/Documents/Code/CS 457"

path = os.path.join(parent_dir, directory)


try:
	os.mkdir(path)
	print("Directory '% s' created" % directory)
except OSError as e:
	if e.errno == errno.EEXIST:
		print("!Failed to create database " + directory + " because it already exists.")


directory = "testRemove"
path = os.path.join(parent_dir, directory)

try:
	os.rmdir(path)
	print("hi from try")
	print(directory + " deleted.")
except OSError as e:
	if e.errno == errno.EEXIST:
		print("!Failed to delete " + directory + " because it does not exist.")

# os.remove() for tables