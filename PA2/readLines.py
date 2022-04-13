import re

text = ""
stopword = ";"
while True:
    line = input()
    if ';' in line:
        text += "%s\n" % line
        break
    text += "%s\n" % line
print (text)

print ("after parser")
splitCommands = list(filter(None, re.split(', |\s|;+|\((.+)\)', text)))
splitCommands[0] = splitCommands[0].upper()

print (splitCommands)