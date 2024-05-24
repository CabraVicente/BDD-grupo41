#!/usr/bin/python3

# enable debugging
import cgitb
cgitb.enable()

def print_table(table):
    file = open("cgi-bin/table.html")
    for line in file.readlines():
        if (line != "<!--tabla-->\n"):
            print(line)
        else:
            print(table)
    file.close()