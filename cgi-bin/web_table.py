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
            n_columnas = len(table["datos"][0])
            
            # rellenar nombres con cosas vacias en caso de que no coincidan
            for i in range(len(table["nombres"]), n_columnas):
                table["nombres"].append("")

            print("<table>")
            
            print("<tr>")
            for i in range(n_columnas):
                nombre = table["nombres"][i]
                print(f"<th>{nombre}</th>")
            print("</tr>")

            for f in table["datos"]:
                print("<tr>")
                for i in range(n_columnas):
                    print(f"<td>{f[i]}</td>")
                print("</tr>")
            
            print("</table>")
    file.close()