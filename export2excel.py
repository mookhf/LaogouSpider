import sqlite3
from xlsxwriter.workbook import Workbook
workbook = Workbook('output2.xlsx')
worksheet = workbook.add_worksheet()

conn=sqlite3.connect('pos.sqlite')
c=conn.cursor()
mysel=c.execute("select * from pos")
for i, row in enumerate(mysel):
    for j, value in enumerate(row):
        worksheet.write(i, j,row[j])
workbook.close()
