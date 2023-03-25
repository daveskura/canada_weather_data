"""
  Dave Skura
  
  File Description:
"""
import sys
import socket
from beststation import beststation
from sqlitedave_package.sqlitedave import sqlite_db

def main():
	x = doit()

class doit():
	def __init__(self):
		self.db = sqlite_db()
		self.WHO = socket.gethostname()

		self.obj = beststation()

		stillworking = True
		while stillworking:
			postalcode = self.getnextone()
			self.updateworkingon(postalcode,'work on it')
			self.obj.output_postalcode_distance(postalcode)
			self.updateworkingon(postalcode,'completed')

		self.db.close()

	def updateworkingon(self,postalcode='',isdoingwhat='work on it'):
		sql = "update whatarewedoing SET who='" + self.WHO + "',isdoingwhat='" + isdoingwhat + "' WHERE postalcode = '" + postalcode + "'"
		print(sql)
		self.db.execute(sql)

	def getnextone(self):
		sql = "SELECT MIN(postalcode) FROM whatarewedoing WHERE who = ''"
		postalcode = self.db.queryone(sql)
		if postalcode:
			return postalcode
		else:
			print('No Postal Codes left in whatarewedoing table')
			sys.exit(0)

if __name__ == '__main__':
	main()




