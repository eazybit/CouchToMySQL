#!/usr/bin/python
# Filename : hello.py

import couchdb
import time
import json
import MySQLdb

_host='localhost'
_usr='root'
_pwd='Sql.yuzhang#06'
_db='test_db'
_table='customer'
_field='customerName'


couch = couchdb.Server()
couch_db = couch['new_database']

# get the last sequence number
changes = couch_db.changes()
last = changes['last_seq']

# continuous geting changes
changes = couch_db.changes(feed='continuous', heartbeat='1000', include_docs=True, since=last-1)
  
# handle each change
for line in changes:
	doc = line['doc']
	json_doc = json.dumps(doc)
	j = json.loads(json_doc)

	# if the changed data has a key called 'database', change the default to user required	
	if 'database' in j.keys():
		_db = j['database']

	if 'table' in j.keys():
		_table = j['table']

	# connnect to mysql server
	mysql_db = MySQLdb.connect(host=_host, user=_usr, passwd=_pwd, db=_db)

	# if successfully connect to mysql server, handle the data
	if (mysql_db > 0) and ('name' in j.keys()) and (line['seq'] > last):
		last = line['seq']
		cur = mysql_db.cursor() 
		name = j['name']
		query = """INSERT INTO %s (%s) VALUES ('%s');""" % (_table, _field, name)
		try:
			cur.execute(query)
			mysql_db.commit()
			print query
		except:
			mysql_db.rollback()

	# close mysql connection
	mysql_db.close()

couch_db.close()
