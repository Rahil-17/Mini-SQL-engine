# Assignment 1:	Mini SQL Engine
# Author:		Rahil Sheth
# Roll number:	20172062

import csv				# to read .csv files
import sys				
import numbers			# aggregate functions
import re 				# regex
import os 				# to open files
sys.path.insert(0,os.getcwd() + "/sqlparse-0.2.4")

import sqlparse			# in built sql parser
from sqlparse.sql import Where


mdDictionary = {} 		# to store schema
parsedQ = {}			# parsed Query
idList = []				# tokens generated after going parsing

def readMetaData():								# reading metadata and populating schema Dictionary
	if not os.path.exists('metadata.txt'):
		print "File doesn't exist"
		sys.exit()

	file = open('metadata.txt','r')
	flag = 0
	tableName = {}
	for i in file:
		i = i.strip()
		if i == "<begin_table>":
			flag = 1
			continue

		if flag == 1:
			tableName = i
			mdDictionary[tableName] = []
			flag=0

		if flag==0 and i != "<end_table>":
			mdDictionary[tableName].append(i)		

	for i in mdDictionary:
		mdDictionary[i].remove(i)		
				
def readTable(filename):						# read table and store it in list
	tableName = filename + '.csv'
	if (os.path.exists(tableName)):
		file = open(tableName,'r')
		table = csv.reader(file)
		result = []
	
		for line in table:
			line = [cell.replace(" ","") for cell in line]
			line = [ int (i) for i in line]
			
			result.append(line)
		return result		
	else:
		return -1;	

def readTables():								# read multiple tables and join them (also for more than 2 tables)
	tableName = idList[3].split(",")
	table = readTable(tableName[0])
	
	if(table == -1):
		return -1

	else:
		for i in xrange(1,len(tableName)):
			temp = []
			t2 = readTable(tableName[i])
			if(t2 == -1):
				return -1
	
			else:
				for j in xrange(0,len(table)):
					for k in xrange(0,len(t2)):
						temp.append(table[j] + t2[k])
						pass
				table = temp
	
	return table							# final joined tables				

def processAttributes(attributes):				# changing attributes semantics ( A ==> table1.A)
	tablename = ""
	flag=0
	
	for i in range(0, len(attributes)):
		
		if ('.' not in attributes[i]):
			if not os.path.exists('metadata.txt'):
				print "File doesn't exist"
				sys.exit()

			file = open('metadata.txt')
			for line in file:	
				line = line.strip()
				if flag==0 and line in idList[3]:
					flag=1
					tablename = line
			
				elif flag==1 and line == attributes[i]:
					attributes[i] = (tablename + "." + line)
					
					
				elif flag==1 and line == "<end_table>":
					 flag=0 	 
				else:
					pass	 

	file.close()
	return attributes

def processWhere(table, where_condition):		# process where clause and evaluate the conditions
	
	cond = re.split('AND|OR',where_condition.upper())
	connectors = []
	finaltable = []
	check_connectors = where_condition.upper().split(" ");
	for i in check_connectors:
		if i=="OR" or i=="AND":
			connectors.append(i.lower())

	conditionInParts = processCondition(cond)		
	newCondition = ""
	
	for i in range(0, len(connectors)):
		newCondition += str(conditionInParts[i]) + " "
		newCondition += str(connectors[i]) + " "

	newCondition += str(conditionInParts[len(conditionInParts) - 1])

	for x in xrange(0,len(table)):
		if eval(newCondition):
			finaltable.append(table[x])	
			
	return finaltable										# final table containing tuples that satisfies the condtions there in where clause 

def processCondition(condition):				# process condition in where clause for evaluation in future
	tables = idList[3].split(",")
	newCondition = []
	length = 0
	for cond in condition:					# check operator
		cond = cond.replace(" ","")

		if ">=" in cond:
			op=">="

		elif "<=" in cond:
			op="<="

		elif ">" in cond:
			op=">"

		elif "<" in cond:
			op="<"

		elif "!=" in cond:
			op="!="

		elif "=" in cond:
			op="=="
			cond =  cond.replace('=','==')
		else:
			pass

		operands = cond.split(op)
		
		flag=0
		count=0
		for i in mdDictionary.keys()[::-1]:				# pre processing conditions
			if i in tables:
				for j in mdDictionary[i]:
					if '.' in operands[0]:
						temp_operand = operands[0].split(".")
						if temp_operand[1] == j and temp_operand[0].lower() == i.lower():
							operands[0] = "table[x][" + str(count) + "]"
							flag=1	


					if operands[0] == j:
						operands[0] = "table[x][" + str(count) + "]"
						flag=1
					count+=1		
		if flag==0:
				print "ERROR! Enter valid where clause"
				sys.exit()			
									
		
		if (not operands[1].isdigit() and '-' not in operands[1]):

			flag=0
			count=0
			for i in mdDictionary.keys()[::-1]:
				if i in tables:
					for j in mdDictionary[i]:

						if '.' in operands[1]:
							temp_operand = operands[1].split(".")
							if temp_operand[1] == j and temp_operand[0].lower() == i.lower():
								operands[1] = "table[x][" + str(count) + "]"
								flag=1

						if operands[1] == j:
							operands[1] = "table[x][" + str(count) + "]"
							flag=1
						count+=1
							
			if flag==0:
				print "ERROR! Enter valid where clause"
				sys.exit()			
					
		temp = str(operands[0] + str(op) + operands[1])

		newCondition.append(temp)
		length+=1
		
	return newCondition
													
def check_columns():							# check validity of columns provided in query with respect to tables
	attributes = idList[1].split(",")
	tables = idList[3].split(",")

	for att in attributes:
		
		if "*" == att and len(attributes)==1:
			pass

		elif "*" == att and len(attributes)!=1:
			return "false"	

		else:
			if "." in att:
				parts = att.split(".")
				if parts[0] not in tables:
					return "false"

				if parts[1] not in mdDictionary[parts[0]]:
					return "false"

			else:		
				flag = 0	
				flag1= 0		
				attribute_num=0
				col = []
				if not os.path.exists('metadata.txt'):
					print "File doesn't exist"
					sys.exit()

				file = open('metadata.txt')
				for line in file:
					line = line.strip()
			
					if line == "<begin_table>":
						flag = 1

					elif flag==1 and line in tables:
						flag = 0
						flag1 = 1

					elif flag1==1:
						
						if line == "<end_table>":
							flag1=0

						elif line==att:
							col.append(attribute_num)
							attribute_num+=1

						elif line!=att:		
							attribute_num+=1

						else:
							pass

					else:
						pass

				file.close()
			
				if(len(col) != 1):
					return "false"
	return "true"					

def printTable(attributes,table):				# display final content on terminal
	attribute_header = ",".join(attributes)
	data = ""

	for row in table:
		temp = []
		for num in row:
			temp.append(str(num)) 
		data = data + "\n" + ",".join(temp)
	print attribute_header + "\n" + data

def processQuery(q):							# parsing string to get identifiers and type of query
	try:
		parsedQ = sqlparse.parse(q)[0].tokens
		queryType = sqlparse.sql.Statement(parsedQ).get_type()

		l = sqlparse.sql.IdentifierList(parsedQ).get_identifiers()				

		for i in l:
			idList.append(str(i))
			
		readMetaData()	

		if(queryType == 'SELECT'):				# if given query is SELECT query
			select_query(q)	
			
		else :
			print "ERROR! please provide Select query."		

	except:
		pass	

def aggregate_column(agg_function):				# find column from metadata on which agg. function is to be applied
	
	if not os.path.exists('metadata.txt'):
		print "File doesn't exist"
		sys.exit()
	file = open('metadata.txt')
	flag=0
	attribute_num=0
	for line in file:	
		if flag==0 and line.strip() in idList[3]:
			flag=1

		elif flag==1 and line.strip() != agg_function:
			attribute_num+=1
		
		elif flag==1 and line.strip() == agg_function:	
			 break
		
		elif flag==1 and line.strip() == "<end_table>":
			 break 	 

	file.close()
	return attribute_num			  

def agg_max(agg_function , table):				# aggregate function max
	
	if(len(idList[1].split(",")) != 1):
		print "kindly provide only one attribute when using aggregate function"
		sys.exit()

	attribute_num = aggregate_column(agg_function)	
	data=[]

	for i in table:
		data.append(int(i[attribute_num]))	
	
	print idList[1].upper()
	print max(data)		

def agg_min(agg_function , table):				# aggregate function min
	
	if(len(idList[1].split(",")) != 1):				# there are attributes with one aggregate function 
		print "kindly provide only one attribute when using aggregate function"
		sys.exit()

	attribute_num = aggregate_column(agg_function)	
	data=[]

	for i in table:
		data.append(int(i[attribute_num]))	

	print idList[1].upper()
	print min(data)

def agg_avg(agg_function , table):				# aggregate function avg
	
	if(len(idList[1].split(",")) != 1):				# there are attributes with one aggregate function 
		print "kindly provide only one attribute when using aggregate function"
		sys.exit()
		
	attribute_num = aggregate_column(agg_function)	
	sum = 0
	count = 0
	for i in table:
		sum+=(int(i[attribute_num]))	
		count+=1
	
	print idList[1].upper()
	print("{0:.2f}".format(round(sum/count,2)))	

def agg_sum(agg_function , table):				# aggregate function sum
	
	if(len(idList[1].split(",")) != 1):				# there are attributes with one aggregate function 
		print "kindly provide only one attribute when using aggregate function"
		sys.exit()

	attribute_num = aggregate_column(agg_function)	
	sum = 0

	for i in table:
		sum+=(int(i[attribute_num]))	

	print idList[1].upper()
	print sum

def agg_count(agg_function , table):			# aggregate function count
	
	if(len(idList[1].split(",")) != 1):				# there are attributes with one aggregate function 
		print "kindly provide only one attribute when using aggregate function"
		sys.exit()

	attribute_num = aggregate_column(agg_function)	

	print idList[1].upper()
	print len(table)

def agg_distinct(agg_function , table):			# aggregate function distinct
	
	if(len(idList[1].split(",")) != 1):				# there are attributes with one aggregate function 
		print "kindly provide only one attribute when using aggregate function"
		sys.exit()

	attribute_num = aggregate_column(agg_function)	
	data=[]

	for i in table:
		data.append(int(i[attribute_num]))	

	print idList[1]
	distinct_data = list(set(data))					

	for i in distinct_data:
		print i	


def select_query(q):							# prcessing SELECT query
	if len(idList) < 4:					# invalid query	
		print "ERROR! kindly enter valid query"
		sys.exit()

	else:	
		idList[1] = idList[1].replace(" ","")	
		agg_function = re.sub(ur"[\(\)]",' ', idList[1]).split()

		#---------------------------------------------------------------
		#Reading tables
		idList[3] = idList[3].replace(" ","")	
		tables = idList[3].split(",")	
		if(len(tables) == 0):								# no tables provided		
			print "ERROR! kindly provide Table names."
			sys.exit()

		elif(len(tables) == 1):								# only one table
			table = readTable(idList[3])

		
		else:												# multiple tables
			table = readTables()
		
		if table == -1:
			print "ERROR! Table doesn't exist"
			sys.exit()	


		#-----------------------------------------------------
		#WHERE clause processing

		if(len(idList) > 4 and "where" in idList[4].lower()):
			where_condition=idList[4][6:]
			
			if len(where_condition) < 3:
				print "ERROR! Enter valid where clause"
				sys.exit()	

			where_condition =  where_condition.replace("  "," ")
			
			table = processWhere(table,where_condition)
		
		#-----------------------------------------------------
		# Projection strategies

		if len(table) == 0:
			print "Empty Table"
			sys.exit()

		if table != -1:
			if(agg_function[0] == '*'):  							#select * from tab
				
				valid = check_columns()								# check validity of attributes
				if valid == "false":
					print "ERROR! kindly enter valid attributes"
					sys.exit()

				attributes = [] 	
				tables = idList[3].split(",")
				for k in tables:
					tempStr = ""
					for i in mdDictionary[k]:
						tempStr += (k + "." + i + ",")
					
					tempStr = tempStr.rstrip(',')	
					attributes.append(tempStr)	
				
				printTable(attributes,table)					# print the result
				


			elif(agg_function[0].lower() == 'max'):				# select max(A) from tab
				agg_max(agg_function[1] , table)
			

			elif(agg_function[0].lower() == 'min'):				# select min(A) from tab
				agg_min(agg_function[1] , table)		
	


			elif(agg_function[0].lower() == 'sum'):				# select sum(A) from tab		
				agg_sum(agg_function[1] , table)



			elif(agg_function[0].lower() == 'avg'):				# select avg(A) from tab
				agg_avg(agg_function[1] , table)

		
			elif(agg_function[0].lower() == 'distinct'):		# select distinct(A) from tab
				agg_distinct(agg_function[1] , table)
				

			elif(agg_function[0].lower() == 'count'):				# select count(A) from tab
				agg_count(agg_function[1] , table)
								
					


			elif len(agg_function)==0:							# no attributes provided
				print "ERROR! kindly enter valid sql query"
				sys.exit()

			else:												# select A from table
				valid = check_columns()							# check validity of attributes
				if valid == "false":
					print "ERROR! kindly enter valid attributes"
					sys.exit()

				attributes = idList[1].split(",")
				
				tables = re.sub(ur"[\,]",' ',idList[3]).split()					
				
				flag = 0
				flag1= 0
				
				attribute_num=0
				col = []
				attributes = processAttributes(attributes)
				tablename = ""

				if not os.path.exists('metadata.txt'):
					print "File doesn't exist"
					sys.exit()

				file = open('metadata.txt')
				for line in file:
					line = line.strip()


					if line == "<begin_table>":
						flag = 1

					elif flag==1 and line in idList[3]:
						tablename = line
						flag = 0
						flag1 = 1

					elif flag1==1:
						line1 = tablename + "." + line
						if line == "<end_table>":
							flag1=0
							tablename = ""

						elif line1 in attributes:
							col.append(attribute_num)
							attribute_num+=1

						elif line1 not in attributes:		
							attribute_num+=1

						else:
							pass

					else:
						pass

				file.close()
				
				table1 = []
				temp = 0
				for i in table:
					temp = 0
					tempList = []
					for j in i:
						if temp in col:
							tempList.append(j)
						temp+=1	
					table1.append(tempList)	 

				#print attributes	
				printTable(attributes,table1)	
		
		else:
			print "Table doesn't exist"
			sys.exit()		
					
	

#----------------------------------------- MAIN FUNCTION

CL_input = sys.argv[1]							# taking input fro command line			
q = CL_input.split(';')
query =' '.join(q)
idList = []
processQuery(query) 
		 
