import itertools
import re
import sys
from statistics import mean 
import copy
import csv

class database:

	def __init__(self):

		self.tables = {}

	def load_table(self,table_name, table_headers):

		file_name = table_name + ".csv"
		
		try:
	
			table_obj = table()
			table_obj.column_headers = table_headers
			table_obj.column_headers = [col_name.lower() for col_name in table_obj.column_headers]
			table_obj.table_name = table_name

			table_rows_list = []

			with open(file_name, newline='') as csvfile:
				filereader = csv.reader(csvfile, delimiter=' ', quotechar='|')
				for row in filereader:
					row_list = []
					temp_list = []
					temp_list = re.split(' |,',row[0])
					
					for item in temp_list:
						item = item.replace('"', '')
						item = item.replace('"', '')
						
						try:
							row_list.append(int(item))
						except ValueError:
							print("Non integer data detected in dataset")
							sys.exit("Aborting process")

					table_rows_list.append(row_list)					

		
			table_obj.data = table_rows_list

			return table_obj	
		
		except FileNotFoundError:
		
			print("File corresponding to table ",table_name," is missing")

			sys.exit("Aborting Process")

	def load_database(self):

		table_headers = []
		table_name = ""

		metadata_file_path = "metadata.txt" 
		
		marker = 0

		for line in open(metadata_file_path):
			line = line.rstrip("\n")

			if line == "<begin_table>" and marker == 0:

				table_obj = table()
				marker = 1

			elif marker == 1:

				table_name = line
				marker = 2

			elif line == "<end_table>":

				table_obj = self.load_table(table_name,table_headers)

				self.tables[table_obj.table_name] = table_obj
				
				del table_obj
				table_headers = []
				marker = 0

			elif marker == 2:

				table_headers.append(table_name+"."+line)

	def print_database_state(self):
			
		for table_name, table_obj in self.tables.items():

			print("table name: ",table_name)

			for row in table_obj.data:

				print(row)

	def validate_sql_statement(self,query,query_tokens):

		if query.count("*") > 1:
			return False

		if query.lower().count("from") > 1 or query.lower().count("where") > 1:
			return False

		if query.lower().count("and") > 1 or query.lower().count("or") > 1:
			return False

		if query.lower().count("distinct") > 1:
			return False

		stmt_tokens = re.split(' ',query)
		query_tokens = list(filter(None, query_tokens))

		for index in range(len(stmt_tokens)):

			if stmt_tokens[index].count(','):
				temp_tokens = re.split(',',stmt_tokens[index])
				if '' in temp_tokens:
					return False;

		if query.count(",,") > 0:
			return False 

		if "select" not in query_tokens or "from" not in query_tokens or query_tokens[1] == "from":
			return False

		from_index = query_tokens.index("from")
		
		if "where" in query_tokens:
			
			where_index = query_tokens.index("where")

			if from_index+1 == where_index:
				return False
		else:
			
			if from_index+1 == len(query_tokens):
				return False		

		if "and" in query and "and" not in query_tokens:
			return False

		if "or" in query and "or" not in query_tokens:
			return False 

		if "and" not in query_tokens and "or" not in query_tokens and "where" in query_tokens:
			if len(query_tokens[query_tokens.index("where")+1:]) > 3:
				return False

		return True

	def parse_sql_query(self, query):
	
		if query.split()[-1] != ";" and query[-1] != ";":
			
			print("Missing semicolon at the end of query")
			sys.exit("Aborting process...")

		if query.count(";") != 1:
			print("Multiple Semicolon detected")
			sys.exit("Aborting process...")

		query = re.sub('[;]', '', query)

		aggregate_funtions = ["sum","average","max","min"]
		operator_list = ["=",">","<","<=",">="]

		parsed_query = {}

		index = -1
		skip_next = False

		for character in query:
			# print(query)
			index += 1
			
			if skip_next == True:
				skip_next = False
				pass

			elif character == '<' and query[index+1] == '=':
				# print("a: ",character)
				query = query[:index]+" "+query[index:]
				query = query[:index+3]+" "+query[index+3:]
				index += 2
				skip_next = True

			elif character == '>' and query[index+1] == '=':
				# print("b: ",character)
				query = query[:index]+" "+query[index:]
				query = query[:index+3]+" "+query[index+3:]
				index += 2
				skip_next = True

			elif character == '=' or character == '>' or character == '<':
				# print("c: ",character,query[index+1])
				query = query[:index]+" "+query[index:]
				query = query[:index+2]+" "+query[index+2:]
				index += 2
				skip_next = False
			

		# print(query)
		query_tokens = re.split(' |,',query)

		# print(query_tokens)

		query_tokens = list(filter(None, query_tokens))
		query_tokens = [token.lower() for token in query_tokens]

		# print(query_tokens)

		if self.validate_sql_statement(query,query_tokens):

			try:

				if "where" in query_tokens:
					
					from_index = 0

					if "from" in query_tokens:
						from_index = query_tokens.index("from")
					else:
						print("Invalid SQL statement")
						sys.exit("Aborting process")

					where_index = 0

					if "where" in query_tokens:
						where_index = query_tokens.index("where")


					if "and" in query_tokens or "or" in query_tokens: 
						
						condition_index = 0

						if "and" in query_tokens:
							parsed_query["boolean_operation"] = "AND"
							condition_index = query_tokens.index("and")

						else:
							parsed_query["boolean_operation"] = "OR"
							condition_index = query_tokens.index("or")


						if query_tokens[1] == "*":
							parsed_query["query_type"] = 6
							parsed_query["columns"] = "*"
							parsed_query["table"] = query_tokens[from_index+1:where_index]

						elif "distinct" == query_tokens[1]:
							distinct_index = query_tokens.index("distinct")

							if query_tokens[distinct_index+1] == "*":
								parsed_query["query_type"] = 12
								parsed_query["columns"] = "*"
							else:	
								parsed_query["query_type"] = 7  
								parsed_query["columns"] = query_tokens[distinct_index+1:from_index]
							
							parsed_query["table"] = query_tokens[from_index+1:where_index]

						else:
							parsed_query["query_type"] = 5
							parsed_query["columns"] = query_tokens[1:from_index]
							parsed_query["table"] = query_tokens[from_index+1:where_index]
						

						#parsed_query["condition_X"]  = [column_name,operator,column value]

		
						parsed_query["condition_one"] = [query_tokens[where_index+1],query_tokens[where_index+2],query_tokens[where_index+3]]
						parsed_query["condition_two"] = [query_tokens[condition_index+1],query_tokens[condition_index+2],query_tokens[condition_index+3]] 
					
					else:

						if query_tokens[1] == "*":
								
							try:

								temp = int(query_tokens[where_index+3])
								parsed_query["query_type"] = 14
								
							except ValueError:
								parsed_query["query_type"] = 9
							
							parsed_query["columns"] = "*"
							parsed_query["table"] = query_tokens[from_index+1:where_index]

						elif "distinct" == query_tokens[1]:
							distinct_index = query_tokens.index("distinct")

							if query_tokens[distinct_index+1] == "*":
								
								try:
									temp = int(query_tokens[where_index+3])
									parsed_query["query_type"] = 16
								
								except ValueError:
									parsed_query["query_type"] = 13
							
								parsed_query["columns"] = "*"
							else:

								try:
									temp = int(query_tokens[where_index+3])
									parsed_query["query_type"] = 17
								
								except ValueError:
									parsed_query["query_type"] = 10

								parsed_query["columns"] = query_tokens[distinct_index+1:from_index]
							
							parsed_query["table"] = query_tokens[from_index+1:where_index]

						else:

							try:
								temp = int(query_tokens[where_index+3])
								parsed_query["query_type"] = 15
							
							except ValueError:
								parsed_query["query_type"] = 8

							parsed_query["columns"] = query_tokens[1:from_index]
							parsed_query["table"] = query_tokens[from_index+1:where_index]
						
						

						parsed_query["condition"] = [query_tokens[where_index+1],query_tokens[where_index+2],query_tokens[where_index+3]]

				else:	

					if query_tokens[1] == '*':

						table_list = query_tokens[3:]

						parsed_query["query_type"] = 1

						parsed_query["columns"] = "*"

						parsed_query["table"] = table_list
						return parsed_query

					elif '(' in query_tokens[1] and ')' in query_tokens[1]:

						parsed_query["query_type"] = 2
						tokens_list = query_tokens[1].split("(")
						
						parsed_query["operation"] = tokens_list[0]
						tokens_list = tokens_list[1].split(")")
						
						column_name = tokens_list[0]
						parsed_query["columns"] = [column_name]
						
						table_list = query_tokens[3:]
						parsed_query["table"] = table_list
					
					else: 
						
						from_index = 0

						if "from" in query_tokens:
							from_index = query_tokens.index("from")
						else:
							print("Invalid SQL statement")
							sys.exit("Aborting process")
						
						if "distinct" in query_tokens:
							
							distinct_index = query_tokens.index("distinct")

							if query_tokens[distinct_index+1] == "*":
								parsed_query["query_type"] = 11
								parsed_query["columns"] = "*"
							else:
								parsed_query["query_type"] = 4
								parsed_query["columns"] = query_tokens[2:from_index]

						else:
							parsed_query["query_type"] = 3
							parsed_query["columns"] = query_tokens[1:from_index]


						parsed_query["table"] = query_tokens[from_index+1:]

			except IndexError:
				print("Invalid SQL statement")
				sys.exit("Aborting process...")
		else:
			print("Invalid SQL statement")
			sys.exit("Aborting process...")
		
		return parsed_query

	def get_column_index(self,column_name,table_obj):

		column_index = 0
		flag = False

		# print("column_name",column_name)

		if "." in column_name:
			try:
				column_index = table_obj.column_headers.index(column_name)
				flag = True
			except ValueError:
				print("Invalid column name" ,column_name)
				sys.exit("Aborting process...")
		else:

			counter = 0
			index = 0
			for columns in table_obj.column_headers:
				
				items = columns.split(".") 
				if column_name == items[1]:
					
					if counter == 0:
						index = table_obj.column_headers.index(columns)
						flag = True
						counter += 1
					elif counter == 1:
						print("Ambiguous column name ",column_name)
						sys.exit("Aborting process... ")

			column_index = index

		if flag == False:
			print("Invalid column name ",column_name)
			sys.exit("Aborting process... ")

		return column_index 

	def table_join(self,parsed_query):

		output_table_obj = table()
		output_table_obj.name = "output"

		column_headers = []

		for table_name in parsed_query["table"]:

				if table_name in self.tables:
					
					input_table_obj = self.tables[table_name]
				
					if output_table_obj.data:

						update_output_data = []

						for output_row in output_table_obj.data:

							temp = []

							for input_row in input_table_obj.data:

								temp = output_row + input_row
								update_output_data.append(temp)

						output_table_obj.data = update_output_data

						output_table_obj.column_headers = output_table_obj.column_headers + input_table_obj.column_headers
					
					else:
						output_table_obj.data = input_table_obj.data
						output_table_obj.column_headers = input_table_obj.column_headers
						
				else:
					print("Table ",table_name," not found in database")		
					sys.exit("Aborting process...")

		return output_table_obj

	def project_columns(self,columns_list,table_obj):

		data = []
		
		new_column_header = []

		col_index_list = []

		columns_list = [token.lower() for token in columns_list]
		
		

		for col in columns_list:
			
			if "." in col:
				try:
					new_column_header.append(col)
					
					col_index_list.append(table_obj.column_headers.index(col))
				except ValueError:
					sys.exit("Invalid column name")
			else:
				counter = 0
				index = -1
				for column_name in table_obj.column_headers:
					
					items = column_name.split(".") 
					if col == items[1]:
						
						new_column_header.append(column_name)
						if counter == 0:
							index = table_obj.column_headers.index(column_name)
							counter += 1
						elif counter == 1:
							print("Ambiguous column name ",col)
							sys.exit("Aborting process... ")
				if index == -1:
					print("Column not found ")
					sys.exit("Aborting process... ")

				col_index_list.append(index)

		
		if len(col_index_list) == 0:
			print("Invalid column name ")
			sys.exit("Aborting process... ")


		for row in table_obj.data:
			temp = []
			for index in col_index_list:	
				# print(row)
				temp.append(row[index])

			data.append(temp)

		return data, new_column_header

	def distinct_rows(self,table_obj):

		data = []

		for row in table_obj.data:
			if row not in data:
				data.append(row)

		return data		

	def apply_where_condition_boolean(self,parsed_query,table_obj):

		data = []
		condition_one_data = []
		condition_two_data = []

		condition_one_column_index = 0
		condition_two_column_index = 0

		condition_one = parsed_query["condition_one"]
		condition_two = parsed_query["condition_two"]

		

		condition_one_column_index = self.get_column_index(condition_one[0],table_obj)
		condition_two_column_index = self.get_column_index(condition_two[0],table_obj)

		

		operator_list = [">","<",">=","<=","="]

		if condition_one[1] not in operator_list or condition_two[1] not in operator_list:
			print("Invalid opeartor ",condition[0])
			sys.exit("Aborting process")

		for row in table_obj.data:

			try:
				if condition_one[1] == "=":

					if row[condition_one_column_index] == int(condition_one[2]):
						condition_one_data.append(row)

				elif condition_one[1] == ">=":

					if row[condition_one_column_index] >= int(condition_one[2]):
						condition_one_data.append(row)

				elif condition_one[1] == "<=":

					if row[condition_one_column_index] <= int(condition_one[2]):
						condition_one_data.append(row)

				elif condition_one[1] == ">":

					if row[condition_one_column_index] > int(condition_one[2]):
						condition_one_data.append(row)

				elif condition_one[1] == "<":
					
					if row[condition_one_column_index] < int(condition_one[2]):
						condition_one_data.append(row)
			except ValueError:
				print("Invalid SQL query")
				sys.exit("Aborting process")

		for row in table_obj.data:

			try:
				if condition_two[1] == "=":

					if row[condition_two_column_index] == int(condition_two[2]):
						condition_two_data.append(row)

				elif condition_two[1] == ">=":

					if row[condition_two_column_index] >= int(condition_two[2]):
						condition_two_data.append(row)

				elif condition_two[1] == "<=":

					if row[condition_two_column_index] <= int(condition_two[2]):
						condition_two_data.append(row)

				elif condition_two[1] == ">":

					if row[condition_two_column_index] > int(condition_two[2]):
						condition_two_data.append(row)

				elif condition_two[1] == "<":
					
					if row[condition_two_column_index] < int(condition_two[2]):
						condition_two_data.append(row)			
			except ValueError:
				print("Invalid SQL query")
				sys.exit("Aborting process")

		if parsed_query["boolean_operation"] == "AND":

			for row_one in condition_one_data:
				if row_one in condition_two_data and row_one not in data:
					data.append(row_one)

		elif parsed_query["boolean_operation"] == "OR":

			for row_one in condition_one_data:
				if row_one not in data:
					data.append(row_one)

			for row_two in condition_two_data:
				if row_two not in data:
					data.append(row_two)
		else:
			print("Invalid boolean operator")
			sys.exit("Aborting process")

		return data

	def apply_where_condition_join(self,parsed_query,table_obj):

		data = []

		condition = parsed_query["condition"]

		column_one_name = condition[0]
		column_two_name = condition[2]

		operator = condition[1]

		column_one_index = self.get_column_index(column_one_name,table_obj)
		column_two_index = self.get_column_index(column_two_name,table_obj)

		operator_list = [">","<",">=","<=","="]

		if operator not in operator_list:
			print("Invalid opeartor ",operator)
			sys.exit("Aborting process")

		try:
			if operator == "=":

				for row in table_obj.data:

					if int(row[column_one_index]) == int(row[column_two_index]):
						data.append(row)
			
			elif operator == "<":

				for row in table_obj.data:

					if int(row[column_one_index]) < int(row[column_two_index]):
						data.append(row)

			elif operator == "<=":

				for row in table_obj.data:

					if int(row[column_one_index]) <= int(row[column_two_index]):
						data.append(row)

			elif operator == ">":

				for row in table_obj.data:

					if int(row[column_one_index]) > int(row[column_two_index]):
						data.append(row)

			elif operator == ">=":

				for row in table_obj.data:

					if int(row[column_one_index]) >= int(row[column_two_index]):
						data.append(row)

		except ValueError:
			print("Invalid SQL query")
			sys.exit("Aborting process")

		return data

	def apply_where_condition(self,parsed_query,table_obj):


		data = []
		
		condition_column_index = 0
		
		condition = parsed_query["condition"]
		
		condition_column_index = self.get_column_index(condition[0],table_obj)
		
		operator_list = [">","<",">=","<=","="]

		if condition[1] not in operator_list:
			print("Invalid opeartor ",condition[0])
			sys.exit("Aborting process")

		for row in table_obj.data:
			try:
				if condition[1] == "=":

					if row[condition_column_index] == int(condition[2]):
						data.append(row)

				elif condition[1] == ">=":

					if row[condition_column_index] >= int(condition[2]):
						data.append(row)

				elif condition[1] == "<=":

					if row[condition_column_index] <= int(condition[2]):
						data.append(row)

				elif condition[1] == ">":

					if row[condition_column_index] > int(condition[2]):
						data.append(row)

				elif condition[1] == "<":
					
					if row[condition_column_index] < int(condition[2]):
						data.append(row)

			except ValueError:
				print("Invalid SQL query")
				sys.exit("Aborting process")

		return data

	def write_table_obj_file(self,parsed_query,output_table_obj):
		
		skip_index = -1

		if "*" in parsed_query["columns"]: 
			if "conditions" in parsed_query:
				if parsed_query["conditions"][1]=="=":
					if parsed_query["query_type"] == 9 or parsed_query["query_type"] == 13:
						condition = parsed_query["condition"]
						column_two_name = condition[2]
						skip_index = output_table_obj.column_headers.index(column_two_name)
				


		for index in range(len(output_table_obj.column_headers)-1):
			if skip_index != -1 and skip_index == index:
				pass
			else:	
				if skip_index != len(output_table_obj.column_headers)-1:
					print(output_table_obj.column_headers[index],",", end =" ")
				else:
					print(output_table_obj.column_headers[index], end =" ")

		if skip_index != len(output_table_obj.column_headers)-1:
			print(output_table_obj.column_headers[len(output_table_obj.column_headers)-1], end =" ")
		
		print()

		if len(output_table_obj.data) == 0:
			print("No rows Found")

		for rows in output_table_obj.data:
			for index in range(len(rows)-1):
				if skip_index != -1 and skip_index == index:
					pass
				else:
					if skip_index != len(rows)-1:
						print(rows[index],",",end = " ")
					else:
						print(rows[index],end = " ")

			if skip_index != len(rows)-1:
				print(rows[len(rows)-1],end = " ")
			print()


	def execute_sql_query(self,parsed_query):
		
		output_table_obj = table()
		output_table_obj.name = "output"

		if parsed_query["query_type"] == 1:
			
			output_table_obj = self.table_join(parsed_query)

		elif parsed_query["query_type"] == 2:
			
			output_table_obj = self.table_join(parsed_query)

			output_table_obj.data, output_table_obj.column_headers = self.project_columns(parsed_query["columns"],output_table_obj)
					
			result = 0
			index = 0

			if parsed_query["operation"] == "sum":
				
				output = "sum({})".format(parsed_query["columns"][0])

				for row in output_table_obj.data:
					result += row[index]
				

			elif parsed_query["operation"] == "max":

				output = "max({})".format(parsed_query["columns"][0])
				
				result = float('-inf')
				for row in output_table_obj.data:
					if row[index] > result:
						result = row[index]

			elif parsed_query["operation"] == "min":

				output = "min({})".format(parsed_query["columns"][0])

				result = float('inf')
				for row in output_table_obj.data:
					if row[index] < result:
						result = row[index]

			elif parsed_query["operation"] == "avg":						
				
				output = "avg({})".format(parsed_query["columns"][0])

				counter = 0
				for row in output_table_obj.data:
					counter += 1
					result += row[index]

				result = float(result/counter)

			else:

				print("Invalid aggregate function")		
				sys.exit("Aborting process...")

			print(output)
			print(result)
			
			return	

		elif parsed_query["query_type"] == 3:
			
			output_table_obj = self.table_join(parsed_query)			

			output_table_obj.data, output_table_obj.column_headers = self.project_columns(parsed_query["columns"],output_table_obj)			

		elif parsed_query["query_type"] == 4:
			
			output_table_obj = self.table_join(parsed_query)			

			output_table_obj.data, output_table_obj.column_headers = self.project_columns(parsed_query["columns"],output_table_obj)			

			output_table_obj.data = self.distinct_rows(output_table_obj)

		elif parsed_query["query_type"] == 5:
			
			output_table_obj = self.table_join(parsed_query)

			output_table_obj.data = self.apply_where_condition_boolean(parsed_query,output_table_obj)			

			output_table_obj.data, output_table_obj.column_headers = self.project_columns(parsed_query["columns"],output_table_obj)			

		elif parsed_query["query_type"] == 6:
		
			output_table_obj = self.table_join(parsed_query)

			output_table_obj.data = self.apply_where_condition_boolean(parsed_query,output_table_obj)			

		elif parsed_query["query_type"] == 7:
			
			output_table_obj = self.table_join(parsed_query)

			output_table_obj.data = self.apply_where_condition_boolean(parsed_query,output_table_obj)

			output_table_obj.data, output_table_obj.column_headers = self.project_columns(parsed_query["columns"],output_table_obj)			

			output_table_obj.data = self.distinct_rows(output_table_obj)

		elif parsed_query["query_type"] == 8:
			
			output_table_obj = self.table_join(parsed_query)

			output_table_obj.data = self.apply_where_condition_join(parsed_query,output_table_obj)

			output_table_obj.data, output_table_obj.column_headers = self.project_columns(parsed_query["columns"],output_table_obj)

		elif parsed_query["query_type"] == 9:
			
			output_table_obj = self.table_join(parsed_query)

			output_table_obj.data = self.apply_where_condition_join(parsed_query,output_table_obj)

		elif parsed_query["query_type"] == 10:
			
			output_table_obj = self.table_join(parsed_query)

			output_table_obj.data = self.apply_where_condition_join(parsed_query,output_table_obj)

			output_table_obj.data, output_table_obj.column_headers = self.project_columns(parsed_query["columns"],output_table_obj)

			output_table_obj.data = self.distinct_rows(output_table_obj)

		elif parsed_query["query_type"] == 11:

			output_table_obj = self.table_join(parsed_query)			

			output_table_obj.data = self.distinct_rows(output_table_obj)

		elif parsed_query["query_type"] == 12:
			
			output_table_obj = self.table_join(parsed_query)

			output_table_obj.data = self.apply_where_condition_boolean(parsed_query,output_table_obj)

			output_table_obj.data = self.distinct_rows(output_table_obj)

		elif parsed_query["query_type"] == 13:
			
			output_table_obj = self.table_join(parsed_query)

			output_table_obj.data = self.apply_where_condition_join(parsed_query,output_table_obj)

			output_table_obj.data = self.distinct_rows(output_table_obj)

		elif parsed_query["query_type"] == 14:

			output_table_obj = self.table_join(parsed_query)

			output_table_obj.data = self.apply_where_condition(parsed_query,output_table_obj)

		elif parsed_query["query_type"] == 15:
			
			output_table_obj = self.table_join(parsed_query)

			output_table_obj.data = self.apply_where_condition(parsed_query,output_table_obj)

			output_table_obj.data, output_table_obj.column_headers = self.project_columns(parsed_query["columns"],output_table_obj)

		elif parsed_query["query_type"] == 16:
			
			output_table_obj = self.table_join(parsed_query)

			output_table_obj.data = self.apply_where_condition(parsed_query,output_table_obj)

			output_table_obj.data = self.distinct_rows(output_table_obj)

		elif parsed_query["query_type"] == 17:
			
			output_table_obj = self.table_join(parsed_query)

			output_table_obj.data = self.apply_where_condition(parsed_query,output_table_obj)

			output_table_obj.data, output_table_obj.column_headers = self.project_columns(parsed_query["columns"],output_table_obj)

			output_table_obj.data = self.distinct_rows(output_table_obj)

		# print(parsed_query)

		self.write_table_obj_file(parsed_query,output_table_obj)
		

class table:

	def __init__(self):

		self.table_name = ""
		self.column_headers = []
		self.data = []



def main():
	
	database_obj = database()
	database_obj.load_database()

	query = sys.argv[1]

	parsed_query = database_obj.parse_sql_query(query)

	database_obj.execute_sql_query(parsed_query)


if __name__ == '__main__':
    main()


#Type of commands that can be queried

#cmd 1 select * from table1, table2...
#cmd 2 select function(column name) from table
#cmd 3 select col1,col2... from table1,table2...
#cmd 4 select distinct col1,col2... from table1,table2... 

#cmd 5 select col1,col2 from table1,table2 where col1 = 10 AND col2 = 20
#cmd 6 select * from table1,table2 where col1 = 10 AND col2 = 20
#cmd 7 select distinct col1,col2 from table1,table2 where col1 = 10 AND col2 = 20

#cmd 8 select col1, col2 from table1,table2 where table1.col1 = table2.col2
#cmd 9 select * from table1, table2 where table1.col1 = table2.col2
#cmd 10 select distinct col1, col2 from table1,table2 where table1.col1 = table2.col2

#cmd 11 select distinct * from table1,table2...
#cmd 12 select distinct * from table1,table2 where col1 = 10 AND col2 = 20
#cmd 13 select distinct * from table1,table2 where table1.col1 = table2.col2

#cmd 14 select * from table1,table2 where col1 = 10 
#cmd 15 select col1,col2 from table1,table2 where col1 = 10 
#cmd 16 select distinct * from table1,table2 where col1 = 10 
#cmd 17 select distinct col1,col2 from table1,table2 where col1 = 10 