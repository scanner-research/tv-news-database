import glob
import re

def snake_to_camel(word):
    return ''.join(x.capitalize() or '_' for x in word.split('_'))

regex = r'^\d{4}-\d{1,2}-\d{1,2}'
match_date = re.compile(regex).match
def validate_date(str_val):
     try:            
         if match_date( str_val ) is not None:
             return True
     except:
         pass
     return False

def is_float(str_val):
	try:
		float(str_val)
		return True 
	except: 
		return False

def is_int(str_val):
	try:
		int(str_val)
		return True 
	except: 
		return False

def get_type(header, data):
	if header == 'id':
		return "Integer, primary_key=True"
	
	if header[-3:] == '_id':
		return "Integer, ForeignKey('{}.id')".format(header[:-3])

	if is_int(data):
		return "Integer"

	if is_float(data):
		return "Float"

	if data in ['t', 'f']:
		return "Boolean"

	if validate_date(data):
		return "DateTime"

	if header == 'name':
		return "String"

	if header == 'blurriness':
		return "Float"

	return "String"

tables = []
for path in glob.glob("/newdisk/pg/query_*.csv"):
	table_name = path[18:-4]
	tables.append(table_name)
	with open(path) as f:
		headers = f.readline()[:-1].split(",")
		data = f.readline()[:-1].split(",")
		print("class {}(Base):".format(snake_to_camel(table_name)))
		print("\t__tablename__ = '{}'".format(table_name))
		for h, d in zip(headers, data):
			print("\t{} = Column({})".format(h, get_type(h, d)))
	print()
print()
print("tables = [{}]".format(tables.join(", ")))
print()
