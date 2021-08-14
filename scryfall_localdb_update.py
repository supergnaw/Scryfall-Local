# IMPORTS
import os
import time
import requests
import json
import tqdm # for progress bar
import pymysql

# GLOBALS
# Time in seconds for how long until a JSON file is refreshed
# (good if you run this automagically every so often)
json_stale_age = 86400

# Database connection settings
DB_USER = "root"
DB_PASS = ""
DB_HOST = "localhost"
DB_NAME = "scryfall"

# FUNCTIONS
# --- GENERIC FUNCTIONS --- #
def clear():
	if os.name == 'nt':
		_ = os.system( "cls" )
	else:
		_ = os.system( "clear" )

def kill_err( e, more = None ):
	# literally used to kill the script with the error and
	# remove all the superflouous print statements that
	# occur without error handling
	print( e )
	if None != more: print( more )
	exit()

# --- WHERE THE MAGIC HAPPENS --- #
def fetch_json( uri ):
	time.sleep( 0.1 )
	uri.replace( "\u0026", "&" )
	try:
		r = requests.get( url = uri )
		return r.json()
	except requests.exceptions.InvalidSchema as e:
		kill_err( e )

def save_json( data, outfile ):
	with open( outfile, 'w', encoding = 'utf-8' ) as f:
		json.dump( data, f, ensure_ascii = False, indent = 4 )

def load_json( filename ):
	if( False == os.path.exists( filename )):
		kill_err( f"Missing json file: {filename}" )
	else:
		fp = open( filename, 'r', encoding='utf8' )
		data = json.load( fp )
		fp.close()
		return data

def fetch_json_paged( uri ):
	uri = uri.replace( "\'", "" )
	cards = []
	more = True
	try:
		while True == more:
			page = fetch_json( uri )
			if( "error" == page["object"] ):
				return page["details"]
			cards = cards + page["data"]
			if True == page["has_more"]:
				uri = page["next_page"]
			else:
				more = False
	except KeyError as e:
		kill_err( e )
	return cards


# --- WHERE THE GATHERING HAPPENS --- #
def db_connect():
	# https://dev.mysql.com/doc/connector-python/en/connector-python-example-connecting.html
	try:
		cnx = pymysql.connect(
			host = DB_HOST,
			user = DB_USER,
			password = DB_PASS,
			database = DB_NAME,
			charset='utf8',
			cursorclass=pymysql.cursors.DictCursor
		)
		cnx.autocommit( True )
	except pymysql.err.OperationalError as e:
		kill_err( e )
	else:
		return cnx

def db_execute( cnx, query, params = None ):
	try:
		cursor = cnx.cursor()
		if None != params:
			rowsAffected = cursor.execute( query, params )
		else:
			rowsAffected = cursor.execute( query )
	except pymysql.err.OperationalError as e:
		kill_err( e, query )
	except pymysql.err.ProgrammingError as e:
		kill_err( e, query )
	except pymysql.err.IntegrityError as e:
		kill_err( e, query )
	except ValueError as e:
		kill_err( e, query )

	# return
	res = cursor.fetchall()
	if None != res:
		return res
	return rowsAffected

def db_insert_card( card, cnx ):
	# do some normalizing
	for field in card:
		if not isinstance( card[field], ( str, bool, int, float )):
			if isinstance( card[field], list ):
				card[field] = "['" + str( "','".join( [str( item ) for item in card[field]] )) + "']"
			else:
				card[field] = str( card[field] )

	cols = []
	vals = []
	params = []
	for field in card:
		cols.append( field )
		vals.append( "%s" )
		params.append( card[field] )
	cols = "`, `".join( cols )
	vals = ", ".join( vals )
	sql = f"INSERT INTO cards ( `{cols}` ) VALUES ( {vals} )"

	# execution
	return db_execute( cnx, sql, params )

# --- WHERE THE TRADING HAPPENS --- #
clear()
# Fetch most recent data
print( "=== FETCHING SETS BULK JSON DUMP ===\n --Source URI: https://api.scryfall.com/sets\n" )
setsJson = fetch_json_paged( "https://api.scryfall.com/sets" )
filename = os.path.join( "json", "all_sets.json" )
save_json( setsJson, filename )

print( "=== UPDATING SETS ===\n --Looping through sets; this could take a few...minutes.\n" )
failed_sets = []
for set in tqdm.tqdm( setsJson ):
	# prepare the filename
	filename = os.path.join( "json", "sets", f"{set['code']}_cards.json" )

	# check if file already exists and remove it if it's stale
	if True == os.path.exists( filename ):
		modified = os.path.getmtime( filename )
		rightNow = time.time()
		if json_stale_age < rightNow - modified:
			os.remove( filename )

	# download and save new file
	if False == os.path.exists( filename ):
		cardsJson = fetch_json_paged( set['search_uri'] )
		if isinstance( cardsJson, dict ):
			save_json( cardsJson, filename )
		else:
			# print( set['code'] )
			failed_sets.append( set['code'].upper() + ": " + cardsJson )

# Close out with the stats
failed_count = len( failed_sets )
print( f"\n --Update complete with the following {failed_count} failures:" )
if( 0 < failed_count ):
	for failure in failed_sets:
		words = failure.split( " " )
		lines = []
		line_length = 70
		while 0 < len( failure ):
			if line_length < len( failure ):
				x = failure[0:line_length].rfind( " " )
				if 0 == x:
					lines.append( failure[0:line_length] + "-" )
					failure = failure[line_length::]
				else:
					lines.append( failure[0:x] )
					failure = failure[x+1::]
			else:
				lines.append( failure )
				failure = ""
		lines = "\n    ".join( lines )
		print( f"\n  * {lines}" )

print( "\n=== UPDATING LOCAL SCRYFALL DATABASE ===\n" )
print( " --Searching for local json dump files..." )
jsonFiles = []
directory = os.path.join( "json", "sets" )
for filename in os.listdir( directory ):
	if filename.endswith( "_cards.json" ):
		jsonFiles.append( filename )
jsonFileCount = len( jsonFiles )
print( f" --Found {jsonFileCount} files." )

print( " --Establishing database connection..." )
cnx = db_connect()

print( " --Clearing cards in database..." )
db_execute( cnx, "TRUNCATE cards" )

print( " --Looping through json files...\n" )
total_cards = 0
for num, filename in enumerate( jsonFiles ):
	if filename.endswith( "_cards.json" ):
		cards = load_json( os.path.join(directory, filename ))
		print( f"\n{num:>3}/{jsonFileCount} - {cards[0]['set_name']}" )
		for card in tqdm.tqdm( cards ):
			db_insert_card( card, cnx )
		card_count = len( cards )
		total_cards = card_count + total_cards
	else:
		continue
print( f"\n  * Total cards: {total_cards}" )
