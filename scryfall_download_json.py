# IMPORTS
import os
import time
import requests
import json
import tqdm # for progress bar

# Time in seconds for how long until a JSON file is refreshed
# (good if you run this automagically every so often)
json_stale_age = 86400

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
	if( False == pathlib.Path( filename ).exists()):
		kill_err( f"Missing json file: {filename}" )
	else:
		fp = open( filename, 'r' )
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
# Fetch most recent data
clear()
print( "=== FETCHING SETS BULK JSON DUMP ===\n --Source URI: https://api.scryfall.com/sets\n" )
setsJson = fetch_json_paged( "https://api.scryfall.com/sets" )
filename = os.path.join( "json", "all_sets.json" )
save_json( setsJson, filename )

print( "=== UPDATING SETS ===\n --Looping through sets; this could take a few...minutes.\n" )
# for set in tqdm.tqdm( setsJson["data"] ):
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
			failed_sets.append( "   " + set['code'].upper() + ": " + cardsJson )

# Close out with the stats
failed_count = len( failed_sets )
print( f"--Update complete with the following {failed_count} failures:" )
if( 0 < failed_count ):
	for failure in failed_sets:
		print( failure )
