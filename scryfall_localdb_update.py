import os
import time
import requests
import json
import tqdm # for progress bar
import pymysql

# GLOBALS
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
				return None
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
	except pymysql.err.OperationalError as e:
		kill_err( e )
	else:
		return cnx

def db_execute( query, params = None, cnx = None ):
	if None == cnx:
		cnx = db_connect()
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

# --- WHERE THE TRADING HAPPENS --- #
def db_insert_card( card, cnx ):
    whitelist = (
        "border_color", "card_back_id", "cmc", "collector_number",
        "color_identity", "color_indicator", "colors", "frame",
        "full_art", "highres_image", "id", "image_status",
        "json_object", "lang", "layout", "loyalty",
        "mana_cost", "name", "oracle_id", "oracle_text",
        "power", "price_usd", "prices", "produced_mana",
        "rarity", "released_at", "set", "set_id",
        "set_name", "textless", "toughness", "type_line",
        "w", "u", "b", "r", "g"
    )

    # minor normalizing
    card["json_object"] = str( card )

    card["set_code"] = card["set"]
    del card["set"]

    if "usd" in card["prices"]:
        card["price_usd"] = card["prices"]["usd"]

    if "colors" not in card: card["colors"] = {}
    card["w"] = 1 if "W" in card["colors"] else 0
    card["u"] = 1 if "U" in card["colors"] else 0
    card["b"] = 1 if "B" in card["colors"] else 0
    card["r"] = 1 if "R" in card["colors"] else 0
    card["g"] = 1 if "G" in card["colors"] else 0

    for field in card:
        if not isinstance( card[field], ( str, bool, int, float )):
            card[field] = str( card[field] )
            # print( field + " converted to string" )

    # prepare statement preparing
    cols = []
    vals = []
    params = []
    for field in card:
        if field in whitelist:
            cols.append( field )
            vals.append( "%s" )
            params.append( card[field] )
    cols = ", ".join( cols )
    vals = ", ".join( vals )
    sql = f"INSERT INTO cards ( {cols} ) VALUES ( {vals} )"
    # print( sql )
    # print( params )

    db_execute( sql, params, cnx )

total_cards = 0
cnx = db_connect()
db_execute( "TRUNCATE cards", None, cnx )
directory = os.path.join( "json", "sets" )
for filename in tqdm.tqdm( os.listdir( directory )):
    if filename.endswith( "_cards.json" ):
        cards = load_json( os.path.join(directory, filename ))
        for card in cards:
            db_insert_card( card, cnx )
        card_count = len( cards )
        total_cards = card_count + total_cards
    else:
        continue
print( f"Total cards: {total_cards}" )
