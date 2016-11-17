#! /usr/bin/python
#
#
## The purpose of this script is to take HTML files from The Huddle and
## parse the HTML and then upload to the football database on the server
## to form part of the 'What If' machine. This will start with the 2014
## season, and possibly populate it back a little further.
##

import re
import getopt
import time
import sys
import urllib
import pg

from BeautifulSoup import BeautifulSoup

def main():
	source = str(sys.argv[1])
	year = int(sys.argv[2])

	football(source,year)

def football(source,year):
	''' the foutball routine will take the HTML files and then pass to the various
	functions to perform the parsing before finally uploading the data to the
	PostgreSQL database. '''

	print 'Started parsing ' + str(source) + ' for the year ' + str(year)

	week, position, headings, stats=parse(source,year)

	update_players(stats, position)

	update_stats(stats,position,week,headings,year)

def update_stats(stats,position,week,headings,year):
	''' This function will insert the stats for all players in a given week. The
	foreign key reference in players will need to be looked up to ensure that the
	data in the stats table references the correct player_id. '''

	# Get the database hook.

	con = pg_con()

	# Change the heading columns to match what is in the 'stats' table.
	# The defensive interceptions are classified as 'DINT' instead of 'INT'

	headings[0] = 'name'
	headings[1] = 'team'
	if position=='df':
		for n,i in enumerate(headings):
			if i=='INT':
				headings[n]='DINT'
	elif position=='pk':
		for n,i in enumerate(headings):
			if i=='FG MISS':
				headings[n]='FG_MISS'
			elif i=='XPT MISS':
				headings[n]='XPT_MISS'
	# print headings

	# Generate the query to update the stats for each player for the week.

	column_names = 'player_id,year,week,' + ",".join(headings[2:])
	# print column_names

	for i in range(0,len(stats)):
		if position=='df' and stats[i][0]=='New York':
			if stats[i][1]=='NYG':
				stats[i][0]='New York Giants'
			else:
				stats[i][0]='New York Jets'
		get_player_id_query = 'select player_id from players where name=\'' + stats[i][0] + '\''
		player_id = con.query(get_player_id_query)
		player_id = player_id.getresult()[0][0]

		stats_query = 'insert into stats(' + column_names + ') values (' + str(player_id) + ',' + str(year) + ',' + str(week) + ',' + ",".join(str(x) for x in stats[i][2:]) + ')'
		# print stats_query

		con.query(stats_query)

def update_players(stats, position):
	''' This function will update the player data in the 'players' table in the
	database 'football'. This will give each player a unique player_id that will
	be referenced in the stats table. If the player already exists, no update
	is made. '''

	#for i in range(0,len(stats)):
	#	print str(stats[i][0]) + ' ' + str(stats[i][1]) + ' ' + str(position).upper()

	# Get the connection OID from pg_con

	con = pg_con()

	# Generate the query to insert or update.
	# The query inserts the player names. It makes the attempt, and if the player name
	# already exists, it raises an exception and passes (try/except criteria).

	for i in range(0,len(stats)):
			if position=='df' and stats[i][0]=='New York':
				if stats[i][1]=='NYG':
					stats[i][0]='New York Giants'
				else:
					stats[i][0]='New York Jets'
			insert_query = 'insert into players(name,team,position) values(\'' + stats[i][0] + '\',\'' + stats[i][1] + '\',\'' + str(position).upper() + '\')'
			# print insert_query
			try:
				con.query(insert_query)
			except:
				pass

	# Close the connection to the database.

	con.close()
	

def pg_con():
	''' This is a function that returns the PostgreSQL connection. '''
	con = pg.connect(dbname='football',host='localhost',user='chris')

	return con

def parse(source,year):
	''' This parsing function will take all the data, and return a list of dictionaries.
	The idea is that it will capture all the information (headings) and pair the stats
	with the columns. To make sure that it is modular, it will associate the stats with
	the heading to ensure that separate functions are not required for different positions. '''

	f = open(source, 'r')

	html = f.read()
	f.close()

	soup = BeautifulSoup(html)

	# Find the week. This is enclosed in <small> tags

	week = soup.find('small')
	re_week = re.compile('<small>Week ([0-9]*)</small>',re.I|re.M|re.S) 
	result = re_week.findall(str(week))
	week = result[0]
	print 'Week ' + str(week)

	# Find the position from the dropdown for other weeks of that position.
	# Grab the first link and parse to find the position.
	position = soup.find('ul', {"id": "drop-other-weeks"})
	position = position.find('a')
	re_position = re.compile('.*pos=([a-z]{2}).*',re.I|re.M|re.S)
	result = re_position.findall(str(position))
	position = result[0]
	print 'Position = ' + position

	# Find all the rows with the player data. This will be in the table.

	body = soup.find('table')
	rows = body.findAll('tr')
	#for i in range(len(rows)):
	#	print 'Row #' + str(i) + ':\n' + str(rows[i])

	# Row #1 contains the heading information
	# Row #2 is where the player stats start.

	headings = rows[1].findAll(text=True)
	cat = []
	for i in range(1,len(headings),2):
		temp_cat = str(re.sub('[\t\r\n\f\v]* (?= )', "", headings[i])).strip()
		cat.append(temp_cat)
	
	stats = []
	for j in range(2,len(rows)):
		player_stats=rows[j].findAll(text=True)
		stats.append([])
		for i in range(1,len(player_stats)):
			# Strip out all the whitespace. If there is only whitespace left, do nothing
			# Replace ' with '' so that players like 'Le'Veon Bell' will get included
			# in the rankings properly.

			temp_stats = str(re.sub('[\t\r\n\f\v]* (?= )', "", player_stats[i])).strip()
			temp_stats = str(re.sub('\'','\'\'',temp_stats))
			if temp_stats == "":
				pass
			else:
				if i>=6:
					stats[j-2].append(int(temp_stats))
				else:
					stats[j-2].append(temp_stats)
	#print cat
	#for i in range(0,len(stats)):
	#	print stats[i]
	#print 'Length of headings: ' + str(len(cat)) + ' Length of stats: ' + str(len(stats[0]))

	return week, position, cat, stats

if __name__ == "__main__":
	main()
