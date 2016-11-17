#! /usr/bin/python2
#
#
## This is the program to grab data directly from the Yahoo
## website. Needs to use the oauth tokens, pulls data, stores
## it in the PostgreSQL database.

## Yahoo MLB 2016 Game ID = 357


import sys
import xml.etree.ElementTree as ET
import pg
from yahoo_oauth import OAuth1

def main():
    ''' This is the main function. Starts the call to all the other subroutines.
    Needs to authorize with Yahoo first, and then start calling various URLs based
    on the parameters given at the commandline. '''
    
    # The first argument from the commandline is the week_id
    week_id = int(sys.argv[1])
    
    # Variables
    game_id = 357
    league_id = 144084
    base_url = 'http://fantasysports.yahooapis.com/fantasy/v2/'

    # Code below to generate a session. When already authorized, no need.
    # session = y_auth()
    oauth = OAuth1(None, None, from_file='/home/chris/python/oauth1.json')
    
    if not oauth.token_is_valid():
        oauth.refresh_access_token()
    
    #standings(oauth, base_url, league_id, game_id)
    #weekly_stats(oauth, base_url, league_id, game_id, 10, 4)
    #stat_categories(oauth, base_url, game_id)
    # Dictionary of all the stats in the Yahoo MLB game.
    # Pass that dictionary and a list of relevant stats to the league_stats function
    # Get two dictionaries that define the relationships between stat_id and stat_names
    # for each relevant stat.
    
    stat_dict = {'NSVH': '90', 'OFA': '86', 'DPT': '87', '2B': '10', 'BB': '39', 'HR': '12', 'IBB': '40', 'SB%': '63', 'K/9': '57', 'H/AB': '60', 'SVOP': '47', '1B': '9', 'BB/9': '78', 'ERA': '26', 'WP': '43', '3B': '11', 'HBP': '41', 'WIN%': '75', 'BLK': '44', 'CYC': '64', 'RW': '70', 'GIDP': '46', '1BA': '76', 'NSB': '62', 'SV%': '81', 'H/9': '77', 'PG': '80', 'E': '53', 'W': '28', 'PC': '67', 'PA': '65', 'WHIP': '27', 'IP': '50', 'RL': '71', 'SV+H': '89', 'ER': '37', 'TB': '49', 'PO': '51', '2BA': '68', 'OUT': '33', 'A': '52', 'NH': '79', 'CI': '88', 'AB': '6', 'GS': '25', 'OBPA': '74', 'GP': '1', 'H': '8', 'K': '42', 'TEAM': '58', 'CG': '30', 'L': '29', 'R': '7', 'RBI': '13', 'CS': '17', 'PICK': '72', 'SLG': '5', 'OBP': '4', 'SHO': '31', 'QS': '83', 'LEAGUE': '59', 'IRA': '82', 'HLD': '48', 'RAPP': '73', 'OPS': '55', 'APP': '24', 'SV': '32', 'NSV': '85', 'BSV': '84', 'SH': '14', 'K/BB': '56', '3BA': '69', 'SB': '16', 'SLAM': '66', 'AVG': '3', 'XBH': '61', 'SF': '15', 'FPCT': '54', 'TBF': '35'}
    rel_stat_id, rel_stat_key = league_stats(['R','H','HR','RBI','SB','AVG','W','CG','SV','K','ERA','WHIP','IP', 'H/AB'],stat_dict)

    league_week_update(oauth, base_url, league_id, game_id, week_id, rel_stat_id)


def league_week_update(oauth, base_url, league_id, game_id, week_id, rel_stat_id):
    ''' Update the entire league stats for the week. Takes a handful of arguments,
    but effectively just grabs the data for the week and uploads it to the PostgreSQL
    database. '''
    # Cycle through the teams, Get their weekly stats, then pair up the stats_id
    # with the name of the stat.

    for i in range(1,9):
        team_week_stats = weekly_stats(oauth, base_url, league_id, game_id, week_id, i)
    #    print team_week_stats
        team_id = i
        temp_stat_dict = {}
        for key in team_week_stats.iterkeys():
            temp_stat_dict[rel_stat_id[key]] = team_week_stats[key]
        
        update_db(temp_stat_dict, team_id, week_id)
        #print temp_stat_dict

def update_db(temp_stat_dict, team_id, week_id):
    ''' Update the database with the weekly stats. Build the query. Open the connection.
    Insert update. Splits the H/AB into just AB. '''

    r = str(temp_stat_dict['R'])
    h = str(temp_stat_dict['H'])
    hr = str(temp_stat_dict['HR'])
    rbi = str(temp_stat_dict['RBI'])
    avg = str(temp_stat_dict['AVG'])
    ab = str(temp_stat_dict['H/AB']).split('/')[1] 
    sb = str(temp_stat_dict['SB'])
    w = str(temp_stat_dict['W'])
    cg = str(temp_stat_dict['CG'])
    sv = str(temp_stat_dict['SV'])
    k = str(temp_stat_dict['K'])
    era = str(temp_stat_dict['ERA'])
    whip = str(temp_stat_dict['WHIP'])
    ip = str(temp_stat_dict['IP'])
    team_id = str(team_id)
    week_id = str(week_id)

    values = ",".join([r, h, hr, rbi, avg, sb, ab, w, cg, sv, k, era, whip, ip])

    query = 'insert into baseball(week_id, team_id, r, h, hr, rbi, avg, sb, ab, w, cg, sv, k, era, whip, ip) values(' + week_id + ',' + team_id +',' + values + ')'
    print query
    con = pg.connect(dbname='baseball', host='localhost', user='chris')

    con.query(query)

    con.close()


def league_stats(stat_list, stat_dict):
    ''' Get the stat_ids of all the stats that are relevant to the league. Returns two
    dictionaries of {id: name} and {name: id} '''
    
    stat_ids = {}
    stat_keys = {}
    for key in stat_list:
        stat_ids[stat_dict[key]] = key
        stat_keys[key] = stat_dict[key]

    
    return stat_ids, stat_keys
    
def standings(oauth, base_url, league_id, game_id):
    ''' Gets the league standings. '''
    
    url = base_url + 'league/' + str(game_id) + '.l.' + str(league_id) + '/standings'
    #print url
    r = oauth.session.get(url, data='body')
    #print r.content
    

def weekly_stats(oauth, base_url, league_id, game_id, week_id, team_id):
    ''' Get the weekly stats for a team for a given week. '''
    
    url = base_url + 'team/' + str(game_id) + '.l.' + str(league_id) + '.t.' + str(team_id) + '/stats;type=week;week=' +str(week_id)
    # print url
    r = oauth.session.get(url, data='body')
    root = ET.fromstring(r.content)
    week_stat_id = {}
    print r.content
    # Print out all the stats.
    # Stats are nested at: [0][12][2] in the XML structure.
    # Add a dictionary entry for the stat_id into the week_stat_id dictionary.
    
    # This little bit of code is because of the 'is_owned_by_current_login' that only shows
    # up for my team.
    
    if team_id==4:
        n=15
    elif team_id in [1,6,5]:
        n=14
    else:
        n=13
    
    for stat in root[0][n][2]:
        #print team_id
        #print stat[0].text, stat[1].text
        week_stat_id[stat[0].text] = stat[1].text
    
    return week_stat_id

def stat_categories(oauth, base_url, game_id):
    ''' Get the definitions for all the stat categories. Prints a dictionary of all the stats.
    This is really just a function that was meant to be used a single time. '''
    
    url = base_url + 'game/' + str(game_id) + '/stat_categories'
    
    r = oauth.session.get(url, data='body')
    
    # Print all the stat categories.
    root = ET.fromstring(r.content)
    # Stats are nested at the third level... [0][8][0]
    stat_dict = {}
    print 'ID\tStat\tSort'
    for stat in root[0][8][0]:
        print stat[0].text + '\t' + stat[2].text + '\t' + stat[3].text
        stat_dict[stat[0].text]=stat[2].text
    print stat_dict

if __name__ == "__main__":
    main()
