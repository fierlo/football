#! /usr/bin/python2
#
#
## This is the program to grab data directly from the Yahoo
## website. Needs to use the oauth tokens, pulls data, stores
## it in the PostgreSQL database.
## 


import sys
import xml.etree.ElementTree as ET
from pg import DB
from yahoo_oauth import OAuth1
from itertools import permutations

def main():
    ''' This is the main function. Starts the call to all the other subroutines.
    Needs to authorize with Yahoo first, and then start calling various URLs based
    on the parameters given at the commandline. '''
    
    # The first argument from the commandline is the week_id
    week_id = int(sys.argv[1])
    update_only = 0

    # If there's a second argument... assign it. Otherwise, nothing to do here.
    #
    try:
        update_only = int(sys.argv[2])
    except:
        pass
    
    year = sys.argv[3]

    # Variables for NFL 2016
    
    game_id, league_id = constants(year)

    base_url = 'http://fantasysports.yahooapis.com/fantasy/v2/'
    
    # The 'ns' variable is used to make it easier to iterate through the XML tags.
    # all the XML tags returned by Yahoo have it prepended to the tag.
    # e.g., {http://fantasysports.yahooapis.com/fantasy/v2/base.rng}player_key
    ns = '{http://fantasysports.yahooapis.com/fantasy/v2/base.rng}'

    # Code below to generate a session. When already authorized, no need.

    oauth = OAuth1(None, None, from_file='/home/chris/python/oauth1.json')
    
    if not oauth.token_is_valid():
        oauth.refresh_access_token()
    
    con = dbcon()
    
    # Dictionary of all the stats. In the form of {stat_id: display_name}
    stat_dict = {'56': 'Pts_Allow_35', '54': 'Pts_Allow_21_27', '51': 'Pts_Allow_1_6', '50': 'Pts_Allow_0', '53': 'Pts_Allow_14_20', '82': 'XPR', '52': 'Pts_Allow_7_13', '20': 'FG_20_29', '21': 'FG_30_39', '22': 'FG_40_49', '23': 'FG_50', '29': 'PAT_Made', '5': 'Pass_TD', '4': 'Pass_Yds', '6': 'Int_Thrown', '9': 'Rush_Yds', '8': 'Rush_Att', '78': 'Targets', '11': 'Rec', '10': 'Rush_TD', '13': 'Rec_TD', '12': 'Rec_Yds', '15': 'Ret_TD', '16': 'Two_PT', '19': 'FG_0_19', '18': 'Fum_Lost', '57': 'Fum_Ret_TD', '49': 'Kick_and_Punt_Ret_TD', '37': 'Blk_Kick', '36': 'Safe', '35': 'TD', '34': 'Fum_Rec', '33': 'Intercepted', '55': 'Pts_Allow_28_34', '32': 'Sack', '31': 'Pts_Allow', '48': 'Ret_Yds'}
    
    if update_only == 1:
#        This is the codepath to compare the teams and print out the season report up to the year/week.

        if int(year)<2016:
            team_range = range(1,11)
        else:
            team_range = range(1,12)
        for team_id in team_range:
            res_insert = optimal_lineup(week_id,team_id,year,con)

        bench_points(week_id,year,con)
        
        lineups = ['actual', 'optimal','draft']
        for lineup in lineups:
            compare_teams(week_id,lineup,year,res_insert,con)
            
    elif update_only == 2:
        print 'Going through the temp route.'
        
        # getmatchup(oauth,ns,game_id,league_id,base_url,year,week_id,con)
        # getteamyear(oauth,ns,game_id,league_id,base_url,year,con)
        # draftresults(oauth, game_id, league_id, base_url, ns, year)
        getplayerpoints(oauth,ns,game_id,league_id,base_url,week_id,'roster',year, stat_dict,con)

    else:
#        print 'Following codepath #1'
        # Add column to 'draft' for the current week.
        try:
            con.query('alter table draft add column week_' + str(week_id) + ' numeric')
        except:
            print 'Column for Week ' + str(week_id) + ' already exists.'
        
        # Get the roster for the given week.
    
        getroster(oauth,ns,game_id,league_id,base_url,week_id,year,con)
    
        # Get the top 5 free agents at each position. Only for the 2016 season. Can't retroactively
        # figure this out.
        if year=='2016':
            getfreeagents(oauth,ns,game_id,league_id,base_url,week_id,year,con)
            getplayerpoints(oauth,ns,game_id,league_id,base_url,week_id,'draft',year,stat_dict,con)

        # Get the points for every player on every roster for the week.
        getplayerpoints(oauth,ns,game_id,league_id,base_url,week_id,'roster',year, stat_dict,con)
    
        # Update the playerinfo table to reflect names not yet in the system.
        update_pinfo(oauth,ns,game_id,league_id,base_url,con)

        # Get the matchup info
        getmatchup(oauth,ns,game_id,league_id,base_url,year,week_id,con)
        
    con.close()


def bench_points(week_id,year,con):
    ''' Get all dem bench points. Grab from roster for teams with started=0, sorted by sum. '''
    
    bench_results = con.query('select team.abbr, sum(points) from roster join team using (team_id, year) where week_id=' + str(week_id) + ' and team.team_id<=10 and started=0 and roster.year=' + year + ' group by 1 order by 2 desc').getresult()

    print 'Bench Winners for Week ' + str(week_id) + '\n'

    for i in range(0,len(bench_results)):
        print bench_results[i][0].center(8) + '|' + str(bench_results[i][1]).center(8) + '|'

    print '\n\n'

def optimal_lineup(week_id,team_id,year,con):
    ''' Determine the optimal lineup for a team. Fills out QB, WR, WR, WR, RB, RB, W/R/T, K, DEF.
    Returns a 1 if the results were inserted into the database, a 0 if the results already exist.
    The returned value is used to determine how the code will be executed later, namely whether
    update_season gets called. '''

    opt_lineup, opt_points = opt_routine(week_id, team_id, 'optimal', year, con)
    
    # Get the team name for the report.
    
    team_name = con.query('select team_name from team where year=' + str(year) + ' and team_id='+str(team_id)).getresult()[0][0]
    
    # If the team isn't the free agent team, then check how many points were actually netted for
    # the week. This is stored as team_points.

    if team_id!=11:
        team_points = con.query('select sum(points) from roster where team_id=' + str(team_id) + ' and week_id=' + str(week_id) + ' and year=' + year + ' and started=1').getresult()[0][0]
        bench_points = con.query('select sum(points) from roster where team_id=' + str(team_id) + ' and week_id=' + str(week_id) + ' and year=' + year + ' and started=0').getresult()[0][0]
    else:
        team_points = 0
        bench_points = 0
    
    #print opt_lineup
        
    #print opt_points
    
    print 'Optimal Lineup for ' + team_name
    print '\n--------------------------------------\n'
    print '[ QB  ]: (' + str(opt_lineup['QB'][0][1]).rjust(6) + ') ' + opt_lineup['QB'][0][0]
    print '[ WR1 ]: (' + str(opt_lineup['WR'][0][1]).rjust(6) + ') ' + opt_lineup['WR'][0][0]
    print '[ WR2 ]: (' + str(opt_lineup['WR'][1][1]).rjust(6) + ') ' + opt_lineup['WR'][1][0]
    print '[ WR3 ]: (' + str(opt_lineup['WR'][2][1]).rjust(6) + ') ' + opt_lineup['WR'][2][0]
    print '[ RB1 ]: (' + str(opt_lineup['RB'][0][1]).rjust(6) + ') ' + opt_lineup['RB'][0][0]
    print '[ RB2 ]: (' + str(opt_lineup['RB'][1][1]).rjust(6) + ') ' + opt_lineup['RB'][1][0]
    if int(year)<2015:
        print '[ TE1 ]: (' + str(opt_lineup['TE'][0][1]).rjust(6) + ') ' + opt_lineup['TE'][0][0]
    else:
        print '[ FLX ]: (' + str(opt_lineup['Flex'][0][1]).rjust(6) + ') ' + opt_lineup['Flex'][0][0]
    print '[  K  ]: (' + str(opt_lineup['K'][0][1]).rjust(6) + ') ' + opt_lineup['K'][0][0]
    print '[ DEF ]: (' + str(opt_lineup['DEF'][0][1]).rjust(6) + ') ' + opt_lineup['DEF'][0][0]
    print '\n--------------------------------------\n'
    print '[ OPT ]: (' + str(opt_points).rjust(6) + ')'
    print '[ ACT ]: (' + str(team_points).rjust(6) + ')'
    print '[BENCH]: (' + str(bench_points).rjust(6) + '}'
    print '\n\n'

    opt_lineup, draft_points = opt_routine(week_id, team_id, 'draft', year,con)

    # Insert the values for the optimal lineup and actual points into the results table in
    # the database.

    try:
        con.insert('results',team_id=team_id,week_id=week_id,actual=team_points,optimal=opt_points,draft=draft_points,year=year)
        res_insert = 1
    except:
        res_insert = 0
    
    return res_insert


def opt_routine(week_id, team_id, res_type,year, con):
    ''' Get the lineup and points for each player in the optimal lineup. '''
    
    # Start getting the skill positions. Get up to 15 people on the team, and then pop it out.
    # 
    
    query_dict = {'optimal': ['select players.name, players.position, roster.points from roster join players on players.player_id=roster.player_id where roster.week_id=' + str(week_id) + ' and year=' + year + ' and team_id=' + str(team_id) + ' order by 3 desc'] ,
                'draft' : ['select players.name, players.position, draft.week_' + str(week_id) + ' from draft join players on players.player_id=draft.player_id where team_id=' + str(team_id) + ' and year=' + year + ' order by 3 desc'] }
    
    plist = con.query(query_dict[res_type][0]).getresult()
    
    #for i in range(0,len(plist)):
    #    print plist[i][0] + '\t' + plist[i][1] + '\t' + str(plist[i][2])

    opt_lineup = {'QB': [], 'WR': [], 'RB': [], 'TE': [], 'Flex': [], 'K': [], 'DEF': []}
    
#    print plist

    # Using the dictionary above, parse the optimal lineup based on the league lineup.
    # The results from the query are sorted from highest points to lowest for the week.
    # If it finds a position, it checks to see how many are in the optimal lineup,
    # if less than the required amount, it keeps going. This ensures that the flex position
    # gets filled with the highest scoring non-QB position in the 'RB','WR','TE' elif line.

    for i in range(0,len(plist)):
        if plist[i][1] == 'QB' and len(opt_lineup['QB'])<1:
            opt_lineup[plist[i][1]].append([plist[i][0], plist[i][2]])
        elif plist[i][1] == 'WR' and len(opt_lineup['WR'])<3:
            opt_lineup[plist[i][1]].append([plist[i][0], plist[i][2]])
        elif plist[i][1] == 'RB' and len(opt_lineup['RB'])<2:
            opt_lineup[plist[i][1]].append([plist[i][0], plist[i][2]])
        elif plist[i][1] == 'TE' and len(opt_lineup['TE'])<2 and int(year)<2015:
            opt_lineup[plist[i][1]].append([plist[i][0], plist[i][2]])
        elif plist[i][1] in ['RB','WR','TE'] and len(opt_lineup['Flex'])<1 and int(year)>2014:
            opt_lineup['Flex'].append([plist[i][0], plist[i][2]])
        elif plist[i][1] == 'K' and len(opt_lineup['K'])<1:
            opt_lineup['K'].append([plist[i][0], plist[i][2]])
        elif plist[i][1] == 'DEF' and len(opt_lineup['DEF'])<1:
            opt_lineup['DEF'].append([plist[i][0], plist[i][2]])

    points = 0
    for pos,player in opt_lineup.iteritems():
        for j in range(0,len(player)):
            # print player[j][1], opt_points
            points += player[j][1]

    return opt_lineup, points

def compare_teams(week_id,lineup,year,res_insert,con):
    ''' Generate the report for the week. '''
    
    # Compare the actual lineups for each team, compare the optimal lineups for each team
    # Compare the actual lineups for each team against other as a round-robin for the entire
    # season rankings.
    
    if lineup=='optimal' and int(year)>2015:
        team_length = '11'
    else:
        team_length = '10'
    
    act_points = con.query('select team.team_id, team.abbr, team.team_name, ' + lineup + ' from results join team using (team_id, year) where team.year=' + str(year) + ' and week_id=' + str(week_id) + ' and results.team_id<=' + team_length + ' order by 1').getresult()
#    print act_points
    
    # Initialize a dictionary with the results, and a blank list for each.
    results = {}
    team_list = []
    for j in range(0,len(act_points)):
        results[act_points[j][1]] = []
        team_list.append(act_points[j][1])

    perms = permutations(act_points,2)
    for p in perms:
        results[p[0][1]].append([p[1][0],int(p[1][3]<p[0][3])])
    
    # If res_insert is 1, then execute update_season. If res_inert is 0, the data has already been updated.
    # 
    if res_insert==1:
#        print 'Updating the season table for ' + lineup
        update_season(results,team_list,lineup+'_results',year,con)

#    print results

    report(results,team_list,lineup,week_id)
    
    # This just generates the season report at the end of the whole output.
    if lineup=='draft':
        season_report(week_id,year,con)


def season_report(week_id,year,con):
    ''' The season long what-if report is generated from the actual_results table. 
    Prints out a table similar to what is produced in the report function. '''

    if year=='2016':
        res_type = ['draft','actual','optimal']
    else:
        res_type = ['draft','optimal','actual']
    
    for lineup in res_type:
        result = con.query('select team.abbr, ' + lineup + '_results.* from '+lineup+'_results join team using (team_id, year) where team.year=' + str(year) + ' order by team.team_id').getresult()

        # Print the header row

        header = 'Season Long What-If Scenarios for ' + lineup.capitalize() + ' Lineups: Week ' + str(week_id)
        print header.upper().center(99) + '\n'
        table = '|'.rjust(9)
        for j in range(0,len(result)):
            table += result[j][0].upper().center(8) + '|'

        print table

        # Generate the table. To through the results from the PostgreSQL query result
        # Put an x for when you play yourself.
        # Since it a season long matchup, make sure to show the record (wins - week_id-wins).
        # The inner 'for' loop has a -1 to the length of the result because the 'year' column is at the
        # end of the table.
        # If it's the optimal lineup for a non-2016 year, remove column 11 from the results as there
        # is no free agent data for those years.
        
        if lineup=='optimal' and int(year)<2016:
            table_size = len(result)-2
        else:
            table_size = len(result)

        for j in range(0,len(result)):
            row_text = result[j][0].upper().center(8) + '|'
            if lineup=='optimal' and int(year)<2016:
                table_size = len(result[j])-2
            else:
                table_size = len(result[j])-1
            for i in range(2,table_size):
                if j==(i-2):
                    row_text += 'x'.center(8) + '|'
                else:
                    row_temp = str(int(result[j][i])) + '-' + str(week_id - int(result[j][i]))
                    row_text += row_temp.center(8) + '|'
            print row_text

        print '\n\n\n'

def update_season(results,team_list,res_type,year,con):
    ''' This function accepts the results dictionary, team_list and the DB connection.
    Goes through the results, and updates the database line by
    adding the number of wins against each opponent. '''

    # The results dictionary is ordered with team_id and win/losses
    # e.g., {'PIGSKIN': [[1,0], [2,1], [3,0], [...], [10,0]] }
    
    for team in team_list:
        team_id = team_list.index(team)+1
        update_query = 'update ' + res_type + ' set '
        update_query2 = str()
        for i in range(0,len(results[team])):
                # Only update the column if result is a win.
                if results[team][i][1]==1:
                    if len(update_query2)!=0:
                        update_query2 += ', '
                    column = 'col' + str(results[team][i][0])
                    update_query2 += column + '=' + column + '+1 '
#                    print update_query2
        # If no data is added to query2, then there aren't any wins to update.
        # 
        if len(update_query2)!=0:
            con.query(update_query + update_query2 + 'where year=' + year + ' and team_id=' + str(team_id))

def report(results,team_list,lineup,week_id):
    ''' Takes results dictionary and list of teams and generates a table. '''
    
    # Get the header setup for the table.

    header = lineup + ' lineup for week ' + str(week_id)
    print header.upper().center(99) + '\n'
    
    # Set up the header row of the table.
    table = '|'.rjust(9)
    
    for j in team_list:
        table += j.center(8) + '|'
    table += 'RECORD'.center(8) + '|'
    print table
    
    # Generate the what-if scenario for each team. 
    # It is based on the length of results to accommodate for
    # the differences between the actual and optimal lineups.
    for team in team_list:
        wins = 0
        row_text = team.center(8) + '|'
        for i in range(0,len(results[team])):
            # Check to see if the team_id-1 (position in team_list) is
            # equal to 'i'. If equal, means that it is a round robin 
            # against the same team.
#            print results[team][i][0]-1, team_list.index(team)
            if team_list.index(team)==i:
                row_text += 'x'.center(8) + '|'
            if results[team][i][1]==1:
                row_text += 'W'.center(8) + '|'
                wins += 1
#                print team + ' victorious over ' + team_list[results[team][i][0]-1]
            elif results[team][i][1]==0:
                row_text += 'L'.center(8) + '|'
#                print team + ' lost to ' + team_list[results[team][i][0]-1]
        if team_list.index(team)==len(team_list)-1:
            row_text += 'x'.center(8) + '|'
        win_loss = str(wins) + '-' + str(len(team_list)-1-wins)
        row_text += win_loss.center(8) + '|'
        print row_text
    print '\n\n\n'
    

def getfreeagents(oauth,ns,game_id,league_id,base_url,week_id,year,con):
    ''' This function gets the player_id of the top 5 free agents for a given week
    at their position. May need to be run before waivers. '''

    # http://fantasysports.yahooapis.com/fantasy/v2/league/223.l.431/players;position=QB;status=A;sort=60;sort_type=week;sort_week=1
    positions = ['QB','RB','WR', 'TE', 'K','DEF']

    fa_data = []

    for i in positions:
        url = base_url + 'league/' + str(game_id) + '.l.' + str(league_id) + '/players;position=' + i + ';status=A;sort=PTS;sort_type=week;sort_week=' + str(week_id) + ';count=5'
        r = oauth.session.get(url, data='body')
        # print r.content
        root = ET.fromstring(r.content)
        league = root.find(ns+'league')
        players = league.find(ns+'players')
        for player in players.findall(ns+'player'):
            player_id = player.find(ns+'player_id').text
            full_name = player.find(ns+'name')
            full_name = full_name.find(ns+'full').text
            pos = player.find(ns+'eligible_positions')
            pos = pos[0].text
            fa_data.append((11,week_id,player_id,None,0,int(year),None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None))

    con.inserttable('roster',fa_data)


def getplayerpoints(oauth,ns,game_id,league_id,base_url,week_id,res_type,year,stat_dict,con):
    ''' Get the points for all the players in the league for a given week. 
    res_type can be either roster or drafted. '''
    
    # http://fantasysports.yahooapis.com/fantasy/v2/league/223.l.431/players;player_keys=223.p.22234,223.p.88302/stats;type=week;week=1
    
    # Change the query based on whether the results are for the drafted players or for rostered players.
    # This is all based on how the data is stored in the database.
    
    query_dict = {'roster': ['select player_id from roster where points is NULL and week_id=' + str(week_id) + ' and year=' + year + ' order by player_id limit 25', 'update roster set points=', ' where week_id=' + str(week_id) + ' and year=' + year + ' and '],
                'draft': ['select player_id from draft where year=' + str(year) + ' and week_' + str(week_id) + ' is NULL order by player_id limit 25', 'update draft set week_'+str(week_id)+'=', ' where year='+ year + ' and ']}
    
    # Selects all the player_id associated with null values from the roster table for the selected week_id.
    # Join all the player_id together. Request the player stats from the fantasy API.
      
    # Do this via a while loop so that it only requests 25 players at a time.
    # 
    missing_data = 25

    while missing_data>0:
        plist = con.query(query_dict[res_type][0])
    
        missing_data = len(plist.getresult())
        print 'Updating ' + str(missing_data) + ' player fantasy points for Week ' + str(week_id)
        pre_key = str(game_id) + '.p.'
        player_keys = []
    
        for i in plist.getresult():
            player_keys.append(pre_key + str(i[0]))
        # print player_keys
    
        # print player_keys
        # print ','.join(player_keys)
    
        url = base_url + 'league/' + str(game_id) + '.l.' + str(league_id) + '/players;player_keys=' + ','.join(player_keys) + '/stats;type=week;week=' + str(week_id)
    
        # print url

        r = oauth.session.get(url, data='body')
        root = ET.fromstring(r.content)    
        league = root.find(ns+'league')
        players = league.find(ns+'players')
        
        for player in players.findall(ns+'player'):
            player_id = player.find(ns+'player_id').text
            points = player.find(ns+'player_points')
            points = points.find(ns+'total').text
    
            query = query_dict[res_type][1] + str(points) + query_dict[res_type][2] + 'player_id=' + str(player_id)
            con.query(query)
        
        if res_type == 'roster':
            for player in players.findall(ns+'player'):
                update_query = 'update roster set '
                player_id = player.find(ns+'player_id').text
                update_query2 = query_dict[res_type][2] + 'player_id=' + str(player_id)
                player_stats = player.find(ns+'player_stats')
                stats = player_stats.find(ns+'stats')
                for stat in stats.findall(ns+'stat'):
                    stat_id = stat.find(ns+'stat_id').text
                    value = stat.find(ns+'value').text
                    update_query += stat_dict[stat_id] + '=' + value + ', '
                    
                con.query(update_query[:-2] + update_query2)
                    
        # Grab the data again to see how many are left.

        plist = con.query(query_dict[res_type][0])
    
        missing_data = len(plist.getresult())

        # print 'Still missing ' + str(missing_data) + ' player entries for Week ' + str(week_id)


def getroster(oauth,ns,game_id,league_id,base_url,week_id,year,con):
    ''' Get the roster information for all the teams. '''

    # http://fantasysports.yahooapis.com/fantasy/v2/team/game_id.l.league_id.t.team_id/stats;type=week;week=2
    # Blank list roster_data list is initiated to start populating the database for the week.
    
    roster_data = []
    for team_id in range(1,11):
        url = base_url + 'team/' + str(game_id) + '.l.' + str(league_id) + '.t.' + str(team_id) + '/roster;week=' + str(week_id)
        r = oauth.session.get(url, data='body')
#        print r.content
        root = ET.fromstring(r.content)
        
        team = root.find(ns+'team')
        roster = team.find(ns+'roster')
        players = roster.find(ns+'players')
        
        for player in players.findall(ns+'player'):
            player_id = player.find(ns+'player_id').text
            sel_pos = player.find(ns+'selected_position')
            sel_pos = sel_pos.find(ns+'position')
            if sel_pos.text == 'BN':
                started = 0
            else:
                started = 1
#            print player_id + '\t' + str(started)
            roster_data.append((team_id, week_id, player_id, None, started, int(year),None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None))
        
    
    con.inserttable('roster', roster_data)
    

def update_pinfo(oauth,ns,game_id,league_id,base_url,con):
    ''' Get the data for players that do not exist in a specific table. '''
    
    query = 'select distinct roster.player_id from roster where not exists (select players.player_id from players where players.player_id=roster.player_id) order by 1'
    plist = con.query(query)
    for i in plist.getresult():
        print 'Getting playerinfo for player_id: ' + str(i[0])
        getpinfo(oauth,ns,game_id,league_id,base_url,con,i[0])
        

def getpinfo(oauth,ns,game_id,league_id,base_url,con,player_id):
    ''' This function is here to get the player name/position and add it to the
    player table. Need to figure out when to call this. '''
    
    # http://fantasysports.yahooapis.com/fantasy/v2/player/223.p.5479
    
    # Get the data for the player_id
    
    url = base_url + 'player/' + str(game_id) + '.p.' + str(player_id)
    r = oauth.session.get(url, data='body')
    root = ET.fromstring(r.content)
    # print r.content
    
    for child in root.iter(ns+'full'):
        fullname = child.text
   
    for child in root.iter(ns+'eligible_positions'):
        position = child[0].text

    # Make the query to update the players table
    con.insert('players',name=fullname,player_id=player_id,position=position)
        

def draftresults(oauth, game_id, league_id, base_url, ns, year):
    ''' This gets all the data from the draft and puts it into the PostgreSQL database '''

    con = dbcon()
    query = 'insert into draft(team_id,player_id,round,pick,year) values'
    
    for i in range(1,11):
        team_id = i
        url = base_url + 'team/' + str(game_id) + '.l.' + str(league_id) + '.t.' + str(team_id) + '/draftresults'
        r = oauth.session.get(url, data='body')
        #print url
        root = ET.fromstring(r.content)
        #print r.content
       
        team = root.find(ns+'team')
        draft_results = team.find(ns+'draft_results')
        for draft_result in draft_results.findall(ns+'draft_result'):
            pick = draft_result.find(ns+'pick').text
            round = draft_result.find(ns+'round').text
            player_id = draft_result.find(ns+'player_key').text
            player_id = player_id[6:]

            values = '(' + str(team_id) + ',' + player_id + ',' + round + ',' + pick + ',' + year + ')'
#            print values
            query = query + values + ','
#            print query[:-1]
        
    con.query(query[:-1])
    con.close()


def getmatchup(oauth,ns,game_id,league_id,base_url,year,week_id,con):
    ''' Get the matchups for a given week. Store it in the matchups table in the database. '''
    
    # The matchup table is year, week_id, team1, team1_proj, team2, team2_proj
    # http://fantasysports.yahooapis.com/fantasy/v2/league/223.l.431/scoreboard;week=2
    # matchup_data is a list of tuples to be inserted into the matchup table.
    matchup_data = []
    
    url = base_url +'league/' + str(game_id) + '.l.' + str(league_id) + '/scoreboard;week=' + str(week_id)
    r = oauth.session.get(url, data='body')
#    print r.content
    root = ET.fromstring(r.content)
    league = root.find(ns + 'league')
    scoreboard = league.find(ns+'scoreboard')
    matchups = scoreboard.find(ns+'matchups')
    
    # Walk through the matchups, find the team_id and projected points for the matchup.
    #
    for matchup in matchups.findall(ns+'matchup'):
        teams = matchup.find(ns+'teams')
        team_id = []
        team_proj = []
        for team in teams.findall(ns+'team'):
            team_id.append(int(team.find(ns+'team_id').text))
            team_projected = team.find(ns+'team_projected_points')
            team_proj.append(team_projected.find(ns+'total').text)
        
        team1_id = team_id[0]
        team1_proj = team_proj[0]
        team2_id = team_id[1]
        team2_proj = team_proj[1]
        
#        print str(team1_id) + ' (' + str(team1_proj) + ')\t' + str(team2_id) + ' (' + str(team2_proj) + ')'
        matchup_data.append((int(year),week_id,team1_id,team1_proj,team2_id,team2_proj))
        matchup_data.append((int(year),week_id,team2_id,team2_proj,team1_id,team1_proj))
#    print matchup_data
    con.inserttable('matchup', matchup_data)
        
def getteamyear(oauth,ns,game_id,league_id,base_url,year,con):
    ''' This routine gets the team information for any given year. It updates the
    team table with the year, team_id, team name. '''
    
    # http://fantasysports.yahooapis.com/fantasy/v2/league/223.l.431/teams
    url = base_url + 'league/' + str(game_id) + '.l.' + str(league_id) + '/teams'
    r = oauth.session.get(url, data='body')
#        print r.content
    root = ET.fromstring(r.content)
    print r.content
    league = root.find(ns + 'league')
    teams = league.find(ns+'teams')
    team_data = []
    for team in teams.findall(ns+'team'):
        team_id = int(team.find(ns+'team_id').text)
        name = team.find(ns+'name').text

        team_data.append((team_id, name, None, int(year)))
        
    
    con.inserttable('team', team_data)

def constants(year):
    ''' Returns the game_id and league_id for the year. '''
    
    id = {'2016': [359, 256486], 
                '2015': [348, 371961],
                '2014': [331, 744428],
                '2013': [314, 280363],
                '2012': [273, 427590],
                '2011': [257, 629966],
                '2010': [242, 728356] }
                
    game_id = id[year][0]
    league_id = id[year][1]
    
    return game_id, league_id

def dbcon():
    ''' Starts database connection. Returns PostgreSQL connection object. '''
    
    con = DB(dbname='football', host='localhost', user='chris')
    
    return con


if __name__ == "__main__":
    main()
