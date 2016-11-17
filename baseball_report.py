#! /usr/bin/python2
#
#
## Program to generate what-if scenarios for baseball
## Calls to PostgreSQL database and compares how a team
## would have performed for a given week if it had been
## matched up against every team.
##

import sys
import pg
from operator import itemgetter


def main():
    ''' This is the main routine. The function expects one argument, which is
    an integer representing the week number. '''
    
    week_id = int(sys.argv[1])
    
    # Set up the constants. Need a list of the stats to make a dictionary that is easy to call.
    
    stats = {'r': 1, 'h': 2, 'hr': 3, 'rbi': 4, 'sb': 5, 'avg': 6, 'w': 7, 'cg': 8, 'sv': 9, 'k': 10, 'era': 11, 'whip':12}
    low_stats = ['era','whip']
    
    # Initialize dictionaries that will store the record for the week, season and team names for lookup.
    
    team_record = {1: [0, 0, 0], 2: [0, 0, 0], 3: [0, 0, 0], 4: [0, 0, 0], 5: [0, 0, 0], 6: [0, 0, 0], 7: [0, 0, 0], 8: [0, 0, 0]}
    team_names = {6: 'A Rod 4 Your Pujols', 3: 'Balls Deep', 2: "RBI'd ForHerPleasure", 5: 'Designated Shitter', 4: 'THE WINGDINGERS', 8: 'Joe Buck yourself', 7: "Who's on 1st", 1: 'Where My Pitches At?'}
    
    # Generate the team records for a given week. The function uses a for loop, so week_id to start, and week_id+1
    # for the range will return only the desired week.

    team_record = week_report(week_id, week_id+1, team_record, team_names, stats, low_stats)
    
    # Call format_report which sorts and prints the team records for the week.
    
    format_report(team_names,team_record,week_id)
    
    # Compile the rest of the weeks (Weeks 1 through N-1).
    
    team_record = week_report(1, week_id, team_record, team_names, stats, low_stats)
 
    # Call format_report with -1 to report for the entire season.
 
    format_report(team_names,team_record,-1)

    # Produce a ranked report for each stat category.

    team_rank = rank_report(week_id, week_id+1, team_names, stats, low_stats)

    team_rank_report = format_rank_report(team_names, team_rank)


def format_rank_report(team_names, team_rank):
    ''' Takes the dictionary of the team rankings, produces an ordered list of team rankings for each stat. '''

    stats = ['r', 'h', 'hr', 'rbi', 'sb', 'avg', 'w', 'cg', 'sv', 'k', 'era', 'whip']
    stat_names = ['Runs', 'Hits', 'Dingers', 'RBI', 'Stolen Bases', 'Batting Average', 'Wins', 'Complete Games', 'Saves', 'Strikeouts', 'ERA', 'WHIP']

    # Go through each stat in 'stats', print the stat name.
    # then go through the team_rank for each dictionary item.
    # Where team_rank is a dictionary of the format {'stat': [(team_id, stat_value), ... ]}
    # Hence the sort of convoluted dictionary call in the ranking line. Need to have j-1,
    # the range is 1-9, and the dictionary lists start at 0.

    for i in range(0,len(stats)):
        print stat_names[i] + '\n'
        for j in range(1,9):
            print str(j) + '.  [ ' + str(team_rank[stats[i]][j-1][1]) + ' ] ' + str(team_names[team_rank[stats[i]][j-1][0]])
        print '\n'


def format_report(team_names, team_record, week_id):
    ''' Sort the team records by winning percentage, then print the report to standard output. '''
    
    # Convert the dictionary into a list of tuples.
    team_list = []
    sorted_list = []
    for team_id in team_names.iterkeys():
        win = team_record[team_id][0]
        loss = team_record[team_id][1]
        tie = team_record[team_id][2]
        win_pct = (win+0.5*tie)/(win+loss+tie)
        team_list.append((team_names[team_id], win_pct, win, loss, tie))
    
    sorted_list = sorted(team_list, key=lambda win_pct: win_pct[1], reverse=True)
    
    if week_id < 0:
        week_id = 'Entire Season'
    else:
        week_id = 'Week ' + str(week_id)
    
    print 'What-If Scenarios for ' + week_id + '\n'
    
    for i in range(0,len(sorted_list)):
        print '{:^5}'.format(sorted_list[i][2]) + '-' + '{:^5}'.format(sorted_list[i][3]) + '-' + '{:^5}'.format(sorted_list[i][4]) + '  ' + '{:0.3f}'.format(sorted_list[i][1]) + '    ' + sorted_list[i][0]

    print '\n\n'

def week_report(week_start, week_end, team_record, team_names, stats, low_stats):
    ''' This generates the win/loss/tie records for the week for all the teams. Returns
    the team_record dictionary with win/loss/tie data. '''
    
    for i in range(week_start,week_end):
        
        week_results = query_db(i)
        
        # Generate the stat dictionary for each stat in the week in the function rank_stats.
        # Requires the results for the week, the initial stat definitions (columns) and which stats are
        # ranked in descending order. 
        
        stat_dict = rank_stats(week_results, stats, low_stats)
        #print 'Week ' + str(i) + ':\n'
        #print stat_dict

        # Go through the stats and determine if each was a win/loss/tie.
        # Check to see if it is in the 'low_stats' category, and store appropriately.
        
        for team_id in range(1,9):
            for key in stat_dict.iterkeys():
                if key in low_stats:
                    loss, win, tie = comp_team(stat_dict[key],team_id)
                else:
                    win, loss, tie = comp_team(stat_dict[key],team_id)
    
                team_record[team_id][0] += win
                team_record[team_id][1] += loss
                team_record[team_id][2] += tie
                
    return team_record


def rank_report(week_start, week_end, team_names, stats, low_stats):
    ''' This generates a ranking for the week for all the teams. Returns a sorted dictionary
    from the rank_stats function. '''

    # Build a query based on the start and end week.

    query_rank = 'select team_id, sum(r) as r, sum(h) as h, sum(hr) as hr, sum(rbi) as rbi, sum(sb) as sb, cast(sum(h)*1000/sum(ab)/1000.0 as numeric(4,3)) as avg, sum(w) as w, sum(cg) as cg, sum(sv) as sv, sum(k) as k, cast(sum(era*ip/9)/sum(ip)*9 as numeric(3,2)) as era, cast(sum(whip*ip)/sum(ip) as numeric(3,2)) as whip from baseball group by 1'

    con = pg.connect(dbname='baseball', host='localhost', user='chris')

    week_list = []
    week_results = con.query(query_rank)
    for i in range(0,len(week_results.getresult())):
         week_list.append((week_results.getresult()[i]))
        
    con.close()

    ranked_stats = rank_stats(week_list, stats, low_stats)

    return ranked_stats

def rank_stats(week_results,stats,low_stats):
    ''' This function accepts the stats, and low_stats to delineate between ERA/WHIP to sort properly.
    It returns a dictionary of stats with team rankings. Requires the results from the week. '''
    
    # Initialize an empty dictionary.
    stat_dict = {}
    
    for j in stats.iterkeys():
        i = 0
        temp_list = []
        stat_rank = []

        # Go through the weeks results, add the sorted values in a list of tuples. Sort in reverse for stats
        # that high scores are better.

        while i < len(week_results):
            if j in low_stats:
                temp_list.append(sorted(week_results, key=lambda rank: rank[stats[j]])[i])
            else:
                temp_list.append(sorted(week_results, key=lambda rank: rank[stats[j]], reverse=True)[i])
            i +=1

        # Stat_rank is a list of tuples, populated by the team_id and the relevant stat number.
        
        for k in range(0,len(temp_list)):
            stat_rank.append((temp_list[k][0], temp_list[k][stats[j]]))

        # Add the ranked stats to the stat dictionary.
        
        stat_dict[j] = stat_rank

    # Return a dictionary of the stats. Ranked best to worst.
    
    return stat_dict


def comp_team(stat, team):
    ''' This function ranks a team in a particular stat. Returns a tuple of wins, losses, ties. 
    Accepts a single argument, which is a list of tuples for the stat. It is already ordered by
    best to worst. Only need to compare against all values to determine success/failure. '''
    
    win = 0
    loss = 0
    tie = 0
    f = itemgetter(0)
    rank = map(f, stat).index(team)
    
    team_list = [stat[0], stat[1], stat[2], stat[3], stat[4], stat[5], stat[6], stat[7]]
    team_stat = team_list[rank][1]
    
    team_list.pop(rank)
    
    # Compare the team stat against the rest of the league.
    
    for i in range(0,len(team_list)):
    
        if team_stat==team_list[i][1]:
            tie +=1
        elif team_stat>team_list[i][1]:
            win +=1
        elif team_stat<team_list[i][1]:
            loss += 1
    
    return win, loss, tie

    
def query_db(week_id):
    ''' Queries the database for the week in question. Returns a dictionary. Format
    will be team_id as a key, and values as a list [r, h, hr, rbi, sb, ba, w, cg, sv, k, era, whip].
    This allows it to be referenced by team id and looping through all key values in
    other functions. '''
    
    con  = pg.connect(dbname='baseball', host='localhost', user='chris')
    query_week = 'select team_id, r, h, hr, rbi, sb, avg, w, cg, sv, k, era, whip from baseball where week_id=\'' + str(week_id) + '\' order by team_id'
    
    week_list = []
    week_results = con.query(query_week)
    for i in range(0,len(week_results.getresult())):
         week_list.append((week_results.getresult()[i]))
        
    con.close()
    return week_list
    
if __name__ == "__main__":
    main()
