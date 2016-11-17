#! /bin/bash
week=1

while [ $week -lt 17 ]; do
	wget "http://www.thehuddle.com/stats/2014/plays_weekly.php?week=${week}&pos=DF&col=FPTS&ccs=5" -O week${week}_d.html
	let week=week+1
done

