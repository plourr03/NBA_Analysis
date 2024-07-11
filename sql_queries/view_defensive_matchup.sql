create or alter view matchupRunningStats as 
with base as (
SELECT bsm.[gameId]
	  ,lgl.GAME_DATE
      ,bsm.[teamId]
      ,bsm.[personIdOff]
	  ,bsm.[nameIOff]
      ,bsm.[personIdDef]
      ,bsm.[nameIDef]
      ,bsm.[matchupMinutes]
	  ,bsm.[playerPoints]/[matchupMinutes] as player_points_per_minute
      ,bsm.[partialPossessions]
      ,bsm.[percentageDefenderTotalTime]
      ,bsm.[percentageOffensiveTotalTime]
      ,bsm.[percentageTotalTimeBothOn]
      ,bsm.[switchesOn]
      ,bsm.[playerPoints]
      ,bsm.[teamPoints]
      ,bsm.[matchupFieldGoalsMade]
      ,bsm.[matchupFieldGoalsAttempted]
      ,bsm.[matchupFieldGoalsPercentage]
      ,bsm.[matchupThreePointersMade]
      ,bsm.[matchupThreePointersAttempted]
      ,bsm.[matchupThreePointersPercentage]

  FROM [nba_game_data].[dbo].[BoxScoreMatchupsV3] bsm

  left join [nba_game_data].[dbo].[LeagueGameLog] lgl 
  on 
  bsm.gameid = lgl.GAME_ID
  and bsm.teamid = lgl.TEAM_ID
  where 
  --personidoff = 1626157 -- this ID is Karls
   matchupMinutes>1.5
  and matchupFieldGoalsAttempted >1
  )
  , base2 as (
  select 
  	b.gameId
,	b.GAME_DATE
,	b.teamId
,	b.personIdOff
,	b.nameIOff
,	b.personIdDef
,	b.nameIDef

,	coalesce(avg(b.matchupMinutes) over (partition by b.personIdOff, b.personIdDef order by b.GAME_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ),avg(b.matchupMinutes) over (partition by b.personIdOff order by b.GAME_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING )) as avgMatchupMinutes
,	coalesce(avg(b.player_points_per_minute) over (partition by b.personIdOff, b.personIdDef order by b.GAME_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ),avg(b.player_points_per_minute) over (partition by b.personIdOff order by b.GAME_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING )) as running_ptsPerMint
,	coalesce(avg(b.partialPossessions) over (partition by b.personIdOff, b.personIdDef order by b.GAME_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ),avg(b.partialPossessions) over (partition by b.personIdOff order by b.GAME_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING )) as running_partialPossessions
,	coalesce(avg(b.playerPoints) over (partition by b.personIdOff, b.personIdDef order by b.GAME_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ),avg(b.playerPoints) over (partition by b.personIdOff order by b.GAME_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING )) as running_playerPoints
,	coalesce(avg(b.teamPoints) over (partition by b.personIdOff, b.personIdDef order by b.GAME_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ),avg(b.teamPoints) over (partition by b.personIdOff order by b.GAME_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING )) as running_teamPoints
,	coalesce(avg(b.matchupFieldGoalsMade) over (partition by b.personIdOff, b.personIdDef order by b.GAME_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ),avg(b.matchupFieldGoalsMade) over (partition by b.personIdOff order by b.GAME_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING )) as running_matchupFieldGoalsMade
,	coalesce(avg(b.matchupFieldGoalsAttempted) over (partition by b.personIdOff, b.personIdDef order by b.GAME_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ),avg(b.matchupFieldGoalsAttempted) over (partition by b.personIdOff order by b.GAME_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING )) as running_matchupFieldGoalsAttempted
,	coalesce(avg(b.matchupFieldGoalsPercentage) over (partition by b.personIdOff, b.personIdDef order by b.GAME_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ),avg(b.matchupFieldGoalsPercentage) over (partition by b.personIdOff order by b.GAME_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING )) as running_matchupFieldGoalsPercentage
,	coalesce(avg(b.matchupThreePointersMade) over (partition by b.personIdOff, b.personIdDef order by b.GAME_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ),avg(b.matchupThreePointersMade) over (partition by b.personIdOff order by b.GAME_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING )) as running_matchupThreePointersMade
,	coalesce(avg(b.matchupThreePointersAttempted) over (partition by b.personIdOff, b.personIdDef order by b.GAME_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ),avg(b.matchupThreePointersAttempted) over (partition by b.personIdOff order by b.GAME_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING )) as running_matchupThreePointersAttempted
,	coalesce(avg(b.matchupThreePointersPercentage) over (partition by b.personIdOff, b.personIdDef order by b.GAME_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ),avg(b.matchupThreePointersPercentage) over (partition by b.personIdOff order by b.GAME_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING )) as running_matchupThreePointersPercentage
,	coalesce(avg(b.percentageDefenderTotalTime) over (partition by b.personIdOff, b.personIdDef order by b.GAME_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ),avg(b.percentageDefenderTotalTime) over (partition by b.personIdOff order by b.GAME_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING )) as running_percentageDefenderTotalTime

,	pas_def_player.PIE
,	pas_def_player.offensiveRating
,	pas_def_player.defensiveRating
,	pas_def_player.netRating
,	pas_def_player.offensiveReboundPercentage
,	pas_def_player.defensiveReboundPercentage
,	pas_def_player.reboundPercentage
,	pas_def_player.turnoverRatio
,	pas_def_player.pace
,	pas_def_player.pacePer40
,	pas_def_player.possessions
,   b.playerPoints

,   coalesce(avg(b.matchupMinutes) over (partition by b.personIdOff, b.personIdDef order by b.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING ),avg(b.matchupMinutes) over (partition by b.personIdOff order by b.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING )) as avgMatchupMinutesLast2Games
,   coalesce(avg(b.player_points_per_minute) over (partition by b.personIdOff, b.personIdDef order by b.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING ),avg(b.player_points_per_minute) over (partition by b.personIdOff order by b.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING )) as running_ptsPerMintLast2Games
,   coalesce(avg(b.partialPossessions) over (partition by b.personIdOff, b.personIdDef order by b.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING ),avg(b.partialPossessions) over (partition by b.personIdOff order by b.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING )) as running_partialPossessionsLast2Games
,   coalesce(avg(b.playerPoints) over (partition by b.personIdOff, b.personIdDef order by b.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING ),avg(b.playerPoints) over (partition by b.personIdOff order by b.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING )) as running_playerPointsLast2Games
,   coalesce(avg(b.teamPoints) over (partition by b.personIdOff, b.personIdDef order by b.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING ),avg(b.teamPoints) over (partition by b.personIdOff order by b.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING )) as running_teamPointsLast2Games
,   coalesce(avg(b.matchupFieldGoalsMade) over (partition by b.personIdOff, b.personIdDef order by b.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING ),avg(b.matchupFieldGoalsMade) over (partition by b.personIdOff order by b.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING )) as running_matchupFieldGoalsMadeLast2Games
,   coalesce(avg(b.matchupFieldGoalsAttempted) over (partition by b.personIdOff, b.personIdDef order by b.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING ),avg(b.matchupFieldGoalsAttempted) over (partition by b.personIdOff order by b.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING )) as running_matchupFieldGoalsAttemptedLast2Games
,   coalesce(avg(b.matchupFieldGoalsPercentage) over (partition by b.personIdOff, b.personIdDef order by b.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING ),avg(b.matchupFieldGoalsPercentage) over (partition by b.personIdOff order by b.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING )) as running_matchupFieldGoalsPercentageLast2Games
,   coalesce(avg(b.matchupThreePointersMade) over (partition by b.personIdOff, b.personIdDef order by b.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING ),avg(b.matchupThreePointersMade) over (partition by b.personIdOff order by b.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING )) as running_matchupThreePointersMadeLast2Games
,   coalesce(avg(b.matchupThreePointersAttempted) over (partition by b.personIdOff, b.personIdDef order by b.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING ),avg(b.matchupThreePointersAttempted) over (partition by b.personIdOff order by b.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING )) as running_matchupThreePointersAttemptedLast2Games
,   coalesce(avg(b.matchupThreePointersPercentage) over (partition by b.personIdOff, b.personIdDef order by b.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING ),avg(b.matchupThreePointersPercentage) over (partition by b.personIdOff order by b.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING )) as running_matchupThreePointersPercentageLast2Games
,   coalesce(avg(b.percentageDefenderTotalTime) over (partition by b.personIdOff, b.personIdDef order by b.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING ),avg(b.percentageDefenderTotalTime) over (partition by b.personIdOff order by b.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING )) as running_percentageDefenderTotalTimeLast2Games



  from base b

  left outer join [nba_game_data].[dbo].[player_advanced_stats] pas_def_player
  on 
  b.personIdDef = pas_def_player.personid
  and b.gameId = pas_def_player.GAME_ID
  )
    SELECT 
        gameId,
		personIdOff as personId,
		--season stats
        AVG(running_ptsPerMint * avgMatchupMinutes) / AVG(avgMatchupMinutes) AS avg_running_ptsPerMint,
        AVG(running_partialPossessions * avgMatchupMinutes) / AVG(avgMatchupMinutes) AS avg_running_partialPossessions,
        AVG(running_playerPoints * avgMatchupMinutes) / AVG(avgMatchupMinutes) AS avg_running_playerPoints,
        AVG(running_teamPoints * avgMatchupMinutes) / AVG(avgMatchupMinutes) AS avg_running_teamPoints,
        AVG(running_matchupFieldGoalsMade * avgMatchupMinutes) / AVG(avgMatchupMinutes) AS avg_running_matchupFieldGoalsMade,
        AVG(running_matchupFieldGoalsAttempted * avgMatchupMinutes) / AVG(avgMatchupMinutes) AS avg_running_matchupFieldGoalsAttempted,
        AVG(running_matchupFieldGoalsPercentage * avgMatchupMinutes) / AVG(avgMatchupMinutes) AS avg_running_matchupFieldGoalsPercentage,
        AVG(running_matchupThreePointersMade * avgMatchupMinutes) / AVG(avgMatchupMinutes) AS avg_running_matchupThreePointersMade,
        AVG(running_matchupThreePointersAttempted * avgMatchupMinutes) / AVG(avgMatchupMinutes) AS avg_running_matchupThreePointersAttempted,
        AVG(running_matchupThreePointersPercentage * avgMatchupMinutes) / AVG(avgMatchupMinutes) AS avg_running_matchupThreePointersPercentage,
        AVG(running_percentageDefenderTotalTime * avgMatchupMinutes) / AVG(avgMatchupMinutes) AS avg_running_percentageDefenderTotalTime,
		--advanced stats 
        AVG(offensiveRating * avgMatchupMinutes) / AVG(avgMatchupMinutes) AS avg_offensiveRating,
        AVG(defensiveRating * avgMatchupMinutes) / AVG(avgMatchupMinutes) AS avg_defensiveRating,
        AVG(netRating * avgMatchupMinutes) / AVG(avgMatchupMinutes) AS avg_netRating,
        AVG(offensiveReboundPercentage * avgMatchupMinutes) / AVG(avgMatchupMinutes) AS avg_offensiveReboundPercentage,
        AVG(defensiveReboundPercentage * avgMatchupMinutes) / AVG(avgMatchupMinutes) AS avg_defensiveReboundPercentage,
        AVG(reboundPercentage * avgMatchupMinutes) / AVG(avgMatchupMinutes) AS avg_reboundPercentage,
        AVG(turnoverRatio * avgMatchupMinutes) / AVG(avgMatchupMinutes) AS avg_turnoverRatio,
        AVG(pace * avgMatchupMinutes) / AVG(avgMatchupMinutes) AS avg_pace,
        AVG(pacePer40 * avgMatchupMinutes) / AVG(avgMatchupMinutes) AS avg_pacePer40,
        AVG(possessions * avgMatchupMinutes) / AVG(avgMatchupMinutes) AS avg_possessions,
        AVG(playerPoints * avgMatchupMinutes) / AVG(avgMatchupMinutes) AS avg_playerPoints,
		--Recent stats
        AVG(running_ptsPerMintLast2Games * avgMatchupMinutesLast2Games) / AVG(avgMatchupMinutesLast2Games) AS avg_running_ptsPerMintLast2Games,
        AVG(running_partialPossessionsLast2Games * avgMatchupMinutesLast2Games) / AVG(avgMatchupMinutesLast2Games) AS avg_running_partialPossessionsLast2Games,
        AVG(running_playerPointsLast2Games * avgMatchupMinutesLast2Games) / AVG(avgMatchupMinutesLast2Games) AS avg_running_playerPointsLast2Games,
	    AVG(running_teamPointsLast2Games * avgMatchupMinutesLast2Games) / AVG(avgMatchupMinutesLast2Games) AS avg_running_teamPointsLast2Games,
	    AVG(running_matchupFieldGoalsMadeLast2Games * avgMatchupMinutesLast2Games) / AVG(avgMatchupMinutesLast2Games) AS avg_running_matchupFieldGoalsMadeLast2Games,
	    AVG(running_matchupFieldGoalsAttemptedLast2Games * avgMatchupMinutesLast2Games) / AVG(avgMatchupMinutesLast2Games) AS avg_running_matchupFieldGoalsAttemptedLast2Games,
	    AVG(running_matchupFieldGoalsPercentageLast2Games * avgMatchupMinutesLast2Games) / AVG(avgMatchupMinutesLast2Games) AS avg_running_matchupFieldGoalsPercentageLast2Games,
	    AVG(running_matchupThreePointersMadeLast2Games * avgMatchupMinutesLast2Games) / AVG(avgMatchupMinutesLast2Games) AS avg_running_matchupThreePointersMadeLast2Games,
	    AVG(running_matchupThreePointersAttemptedLast2Games * avgMatchupMinutesLast2Games) / AVG(avgMatchupMinutesLast2Games) AS avg_running_matchupThreePointersAttemptedLast2Games,
	    AVG(running_matchupThreePointersPercentageLast2Games * avgMatchupMinutesLast2Games) / AVG(avgMatchupMinutesLast2Games) AS avg_running_matchupThreePointersPercentageLast2Games,
	    AVG(running_percentageDefenderTotalTimeLast2Games * avgMatchupMinutesLast2Games) / AVG(avgMatchupMinutesLast2Games) AS avg_running_percentageDefenderTotalTimeLast2Games
    FROM base2
    GROUP BY gameId,personIdOff


--  ,base3 as (
--SELECT 
--    gameId,
--    GAME_DATE,
--    teamId,
--    personIdOff,
--    nameIOff,

--    SUM(running_percentageDefenderTotalTime * running_ptsPerMint) / SUM(running_percentageDefenderTotalTime) as weighted_running_ptsPerMint,
--    SUM(running_percentageDefenderTotalTime * running_partialPossessions) / SUM(running_percentageDefenderTotalTime) as weighted_running_partialPossessions,
--    SUM(running_playerPoints) as weighted_running_playerPoints,
--    SUM(running_percentageDefenderTotalTime * running_teamPoints) / SUM(running_percentageDefenderTotalTime) as weighted_running_teamPoints,
--    SUM(running_percentageDefenderTotalTime * running_matchupFieldGoalsMade) / SUM(running_percentageDefenderTotalTime) as weighted_running_matchupFieldGoalsMade,
--    SUM(running_percentageDefenderTotalTime * running_matchupFieldGoalsAttempted) / SUM(running_percentageDefenderTotalTime) as weighted_running_matchupFieldGoalsAttempted,
--    SUM(running_percentageDefenderTotalTime * running_matchupFieldGoalsPercentage) / SUM(running_percentageDefenderTotalTime) as weighted_running_matchupFieldGoalsPercentage,
--    SUM(running_percentageDefenderTotalTime * running_matchupThreePointersMade) / SUM(running_percentageDefenderTotalTime) as weighted_running_matchupThreePointersMade,
--    SUM(running_percentageDefenderTotalTime * running_matchupThreePointersAttempted) / SUM(running_percentageDefenderTotalTime) as weighted_running_matchupThreePointersAttempted,
--    SUM(running_percentageDefenderTotalTime * running_matchupThreePointersPercentage) / SUM(running_percentageDefenderTotalTime) as weighted_running_matchupThreePointersPercentage,
--    SUM(running_percentageDefenderTotalTime * PIE) / SUM(running_percentageDefenderTotalTime) as weighted_PIE,
--    SUM(running_percentageDefenderTotalTime * offensiveRating) / SUM(running_percentageDefenderTotalTime) as weighted_offensiveRating,
--    SUM(running_percentageDefenderTotalTime * defensiveRating) / SUM(running_percentageDefenderTotalTime) as weighted_defensiveRating,
--    SUM(running_percentageDefenderTotalTime * netRating) / SUM(running_percentageDefenderTotalTime) as weighted_netRating,
--    SUM(running_percentageDefenderTotalTime * offensiveReboundPercentage) / SUM(running_percentageDefenderTotalTime) as weighted_offensiveReboundPercentage,
--    SUM(running_percentageDefenderTotalTime * defensiveReboundPercentage) / SUM(running_percentageDefenderTotalTime) as weighted_defensiveReboundPercentage,
--    SUM(running_percentageDefenderTotalTime * reboundPercentage) / SUM(running_percentageDefenderTotalTime) as weighted_reboundPercentage,
--    SUM(running_percentageDefenderTotalTime * turnoverRatio) / SUM(running_percentageDefenderTotalTime) as weighted_turnoverRatio,
--    SUM(running_percentageDefenderTotalTime * pace) / SUM(running_percentageDefenderTotalTime) as weighted_pace,
--    SUM(running_percentageDefenderTotalTime * pacePer40) / SUM(running_percentageDefenderTotalTime) as weighted_pacePer40,
--    SUM(running_percentageDefenderTotalTime * possessions) / SUM(running_percentageDefenderTotalTime) as weighted_possessions,
--    SUM(running_percentageDefenderTotalTime) as total_percentageDefenderTotalTime
--FROM base2
--GROUP BY gameId, GAME_DATE, teamId, personIdOff, nameIOff


--) 
--SELECT 
--lgl.TEAM_NAME,
--lgl.oppAbrv,
--B.* 
--FROM base3 B

--LEFT OUTER JOIN [nba_game_data].[dbo].[LeagueGameLog] lgl 
--on 
--B.gameId = lgl.GAME_ID
--and B.teamId = lgl.TEAM_ID
--ORDER BY GAME_DATE;
