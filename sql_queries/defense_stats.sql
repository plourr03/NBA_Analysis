/****** Script for SelectTopNRows command from SSMS  ******/
SELECT 
	   bsd.[gameId]
      ,bsd.[teamId]
      ,bsd.[teamCity]
      ,bsd.[teamName]
      ,bsd.[teamTricode]
      ,bsd.[teamSlug]
      ,COALESCE(CASE WHEN ctr.[POSITION] = 'G-F' THEN 'G' ELSE
				 CASE WHEN ctr.[POSITION] = 'F-C' THEN 'F' ELSE
				 CASE WHEN ctr.[POSITION] = 'F-G' THEN 'F' ELSE
				 CASE WHEN ctr.[POSITION] = 'C-F' THEN 'C' ELSE
				 ctr.[POSITION] END END END END ,ctr2.POSITION,'NF') as POSITION
      ,SUM(bsd.[matchupMinutes]) as [matchupMinutes]
      ,SUM(bsd.[partialPossessions]) as [partialPossessions]
      ,SUM(bsd.[playerPoints]) AS PointsAllowed
      ,sum(bsd.[defensiveRebounds]) AS numOfDrebThisPlayerHad
      ,sum(bsd.[matchupAssists]) as [matchupAssists]
      ,sum(bsd.[matchupTurnovers]) as [matchupTurnovers]
      ,sum(bsd.[steals]) as [steals]
      ,sum(bsd.[blocks]) as [blocks]
      ,sum(bsd.[matchupFieldGoalsMade]) as [matchupFieldGoalsMade]
      ,sum(bsd.[matchupFieldGoalsAttempted]) as [matchupFieldGoalsAttempted]
	  ,sum(bsd.[matchupFieldGoalsMade])/NULLIF(sum(bsd.[matchupFieldGoalsAttempted]),0) as [matchupFieldGoalPercentage]

      ,sum(bsd.[matchupThreePointersMade]) as [matchupThreePointersMade]
      ,sum(bsd.[matchupThreePointersAttempted]) as [matchupThreePointersAttempted]
	  ,sum(bsd.[matchupThreePointersMade])/nullif(sum(bsd.[matchupThreePointersAttempted]),0) as [matchupThreePointerPercentage]

  FROM [nba_game_data].[dbo].[BoxScoreDefensiveV2] bsd

  LEFT OUTER JOIN [nba_game_data].[dbo].[PlayerGameLogs]
ON 
bsd.personId = [PlayerGameLogs].PLAYER_ID
and bsd.gameId = [PlayerGameLogs].GAME_ID

  LEFT OUTER JOIN [nba_game_data].[dbo].[CommonTeamRoster] ctr
ON
cast(bsd.personId as int) = cast(ctr.PLAYER_ID as int)
AND cast(bsd.teamId as int) = cast(ctr.TeamID as int)
AND cast([PlayerGameLogs].yearSeason as int) = cast(ctr.SEASON as int)

LEFT OUTER JOIN [nba_game_data].[dbo].[BackupPlayerPosition] ctr2
ON 
cast(bsd.personId as int) = cast(ctr2.PLAYER_ID as int)


  where bsd.teamId ='1610612750'
  and yearSeason = 2024
 and gameId = '0022300411'

 GROUP BY 
 	   bsd.[gameId]
      ,bsd.[teamId]
      ,bsd.[teamCity]
      ,bsd.[teamName]
      ,bsd.[teamTricode]
      ,bsd.[teamSlug]
	  ,GAME_DATE
      ,COALESCE(CASE WHEN ctr.[POSITION] = 'G-F' THEN 'G' ELSE
				 CASE WHEN ctr.[POSITION] = 'F-C' THEN 'F' ELSE
				 CASE WHEN ctr.[POSITION] = 'F-G' THEN 'F' ELSE
				 CASE WHEN ctr.[POSITION] = 'C-F' THEN 'C' ELSE
				 ctr.[POSITION] END END END END ,ctr2.POSITION,'NF')

  order by GAME_DATE, COALESCE(CASE WHEN ctr.[POSITION] = 'G-F' THEN 'G' ELSE
				 CASE WHEN ctr.[POSITION] = 'F-C' THEN 'F' ELSE
				 CASE WHEN ctr.[POSITION] = 'F-G' THEN 'F' ELSE
				 CASE WHEN ctr.[POSITION] = 'C-F' THEN 'C' ELSE
				 ctr.[POSITION] END END END END ,ctr2.POSITION,'NF')