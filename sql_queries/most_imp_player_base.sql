/****** Script for SelectTopNRows command from SSMS  ******/
with minutesPerGame as (
SELECT [SEASON_YEAR]
      ,[PLAYER_ID]
	  ,avg(MIN) avg_min

	  --,PTS
  FROM [nba_game_data].[dbo].[PlayerGameLogs]
  where yearSeason =2024
  group by [SEASON_YEAR]
      ,[PLAYER_ID]
)
SELECT pgl.[SEASON_YEAR]
      ,pgl.[PLAYER_ID]
      ,pgl.[PLAYER_NAME]
      ,pgl.[NICKNAME]
      ,pgl.[TEAM_ID]
      ,pgl.[TEAM_ABBREVIATION]
      ,pgl.[TEAM_NAME]
      ,pgl.[GAME_ID]
      ,pgl.[GAME_DATE]

	  ,mpg.avg_min

	  ,pgl.PTS
  FROM [nba_game_data].[dbo].[PlayerGameLogs] pgl
  left outer join minutesPerGame mpg
  on 
  pgl.[PLAYER_ID] = mpg.[PLAYER_ID]
  and pgl.[SEASON_YEAR] = mpg.[SEASON_YEAR]

  where yearSeason =2024
and mpg.avg_min > 11

  and pgl.PLAYER_ID = 1629639
