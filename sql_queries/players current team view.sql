/****** Script for SelectTopNRows command from SSMS  ******/
create or alter view PlayersCurrentTeam as 
with base as (
SELECT
      PLAYER_ID
      ,[TEAM_ABBREVIATION]
      ,rank() over (partition by [PLAYER_ID], [SEASON_YEAR] order by GAME_DATE desc) as ranking 
  FROM [nba_game_data].[dbo].[PlayerGameLogs]
  where yearSeason = 2024
  )
  select * from base where ranking  = 1