/****** Script for SelectTopNRows command from SSMS  ******/
create or alter view teamAdvancedStats as 
SELECT [teamId]
      ,[teamName]
      ,[teamTricode]
	  ,GAME_ID
	  
      ,avg([estimatedOffensiveRating]) over (partition by teamName, lgl.yearSeason order by lgl.GAME_DATE, lgl.yearSeason ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING) as estimatedOffensiveRating
      ,avg([offensiveRating]) over (partition by teamName, lgl.yearSeason order by lgl.GAME_DATE, lgl.yearSeason ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING) as offensiveRating
      ,avg([estimatedDefensiveRating]) over (partition by teamName, lgl.yearSeason order by lgl.GAME_DATE, lgl.yearSeason ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING) as estimatedDefensiveRating
      ,avg([defensiveRating]) over (partition by teamName, lgl.yearSeason order by lgl.GAME_DATE, lgl.yearSeason ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING) as defensiveRating
      ,avg([estimatedNetRating]) over (partition by teamName, lgl.yearSeason order by lgl.GAME_DATE, lgl.yearSeason ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING) as estimatedNetRating
      ,avg([netRating]) over (partition by teamName, lgl.yearSeason order by lgl.GAME_DATE, lgl.yearSeason ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING) as netRating
      ,avg([assistPercentage]) over (partition by teamName, lgl.yearSeason order by lgl.GAME_DATE, lgl.yearSeason ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING) as assistPercentage
      ,avg([assistToTurnover]) over (partition by teamName, lgl.yearSeason order by lgl.GAME_DATE, lgl.yearSeason ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING) as assistToTurnover
      ,avg([assistRatio]) over (partition by teamName, lgl.yearSeason order by lgl.GAME_DATE, lgl.yearSeason ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING) as assistRatio
      ,avg([offensiveReboundPercentage]) over (partition by teamName, lgl.yearSeason order by lgl.GAME_DATE, lgl.yearSeason ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING) as offensiveReboundPercentage
      ,avg([defensiveReboundPercentage]) over (partition by teamName, lgl.yearSeason order by lgl.GAME_DATE, lgl.yearSeason ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING) as defensiveReboundPercentage
      ,avg([reboundPercentage]) over (partition by teamName, lgl.yearSeason order by lgl.GAME_DATE, lgl.yearSeason ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING) as reboundPercentage
      ,avg([estimatedTeamTurnoverPercentage]) over (partition by teamName, lgl.yearSeason order by lgl.GAME_DATE, lgl.yearSeason ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING) as estimatedTeamTurnoverPercentage
      ,avg([turnoverRatio]) over (partition by teamName, lgl.yearSeason order by lgl.GAME_DATE, lgl.yearSeason ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING) as turnoverRatio
      ,avg([effectiveFieldGoalPercentage]) over (partition by teamName, lgl.yearSeason order by lgl.GAME_DATE, lgl.yearSeason ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING) as effectiveFieldGoalPercentage
      ,avg([trueShootingPercentage]) over (partition by teamName, lgl.yearSeason order by lgl.GAME_DATE, lgl.yearSeason ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING) as trueShootingPercentage
      ,avg([usagePercentage]) over (partition by teamName, lgl.yearSeason order by lgl.GAME_DATE, lgl.yearSeason ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING) as usagePercentage
      ,avg([estimatedUsagePercentage]) over (partition by teamName, lgl.yearSeason order by lgl.GAME_DATE, lgl.yearSeason ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING) as estimatedUsagePercentage
      ,avg([estimatedPace]) over (partition by teamName, lgl.yearSeason order by lgl.GAME_DATE, lgl.yearSeason ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING) as estimatedPace
      ,avg([pace]) over (partition by teamName, lgl.yearSeason order by lgl.GAME_DATE, lgl.yearSeason ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING) as pace
      ,avg([pacePer40]) over (partition by teamName, lgl.yearSeason order by lgl.GAME_DATE, lgl.yearSeason ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING) as pacePer40
      ,avg([possessions]) over (partition by teamName, lgl.yearSeason order by lgl.GAME_DATE, lgl.yearSeason ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING) as possessions
      ,avg([PIE]) over (partition by teamName, lgl.yearSeason order by lgl.GAME_DATE, lgl.yearSeason ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING) as PIE
  
  FROM [nba_game_data].[dbo].[TeamBoxScoreAdvancedV3] tbsa

  left outer join [nba_game_data].[dbo].[LeagueGameLog] lgl
  on 
  tbsa.teamId = lgl.TEAM_ID
  and tbsa.gameId = lgl.GAME_ID
