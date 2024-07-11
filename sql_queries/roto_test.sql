CREATE or alter PROCEDURE usp_GetPlayerGameStats
    @threshold INT,
    @player_id INT,
    @game_date VARCHAR(MAX),
	@line_Type VARCHAR(MAX)
AS
BEGIN
DECLARE @sql NVARCHAR(MAX);
DECLARE @params NVARCHAR(MAX);
    SET @sql = N'
with base as (
SELECT [PLAYER_ID]
,[PLAYER_NAME]
,[TEAM_ID]
,[TEAM_ABBREVIATION]
,[TEAM_NAME]
,pgl.[GAME_ID]
,[GAME_DATE]
,yearSeason
,case when ' + @line_Type + N' > ' + CAST(@threshold AS NVARCHAR) + N' then 1 else 0 end as overUnder
,pgl.oppAbrv
,case when lag(pgl.[TEAM_ID]) over (PARTITION BY pgl.PLAYER_ID ORDER BY pgl.GAME_DATE) != pgl.[TEAM_ID] then 1 else 0 end as traded
,round(avg(case when ' + @line_Type + N'>' + CAST(@threshold AS NVARCHAR) + N' then 1.0 else 0.0 end) over (PARTITION BY pgl.PLAYER_ID ORDER BY pgl.GAME_DATE ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING),2) as hitRateLast5
,round(avg(case when ' + @line_Type + N'>' + CAST(@threshold AS NVARCHAR) + N' then 1.0 else 0.0 end) over (PARTITION BY pgl.PLAYER_ID ORDER BY pgl.GAME_DATE ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING),2) as hitRateLast10
,round(avg(case when ' + @line_Type + N'>' + CAST(@threshold AS NVARCHAR) + N' then 1.0 else 0.0 end) over (PARTITION BY pgl.PLAYER_ID ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),2) as hitRateLast20
,round(avg(case when ' + @line_Type + N'>' + CAST(@threshold AS NVARCHAR) + N' then 1.0 else 0.0 end) over (PARTITION BY pgl.PLAYER_ID ORDER BY pgl.GAME_DATE ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING),2) as hitRateSeason
,coalesce(round(avg(case when ' + @line_Type + '>' + CAST(@threshold AS NVARCHAR) + N' then 1.0 else 0.0 end) over (PARTITION BY pgl.PLAYER_ID, pgl.oppAbrv ORDER BY pgl.GAME_DATE ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING),2),-1.0) as hitRateVsOpp
,case when coalesce(round(avg(cast(' + @line_Type + N' as float)) over (PARTITION BY pgl.PLAYER_ID, pgl.oppAbrv ORDER BY pgl.GAME_DATE ROWS BETWEEN unbounded PRECEDING AND 1 PRECEDING),2),-1) 
< coalesce(round(avg(cast(' + @line_Type + ' as float)) over (PARTITION BY pgl.PLAYER_ID, pgl.oppAbrv, pgl.yearSeason ORDER BY pgl.GAME_DATE ROWS BETWEEN unbounded PRECEDING AND 1 PRECEDING),2),999) then 0 else 1 end as OpponentPerformanceComparisonMAFlag
,case when round(avg(bsa.PIE) over (PARTITION BY pgl.PLAYER_ID ORDER BY pgl.GAME_DATE ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING),2)>
round(avg(bsa.PIE) over (PARTITION BY pgl.PLAYER_ID ORDER BY pgl.GAME_DATE ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING),2) then 1 else 0 end as playerImpactComparisonMAFlag
FROM [nba_game_data].[dbo].[PlayerGameLogs] pgl
left outer join [nba_game_data].[dbo].[BoxScoreAdvancedV3] bsa
on 
pgl.GAME_ID=bsa.GAME_ID
and pgl.PLAYER_ID=bsa.personId
left outer join [nba_game_data].[dbo].[TeamBoxScoreAdvancedV3] tbsa
on 
pgl.GAME_ID=tbsa.gameId
and pgl.oppAbrv=tbsa.teamTricode
where  
PLAYER_ID=' + CAST(@player_id AS NVARCHAR) + N' 
and  yearSeason in (2024,2023)
)
,base2 as(
select PLAYER_ID
,PLAYER_NAME
,base.TEAM_ABBREVIATION
,oppAbrv
,base.GAME_ID
,base.GAME_DATE
,overUnder
,hitRateLast5
,hitRateLast10
,hitRateLast20
,hitRateSeason
,hitRateVsOpp
,hitRateLast5 + hitRateLast10 + hitRateLast20 + hitRateSeason + hitRateVsOpp as hitRateSum
,case when hitRateLast5>.5 then 1 else 0 end + case when hitRateLast10>.5 then 1 else 0 end + case when hitRateLast20>.5 then 1 else 0 end + case when hitRateSeason>.5 then 1 else 0 end + case when hitRateVsOpp>.5 then 1 else 0 end as totalHitRateOverFifty
, case when hitRateLast5>.5 and hitRateLast10>.5 and hitRateLast20>.5 and hitRateSeason>.5 and hitRateVsOpp>.5 then 1 else 0 end as hitRateEdgeFactor
,sum(traded) over (PARTITION BY PLAYER_ID, yearSeason ORDER BY base.GAME_DATE) as traded
,OpponentPerformanceComparisonMAFlag
,playerImpactComparisonMAFlag 
,tdr.daily_rank as OppDefensiveRanking
,[cumulativeDR] as OppDefensiveRating
,case when tdr.daily_rank<=7 then 1 else 0 end as againstTopQuortileDefense
,case when tdr.daily_rank<=15 then 1 else 0 end as againstTopHalfDefense
from base 
left outer join [nba_game_data].[dbo].[teamAdvancedStats] tas
on 
base.GAME_ID=tas.GAME_ID
and base.TEAM_ID=tas.[teamId]
left outer join [nba_game_data].[dbo].TeamAbrvMap tam
on 
trim(base.oppAbrv)=trim(tam.TEAM_ABBREVIATION)
left outer join [nba_game_data].[dbo].[teamDailyRankings] tdr
on 
cast(base.GAME_DATE as date)=cast(tdr.GAME_DATE as date)
and cast(tam.[TEAM_ID] as int)= cast(tdr.teamid as int)
where yearSeason=2024
)
select * 
from base2
where GAME_DATE=''' + CAST(@game_date AS NVARCHAR) + N'''; ';

 SET @params = N'@player_id INT, @game_date VARCHAR(MAX)';
 
EXEC sp_executesql @sql, @params, @player_id = @player_id, @game_date = @game_date;

END;