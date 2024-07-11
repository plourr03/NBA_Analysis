
CREATE OR ALTER PROCEDURE seasonPtsDataSet
    @pts_thresh FLOAT,
    @player_id INT,
	@line_type varchar(max)
	
AS
BEGIN
    DECLARE @sql NVARCHAR(MAX);

    SET 
    @sql = N'
with base as (
SELECT 
       pgl.[SEASON_YEAR]
      ,pgl.[PLAYER_ID]
      ,pgl.[PLAYER_NAME]
      ,pgl.[TEAM_ID]
      ,pgl.[TEAM_ABBREVIATION]
	  ,pgl.oppAbrv
      ,pgl.[TEAM_NAME]
      ,pgl.[GAME_ID]
	  ,pgl.yearSeason
   
      ,pgl.PTS
	  ,pgl.[GAME_DATE]
	  ,ifp.[PTS_TEAMMATE_ID]
	  ,dpm.DID_PLAYER_MISS_GAME as didTeammatePlay

	  ,DATEDIFF(day,lag(pgl.GAME_DATE) over(partition by pgl.PLAYER_ID ORDER BY pgl.game_date),pgl.[GAME_DATE])-1 as daysOfRest
	  ,case when DATEDIFF(day,lag(pgl.GAME_DATE) over(partition by pgl.PLAYER_ID ORDER BY pgl.game_date),pgl.[GAME_DATE])-1 = 0 then 1 else 0 end as secondHalfOfBackToBack


	  ,case when ' + @line_type + N' > @ptsThreshParam then 1 else 0 end as lineThresh


	  ,lag(' + @line_type + N' ) over(partition by pgl.PLAYER_ID, pgl.oppAbrv ORDER BY pgl.game_date) as PlayerGotLastMeeting

	  ,ROUND(SUM(case when ' + @line_type + N'  > @ptsThreshParam then 1 else 0 end) OVER (PARTITION BY pgl.PLAYER_ID,oppAbrv ORDER BY pgl.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),2) NumberTimesHitAgaisntOppLast2
	  ,ROUND(SUM(case when ' + @line_type + N'  > @ptsThreshParam then 1 else 0 end) OVER (PARTITION BY pgl.PLAYER_ID,oppAbrv ORDER BY pgl.GAME_DATE ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING),2) NumberTimesHitAgaisntOppLast5

	  ,ROUND(AVG(CAST(' + @line_type + N'  AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID,oppAbrv ORDER BY pgl.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),2) PerGameAgainstOppLast2
	  ,ROUND(AVG(CAST(' + @line_type + N'  AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID,oppAbrv ORDER BY pgl.GAME_DATE ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING),2) PerGameAgainstOppLast5

	  ,lag(' + @line_type + N' ) OVER(PARTITION BY pgl.PLAYER_ID ORDER BY pgl.game_date) AS PlayerGotLastGame

	  ,ROUND(SUM(CAST(case when ' + @line_type + N'  > @ptsThreshParam then 1 else 0 end AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING),2) 
	  /NULLIF(ROUND(count(CAST(case when ' + @line_type + N'  > @ptsThreshParam then 1 else 0 end AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING),2),0) as PctTimesHitLast10
	  ,ROUND(SUM(CAST(case when ' + @line_type + N'  > @ptsThreshParam then 1 else 0 end AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING),2) 
	  /NULLIF(ROUND(count(CAST(case when ' + @line_type + N'  > @ptsThreshParam then 1 else 0 end AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING),2),0) as PctTimesHitLast5

      ,ROUND(AVG(CAST(' +  @line_type + N'  AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),2) AS lineLast2
	  ,ROUND(AVG(CAST(' +  @line_type + N'  AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING),2) AS lineLast8
	  ,ROUND(AVG(CAST(' +  @line_type + N'  AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),2) AS lineLast20
	  ,ROUND(AVG(CAST(' +  @line_type + N'  AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 82 PRECEDING AND 1 PRECEDING),2) AS linePerGameSeason
	  ,CASE WHEN 		   
		ROUND(AVG(CAST(' + @line_type + N'  AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING),2) > 
		ROUND(AVG(CAST(' + @line_type + N'  AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),2) 
	  THEN 1 else 0 end as lineLongTrend


	  ,ROUND(AVG(CAST(pgl.MIN AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),2) AS MinutesPerGameLast2
	  ,ROUND(AVG(CAST(pgl.MIN AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING),2) AS MinutesPerGameLast8
	  ,ROUND(AVG(CAST(pgl.MIN AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),2) AS MinutesPerGameLast20
	   
	  ,ROUND(SUM(CAST(pgl.FGM AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)/
	  NULLIF(SUM(CAST(pgl.FGA AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),0),2) AS FieldGoalPctLast2
	  ,ROUND(SUM(CAST(pgl.FGM AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING)/
	  NULLIF(SUM(CAST(pgl.FGA AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING),0),2) AS FieldGoalPctLast8
	  ,ROUND(SUM(CAST(pgl.FGM AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING)/
	  NULLIF(SUM(CAST(pgl.FGA AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),0),2) AS FieldGoalPctLast20

      ,ROUND(SUM(CAST(pgl.FG3M AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)/
	  NULLIF(SUM(CAST(pgl.FG3A AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),0),2) AS FieldGoalThreePctLast2
	  ,ROUND(SUM(CAST(pgl.FG3M AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING)/
	  NULLIF(SUM(CAST(pgl.FG3A AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING),0),2) AS FieldGoalThreePctLast8
	  ,ROUND(SUM(CAST(pgl.FG3M AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING)/
	  NULLIF(SUM(CAST(pgl.FG3A AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),0),2) AS FieldGoalThreePctLast20
	  ,CASE WHEN 
		ROUND(SUM(CAST(pgl.FG3M AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)/
	  NULLIF(SUM(CAST(pgl.FG3A AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING),0),2) > 
		ROUND(SUM(CAST(pgl.FG3M AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING)/
	  NULLIF(SUM(CAST(pgl.FG3A AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),0),2) 
	  THEN 1 else 0 end as FieldGoalThreePctTrend
      
	  ,ROUND(AVG(CAST(pgl.PF AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING),2) AS PersonalFoulsLast8
	  ,ROUND(AVG(CAST(pgl.PFD AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING),2) AS PersonalFoulsDrawnLast8
	  ,CASE WHEN 
		ROUND(AVG(CAST(pgl.PFD AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING),2) > 
		ROUND(AVG(CAST(pgl.PFD AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),2) 
	  THEN 1 else 0 end as PersonalFoulsDrawnTrend

	  ,ROUND(AVG(CAST(pgl.PLUS_MINUS AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID ORDER BY pgl.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),2) AS PlusMinusLast2
	  ,ROUND(AVG(CAST(pgl.PLUS_MINUS AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING),2) AS PlusMinusLast8
	  ,ROUND(AVG(CAST(pgl.PLUS_MINUS AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),2) AS PlusMinusLast20

	  ,ROUND(AVG(CAST(pgl.NBA_FANTASY_PTS AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID ORDER BY pgl.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),2) AS FantPointsLast2
	  ,ROUND(AVG(CAST(pgl.NBA_FANTASY_PTS AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING),2) AS FantPointsLast8
	  ,ROUND(AVG(CAST(pgl.NBA_FANTASY_PTS AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),2) AS FantPointsLast20
	  ,CASE WHEN 
		ROUND(AVG(CAST(pgl.NBA_FANTASY_PTS AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING),2) > 
		ROUND(AVG(CAST(pgl.NBA_FANTASY_PTS AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),2) 
	  THEN 1 else 0 end as FantPointsDrawnTrend

	  ,ROUND(SUM(CAST(pgl.DD2 AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID ORDER BY pgl.GAME_DATE ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING),2) AS NumberOfDoubleDoublesLast10
	  ,ROUND(SUM(CAST(pgl.TD3 AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID ORDER BY pgl.GAME_DATE ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING),2) AS NumberOfTripleDoublesLast10

	  ,RANK() OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ) AS GameNumberThisSeason

	  ,ROUND(AVG(CAST(bsa.offensiveRating AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),2) AS OffensiveRatingLast2
	  ,ROUND(AVG(CAST(bsa.offensiveRating AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING),2) AS OffensiveRatingLast8
	  ,ROUND(AVG(CAST(bsa.offensiveRating AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),2) AS OffensiveRatingLast20
	   
	  ,ROUND(AVG(CAST(bsa.defensiveRating AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),2) AS DefensiveRatingLast2
	  ,ROUND(AVG(CAST(bsa.defensiveRating AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING),2) AS DefensiveRatingLast8
	  ,ROUND(AVG(CAST(bsa.defensiveRating AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),2) AS DefensiveRatingLast20
	  
	  ,ROUND(AVG(CAST(bsa.netRating AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING),2) AS NetRatingLast8
	  ,ROUND(AVG(CAST(bsa.netRating AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),2) AS NetRatingLast20
	  
	  ,ROUND(AVG(CAST(bsa.effectiveFieldGoalPercentage AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),2) AS EffectiveFGPctLast2
	  ,ROUND(AVG(CAST(bsa.effectiveFieldGoalPercentage AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING),2) AS EffectiveFGPctLast8
	  ,ROUND(AVG(CAST(bsa.effectiveFieldGoalPercentage AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),2) AS EffectiveFGPctLast20

	  
      ,ROUND(AVG(CAST(bsa.trueShootingPercentage AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),2) AS TSPctLast2
	  ,ROUND(AVG(CAST(bsa.trueShootingPercentage AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING),2) AS TSPctLast8
	  ,ROUND(AVG(CAST(bsa.trueShootingPercentage AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),2) AS TSPctLast20

	  
      ,ROUND(AVG(CAST(bsa.usagePercentage AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),2) AS UsagePctLast2
	  ,ROUND(AVG(CAST(bsa.usagePercentage AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING),2) AS UsagePctLast8
	  ,ROUND(AVG(CAST(bsa.usagePercentage AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),2) AS UsagePctLast20

	  ,ROUND(AVG(CAST(bsa.possessions AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),2) AS PossessionsLast2
	  ,ROUND(AVG(CAST(bsa.possessions AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING),2) AS PossessionsLast8
	  ,ROUND(AVG(CAST(bsa.possessions AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),2) AS PossessionsLast20
	  ,CASE WHEN 
		ROUND(AVG(CAST(bsa.possessions AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING),2) > 
		ROUND(AVG(CAST(bsa.possessions AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),2) 
	  THEN 1 else 0 end as PossessionsTrend
	  
	  ,ROUND(AVG(CAST(bsa.PIE AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),2) AS PIELast2
	  ,ROUND(AVG(CAST(bsa.PIE AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING),2) AS PIELast8
	  ,ROUND(AVG(CAST(bsa.PIE AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),2) AS PIELast20

	  
	  	  ,ROUND(AVG(CAST(bsm.pointsOffTurnovers AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),2) AS PTSOffTOLast2
	  ,ROUND(AVG(CAST(bsm.pointsOffTurnovers AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING),2) AS PTSOffTOLast8
	  ,ROUND(AVG(CAST(bsm.pointsOffTurnovers AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),2) AS PTSOffTOLast20
	  ,CASE WHEN 
		ROUND(AVG(CAST(bsm.pointsOffTurnovers AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING),2) > 
		ROUND(AVG(CAST(bsm.pointsOffTurnovers AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),2) 
	  THEN 1 else 0 end as PTSOffTOTrend

	  ,ROUND(AVG(CAST(bsm.pointsSecondChance AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),2) AS SecondChancePtsLast2
	  ,ROUND(AVG(CAST(bsm.pointsSecondChance AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING),2) AS SecondChancePtsLast8
	  ,ROUND(AVG(CAST(bsm.pointsSecondChance AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),2) AS SecondChancePtsLast20

	  ,ROUND(AVG(CAST(bsm.pointsFastBreak AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),2) AS FastBreakPtsLast2
	  ,ROUND(AVG(CAST(bsm.pointsFastBreak AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING),2) AS FastBreakPtsLast8
	  ,ROUND(AVG(CAST(bsm.pointsFastBreak AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),2) AS FastBreakPtsLast20

	  ,ROUND(AVG(CAST(bsm.pointsPaint AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),2) AS PaintPtsLast2
	  ,ROUND(AVG(CAST(bsm.pointsPaint AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING),2) AS PaintPtsLast8
	  ,ROUND(AVG(CAST(bsm.pointsPaint AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),2) AS PaintPtsLast20
	  ,CASE WHEN 
		ROUND(AVG(CAST(bsm.pointsPaint AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING),2) > 
		ROUND(AVG(CAST(bsm.pointsPaint AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),2) 
	  THEN 1 else 0 end as PaintPtsTrend


	  ,ROUND(SUM(CAST(bst.contestedFieldGoalsMade AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING)/
	  NULLIF(SUM(CAST(bst.contestedFieldGoalsAttempted AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING),0),2) AS ContestedFGPctLast8
	  ,ROUND(SUM(CAST(bst.contestedFieldGoalsMade AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING)/
	  NULLIF(SUM(CAST(bst.contestedFieldGoalsAttempted AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),0),2) AS ContestedFGPctLast20

	  ,ROUND(SUM(CAST(bst.uncontestedFieldGoalsMade AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)/
	  NULLIF(SUM(CAST(bst.uncontestedFieldGoalsAttempted AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),0),2) AS UncontestedFGPctLast2
	  ,ROUND(SUM(CAST(bst.uncontestedFieldGoalsMade AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING)/
	  NULLIF(SUM(CAST(bst.uncontestedFieldGoalsAttempted AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING),0),2) AS UncontestedFGPctLast8
	  ,ROUND(SUM(CAST(bst.uncontestedFieldGoalsMade AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING)/
	  NULLIF(SUM(CAST(bst.uncontestedFieldGoalsAttempted AS FLOAT)) OVER (PARTITION BY pgl.PLAYER_ID, pgl.SEASON_YEAR ORDER BY pgl.GAME_DATE ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),0),2) AS UncontestedFGPctLast20



  FROM [nba_game_data].[dbo].[PlayerGameLogs] pgl

  LEFT OUTER JOIN [nba_game_data].[dbo].[BoxScoreAdvancedV3] bsa
  ON
  cast(pgl.PLAYER_ID as int) = cast(bsa.personId as int)
  AND pgl.GAME_ID = bsa.GAME_ID

   LEFT OUTER JOIN [nba_game_data].[dbo].BoxScoreMiscV3 bsm
   ON
   cast(pgl.PLAYER_ID as int) = cast(bsm.personId as int)
   AND pgl.GAME_ID = bsm.gameId

   LEFT OUTER JOIN [nba_game_data].[dbo].[BoxScorePlayerTrackV3] bst
   ON
   cast(pgl.PLAYER_ID as int) = cast(bst.personId as int)
   AND pgl.GAME_ID = bst.gameId

   LEFT OUTER JOIN [nba_game_data].[dbo].[InfluentialPlayersV1] ifp
   ON
   cast(pgl.PLAYER_ID as int) = cast(ifp.[PLAYER_ID] as int)

   LEFT OUTER JOIN [nba_game_data].[dbo].[GamesPlayerMissedV1] dpm
   ON
   cast(ifp.[PTS_TEAMMATE_ID] as int) = cast(dpm.[PLAYER_ID] as int)
   AND pgl.GAME_ID = dpm.GAME_ID

  WHERE  pgl.PLAYER_ID = @playerIdParam


  )
  select 
  b.PLAYER_ID,
  b.GAME_DATE,
  b.lineThresh,
 -- b.didTeammatePlay,
  b.daysOfRest,
  b.secondHalfOfBackToBack,
  b.PlayerGotLastMeeting,
  b.NumberTimesHitAgaisntOppLast2,
  b.NumberTimesHitAgaisntOppLast5,
  b.PerGameAgainstOppLast2,
  b.PerGameAgainstOppLast5,
  b.PlayerGotLastGame,
  b.PctTimesHitLast10,
  b.PctTimesHitLast5,
  b.lineLast2,
  b.lineLast8,
  b.lineLast20,
  b.linePerGameSeason,
  b.lineLongTrend,
  b.MinutesPerGameLast2,
      b.MinutesPerGameLast8,
      b.MinutesPerGameLast20,
      b.FieldGoalPctLast2,
      b.FieldGoalPctLast8,
      b.FieldGoalPctLast20,
      b.FieldGoalThreePctLast2,
      b.FieldGoalThreePctLast8,
      b.FieldGoalThreePctLast20,
      b.FieldGoalThreePctTrend,
      b.PersonalFoulsLast8,
      b.PersonalFoulsDrawnLast8,
      b.PersonalFoulsDrawnTrend,
      b.PlusMinusLast2,
      b.PlusMinusLast8,
      b.PlusMinusLast20,
      b.FantPointsLast2,
      b.FantPointsLast8,
      b.FantPointsLast20,
      b.FantPointsDrawnTrend,
      b.NumberOfDoubleDoublesLast10,
      b.NumberOfTripleDoublesLast10,
      b.GameNumberThisSeason,
      b.OffensiveRatingLast2,
      b.OffensiveRatingLast8,
      b.OffensiveRatingLast20,
      b.DefensiveRatingLast2,
      b.DefensiveRatingLast8,
      b.DefensiveRatingLast20,
      b.NetRatingLast8,
      b.NetRatingLast20,
      b.EffectiveFGPctLast2,
      b.EffectiveFGPctLast8,
      b.EffectiveFGPctLast20,
      b.TSPctLast2,
      b.TSPctLast8,
      b.TSPctLast20,
      b.UsagePctLast2,
      b.UsagePctLast8,
      b.UsagePctLast20,
      b.PossessionsLast2,
      b.PossessionsLast8,
      b.PossessionsLast20,
      b.PossessionsTrend,
      b.PIELast2,
      b.PIELast8,
      b.PIELast20,
      b.PTSOffTOLast2,
      b.PTSOffTOLast8,
      b.PTSOffTOLast20,
      b.PTSOffTOTrend,
      b.SecondChancePtsLast2,
      b.SecondChancePtsLast8,
      b.SecondChancePtsLast20,
      b.FastBreakPtsLast2,
      b.FastBreakPtsLast8,
      b.FastBreakPtsLast20,
      b.PaintPtsLast2,
      b.PaintPtsLast8,
      b.PaintPtsLast20,
      b.PaintPtsTrend,
      b.ContestedFGPctLast8,
      b.ContestedFGPctLast20,
      b.UncontestedFGPctLast2,
      b.UncontestedFGPctLast8,
      b.UncontestedFGPctLast20,
       ts_allow.[TeamORatingLast2]
      ,ts_allow.[TeamORatingLast10]
      ,ts_allow.[TeamORatingSeason]
      ,ts_allow.[TeamDRatingLast2]
      ,ts_allow.[TeamDRatingLast10]
      ,ts_allow.[TeamDRatingSeason]
      ,ts_allow.[TeamNetRatingLast2]
      ,ts_allow.[TeamNetRatingLast10]
      ,ts_allow.[TeamNetRatingSeason]
      ,ts_allow.[TeamPIELast2]
      ,ts_allow.[TeamPIELast10]
      ,ts_allow.[TeamPIESeason]
      ,ts_allow.[avgPtsAllowedPerGameLast2]
      ,ts_allow.[avgPtsAllowedPerGameLast10]
      ,ts_allow.[avgPtsAllowedPerGameSeason]
      ,ts_allow.[FieldGoalPctAllowedLast2]
      ,ts_allow.[FieldGoalPctAllowedLast10]
      ,ts_allow.[FieldGoalPctAllowedSeason]
      ,ts_allow.[FieldGoal3PctAllowedLast2]
      ,ts_allow.[FieldGoal3PctAllowedLast10]
      ,ts_allow.[FieldGoal3PctAllowedSeason]
      ,ts_allow.[FreeThrowPctAllowedLast2]
      ,ts_allow.[FreeThrowPctAllowedLast10]
      ,ts_allow.[FreeThrowPctAllowedSeason]
      ,ts_allow.[RebsAllowedLast2]
      ,ts_allow.[RebsAllowedLast10]
      ,ts_allow.[RebsAllowedSeason]
      ,ts_allow.[ORebsAllowedLast2]
      ,ts_allow.[ORebsAllowedLast10]
      ,ts_allow.[ORebsAllowedSeason]
      ,ts_allow.[DRebsAllowedLast2]
      ,ts_allow.[DRebsAllowedLast10]
      ,ts_allow.[DRebsAllowedSeason]
      ,ts_allow.[AstAllowedLast2]
      ,ts_allow.[AstAllowedLast10]
      ,ts_allow.[AstAllowedSeason]
      ,ts_allow.[StlAllowedLast2]
      ,ts_allow.[StlAllowedLast10]
      ,ts_allow.[StlAllowedSeason]
      ,ts_allow.[BlkAllowedLast2]
      ,ts_allow.[BlkAllowedLast10]
      ,ts_allow.[BlkAllowedSeason]
      ,ts_allow.[TovAllowedLast2]
      ,ts_allow.[TovAllowedLast10]
      ,ts_allow.[TovAllowedSeason]
      ,ts_allow.[ORatingAllowedLast2]
      ,ts_allow.[ORatingAllowedLast10]
      ,ts_allow.[ORatingAllowedSeason]
      ,ts_allow.[DRatingAllowedLast2]
      ,ts_allow.[DRatingAllowedLast10]
      ,ts_allow.[DRatingAllowedSeason]
      ,ts_allow.[NetRatingAllowedLast2]
      ,ts_allow.[NetRatingAllowedLast10]
      ,ts_allow.[NetRatingAllowedSeason]
      ,ts_allow.[AstPctAllowedLast2]
      ,ts_allow.[AstPctAllowedLast10]
      ,ts_allow.[AstPctAllowedSeason]
      ,ts_allow.[AstToTovAllowedLast2]
      ,ts_allow.[AstToTovAllowedLast10]
      ,ts_allow.[AstToTovAllowedSeason]
      ,ts_allow.[AstRatioAllowedLast2]
      ,ts_allow.[AstRatioAllowedLast10]
      ,ts_allow.[AstRatioAllowedSeason]
      ,ts_allow.[ORebPctAllowedLast2]
      ,ts_allow.[ORebPctAllowedLast10]
      ,ts_allow.[ORebPctAllowedSeason]
      ,ts_allow.[DRebPctAllowedLast2]
      ,ts_allow.[DRebPctAllowedLast10]
      ,ts_allow.[DRebPctAllowedSeason]
      ,ts_allow.[RebPctAllowedLast2]
      ,ts_allow.[RebPctAllowedLast10]
      ,ts_allow.[RebPctAllowedSeason]
      ,ts_allow.[TovRatioAllowedLast2]
      ,ts_allow.[TovRatioAllowedLast10]
      ,ts_allow.[TovRatioAllowedSeason]
      ,ts_allow.[EffFldGoalPctAllowedLast2]
      ,ts_allow.[EffFldGoalPctAllowedLast10]
      ,ts_allow.[EffFldGoalPctAllowedSeason]
      ,ts_allow.[TruShoPctAllowedLast2]
      ,ts_allow.[TruShoPctPctAllowedLast10]
      ,ts_allow.[TruShoPctPctAllowedSeason]
      ,ts_allow.[PaceAllowedLast2]
      ,ts_allow.[PaceAllowedLast10]
      ,ts_allow.[PaceAllowedSeason]
      ,ts_allow.[PacePer40AllowedLast2]
      ,ts_allow.[PacePer40AllowedLast10]
      ,ts_allow.[PacePer40AllowedSeason]
      ,ts_allow.[PossessionsAllowedLast2]
      ,ts_allow.[PossessionsAllowedLast10]
      ,ts_allow.[PossessionsAllowedSeason]
      ,ts_allow.[PIEAllowedLast2]
      ,ts_allow.[PIEAllowedLast10]
      ,ts_allow.[PIEAllowedSeason]
      ,ts_allow.[FGAPct2AtmtAllowedLast2]
      ,ts_allow.[FGAPct2AtmtAllowedLast10]
      ,ts_allow.[FGAPct2AtmtAllowedSeason]
      ,ts_allow.[FGAPct3AtmtAllowedLast2]
      ,ts_allow.[FGAPct3AtmtAllowedLast10]
      ,ts_allow.[FGAPct3AtmtAllowedSeason]
      ,ts_allow.[PctPts2PtrsAllowedLast2]
      ,ts_allow.[PctPts2PtrsAllowedLast10]
      ,ts_allow.[PctPts2PtrsAllowedSeason]
      ,ts_allow.[PctPts3PtrsAllowedLast2]
      ,ts_allow.[PctPts3PtrsAllowedLast10]
      ,ts_allow.[PctPts3PtrsAllowedSeason]
      ,ts_allow.[PctPtsMidRange2PtrsAllowedLast2]
      ,ts_allow.[PctPtsMidRange2PtrsAllowedLast10]
      ,ts_allow.[PctPtsMidRange2PtrsAllowedSeason]
      ,ts_allow.[PctPtsFastBreakPtrsAllowedLast2]
      ,ts_allow.[PctPtsFastBreakPtrsAllowedLast10]
      ,ts_allow.[PctPtsFastBreakPtrsAllowedSeason]
      ,ts_allow.[PctPtsFreeThrwPtrsAllowedLast2]
      ,ts_allow.[PctPtsFreeThrwPtrsAllowedLast10]
      ,ts_allow.[PctPtsFreeThrwPtrsAllowedSeason]
      ,ts_allow.[PctPtsOffTovPtrsAllowedLast2]
      ,ts_allow.[PctPtsOffTovPtrsAllowedLast10]
      ,ts_allow.[PctPtsOffTovPtrsAllowedSeason]
      ,ts_allow.[PctPtsInPaintAllowedLast2]
      ,ts_allow.[PctPtsInPaintAllowedLast10]
      ,ts_allow.[PctPtsInPaintAllowedSeason]
      ,ts_allow.[PctPtsAstsed2AllowedLast2]
      ,ts_allow.[PctPtsAstsed2AllowedLast10]
      ,ts_allow.[PctPtsAstsed2AllowedSeason]
      ,ts_allow.[PctPtsUnAstsed2AllowedLast2]
      ,ts_allow.[PctPtsUnAstsed2AllowedLast10]
      ,ts_allow.[PctPtsUnAstsed2AllowedSeason]
      ,ts_allow.[PctPtsAstsed3AllowedLast2]
      ,ts_allow.[PctPtsAstsed3AllowedLast10]
      ,ts_allow.[PctPtsAstsed3AllowedSeason]
      ,ts_allow.[PctPtsUnAstsed3AllowedLast2]
      ,ts_allow.[PctPtsUnAstsed3AllowedLast10]
      ,ts_allow.[PctPtsUnAstsed3AllowedSeason]
      ,ts_allow.[PctPtsAstsedAllowedLast2]
      ,ts_allow.[PctPtsAstsedAllowedLast10]
      ,ts_allow.[PctPtsAstsedAllowedSeason]
      ,ts_allow.[PctPtsUnAstsedAllowedLast2]
      ,ts_allow.[PctPtsUnAstsedAllowedLast10]
      ,ts_allow.[PctPtsUnAstsedAllowedSeason]
      ,ts_allow.[DistanceAllowedLast2]
      ,ts_allow.[DistanceAllowedLast10]
      ,ts_allow.[DistanceAllowedSeason]
      ,ts_allow.[ORebChncsAllowedLast2]
      ,ts_allow.[ORebChncsAllowedLast10]
      ,ts_allow.[ORebChncsAllowedSeason]
      ,ts_allow.[DRebChncsAllowedLast2]
      ,ts_allow.[DRebChncsAllowedLast10]
      ,ts_allow.[DRebChncsAllowedSeason]
      ,ts_allow.[RebChncsAllowedLast2]
      ,ts_allow.[RebChncsAllowedLast10]
      ,ts_allow.[RebChncsAllowedSeason]
      ,ts_allow.[TchsAllowedLast2]
      ,ts_allow.[TchsAllowedLast10]
      ,ts_allow.[TchsAllowedSeason]
      ,ts_allow.[SecdryAstAllowedLast2]
      ,ts_allow.[SecdryAstAllowedLast10]
      ,ts_allow.[SecdryAstAllowedSeason]
      ,ts_allow.[FTAstAllowedLast2]
      ,ts_allow.[FTAstAllowedLast10]
      ,ts_allow.[FTAstAllowedSeason]
      ,ts_allow.[PassesAllowedLast2]
      ,ts_allow.[PassesAllowedLast10]
      ,ts_allow.[PassesAllowedSeason]
      ,ts_allow.[ContestedFieldGoalPctAllowedLast2]
      ,ts_allow.[ContestedFieldGoalPctAllowedLast10]
      ,ts_allow.[ContestedFieldGoalPctAllowedSeason]
      ,ts_allow.[UnContestedFieldGoalPctAllowedLast2]
      ,ts_allow.[UnContestedFieldGoalPctAllowedLast10]
      ,ts_allow.[UnContestedFieldGoalPctAllowedSeason]
      ,ts_allow.[DefendedAtRimFieldGoalPctAllowedLast2]
      ,ts_allow.[DefendedAtRimFieldGoalPctAllowedLast10]
      ,ts_allow.[DefendedAtRimFieldGoalPctAllowedSeason]

	  ,b.PlusMinusLast2 * ts_allow.TchsAllowedLast2 as OppDefLeniencyImpact
	  ,PaintPtsTrend * TchsAllowedSeason AS PaintPtsTrendVsTchsAllowedSeason,
	  MinutesPerGameLast8 * TchsAllowedSeason AS MPGRecentVsTchsAllowedSeason,
	  SecondChancePtsLast8 * TchsAllowedLast2 AS SecondChancePtsRecentVsTchsAllowedShort,
	  PossessionsLast20 * TchsAllowedLast2 AS PossessionsRecentVsTchsAllowedShort,
	  OffensiveRatingLast2 * TchsAllowedLast2 AS OffRatingRecentVsTchsAllowedShort,
	  DefensiveRatingLast2 * TchsAllowedLast2 AS DefRatingRecentVsTchsAllowedShort,
	  SecondChancePtsLast8 * TchsAllowedSeason AS SecondChancePtsRecentVsTchsAllowedSeason,
	  FastBreakPtsLast2 * TchsAllowedLast2 AS FastBreakPtsRecentVsTchsAllowedShort,
	  PersonalFoulsDrawnTrend * TchsAllowedLast2 AS PFoulsDrawnTrendVsTchsAllowedShort,
	  avgPtsAllowedPerGameLast2 * TchsAllowedLast2 AS AvgPtsAllowedRecentVsTchsAllowedShort,
	  FantPointsLast20 * TchsAllowedSeason AS FantPtsRecentVsTchsAllowedSeason,
	  PTSOffTOLast2 * TchsAllowedLast2 AS PTSOffTORecentVsTchsAllowedShort,
	  PaintPtsLast20 * TchsAllowedSeason AS PaintPtsRecentVsTchsAllowedSeason,
	  PlusMinusLast2 * TchsAllowedLast10 AS PlusMinusRecentVsTchsAllowedMedium,
	  SecondChancePtsLast2 * TchsAllowedLast2 AS SecondChancePtsRecentVsTchsAllowedShort2,
	  NetRatingLast8 * TchsAllowedLast2 AS NetRatingRecentVsTchsAllowedShort,
	  FantPointsDrawnTrend * TchsAllowedSeason AS FantPtsDrawnTrendVsTchsAllowedSeason,
	  TchsAllowedLast10 * TchsAllowedSeason AS TchsAllowedMediumVsSeason,
	  TchsAllowedLast2 * TchsAllowedLast10 AS TchsAllowedShortVsMedium,
	  PossessionsTrend * TchsAllowedSeason AS PossessionsTrendVsTchsAllowedSeason,
	  DefensiveRatingLast2 * TchsAllowedLast10 AS DefRatingRecentVsTchsAllowedMedium,
	  PaceAllowedSeason * TchsAllowedLast2 AS PaceAllowedSeasonVsTchsAllowedShort,
	  FantPointsLast2 * TchsAllowedLast2 AS FantPtsRecentVsTchsAllowedShort,
	  PlusMinusLast20 * TchsAllowedLast10 AS PlusMinusRecentVsTchsAllowedMedium2,
	  PossessionsLast2 * TchsAllowedLast10 AS PossessionsRecentVsTchsAllowedMedium,
	  PlusMinusLast8 * TchsAllowedLast2 AS PlusMinusRecentVsTchsAllowedShort,
	  MinutesPerGameLast2 * TchsAllowedSeason AS MPGRecentVsTchsAllowedSeason2,
	  PossessionsAllowedLast2 * TchsAllowedLast2 AS PossessionsAllowedRecentVsTchsAllowedShort,
	  PersonalFoulsLast8 * TchsAllowedSeason AS PFoulsRecentVsTchsAllowedSeason,
	  FantPointsLast8 * TchsAllowedLast2 AS FantPtsRecentVsTchsAllowedMedium,
	  PlusMinusLast8 * TchsAllowedLast10 AS PlusMinusRecentVsTchsAllowedMedium3,
	  PaintPtsLast20 * TchsAllowedLast10 AS PaintPtsRecentVsTchsAllowedMedium,
	  NetRatingLast20 * TchsAllowedLast10 AS NetRatingLongVsTchsAllowedMedium,
	  PossessionsLast2 * TchsAllowedLast2 AS PossessionsRecentVsTchsAllowedShort2,
	  avgPtsAllowedPerGameSeason * TchsAllowedLast2 AS AvgPtsAllowedSeasonVsTchsAllowedShort,
	  FastBreakPtsLast2 * TchsAllowedLast10 AS FastBreakPtsRecentVsTchsAllowedMedium,
	  FantPointsLast20 * TchsAllowedLast2 AS FantPtsRecentVsTchsAllowedShort2,
	  TchsAllowedLast2 * TchsAllowedSeason AS TchsAllowedShortVsSeason,
	  SecondChancePtsLast2 * TchsAllowedLast10 AS SecondChancePtsRecentVsTchsAllowedMedium,
	  PaintPtsLast8 * TchsAllowedLast10 AS PaintPtsRecentVsTchsAllowedMedium2,
	 
	  TeamNetRatingLast2 * TchsAllowedLast2 AS TeamNetRatingRecentVsTchsAllowedShort,
	  MinutesPerGameLast2 * TchsAllowedLast2 AS MPGRecentVsTchsAllowedShort,
	  PossessionsAllowedSeason * TchsAllowedLast10 AS PossessionsAllowedSeasonVsTchsAllowedMedium,
	  PTSOffTOTrend * TchsAllowedSeason AS PTSOffTOTrendVsTchsAllowedSeason,
	  DefensiveRatingLast2 * avgPtsAllowedPerGameLast2 AS DefRatingVsAvgPtsAllowedRecent,
	  DefensiveRatingLast2 * PossessionsLast2 AS DefRatingVsPossessionsRecent,
	  OffensiveRatingLast2 * OffensiveRatingLast8 AS OffRatingShortVsMedium,
	  OffensiveRatingLast2 * DefensiveRatingLast2 AS OffRatingVsDefRatingRecent,
	  PlusMinusLast2 * OffensiveRatingLast2 AS PlusMinusVsOffRatingRecent,
	  TeamORatingLast2 * DRatingAllowedLast2 AS TeamORatingVsDRatingAllowed,
	  PossessionsLast2 * TeamNetRatingLast2 AS PossessionsVsTeamNetRating,
	  OffensiveRatingLast2 * PossessionsLast2 AS OffRatingVsPossessionsRecent,
	  TeamDRatingLast2 * avgPtsAllowedPerGameLast2 AS TeamDRatingVsAvgPtsAllowedRecent,
	  OffensiveRatingLast2 * TeamNetRatingLast2 AS OffRatingVsTeamNetRating,
	  OffensiveRatingLast2 * NetRatingLast8 AS OffRatingVsNetRatingMedium,
	  OffensiveRatingLast2 * avgPtsAllowedPerGameSeason AS OffRatingVsAvgPtsAllowedSeason,
	  FantPointsLast2 * OffensiveRatingLast2 AS FantPtsVsOffRatingRecent,
	  TeamNetRatingLast2 * NetRatingAllowedLast2 AS TeamNetRatingVsNetRatingAllowed,
	  FantPointsLast2 * PossessionsLast2 AS FantPtsVsPossessionsRecent,
	  DefensiveRatingLast2 * TeamNetRatingLast2 AS DefRatingVsTeamNetRating,
	  NetRatingLast8 * TeamORatingLast2 AS NetRatingVsTeamORating,
	  OffensiveRatingLast2 * avgPtsAllowedPerGameLast10 AS OffRatingVsAvgPtsAllowedLast10,
	  PlusMinusLast2 * DefensiveRatingLast2 AS PlusMinusVsDefRating,
	  PossessionsLast2 * avgPtsAllowedPerGameLast2 AS PossessionsVsAvgPtsAllowedRecent,
	  OffensiveRatingLast2 * TeamORatingLast2 AS OffRatingVsTeamORating,
	  PlusMinusLast2 * FantPointsLast2 AS PlusMinusVsFantPts,
	  DefensiveRatingLast2 * DefensiveRatingLast8 AS DefRatingShortVsMedium,
	  MinutesPerGameLast2 * OffensiveRatingLast2 AS MPGRecentVsOffRating,
	  FantPointsLast2 * TeamNetRatingLast2 AS FantPtsVsTeamNetRating,
	  OffensiveRatingLast2 * NetRatingLast20 AS OffRatingVsNetRatingLong,
	  OffensiveRatingLast8 * DefensiveRatingLast2 AS OffRatingMediumVsDefRating,
	  DefensiveRatingLast2 * TeamORatingLast2 AS DefRatingVsTeamORating,
	  FantPointsLast20 * avgPtsAllowedPerGameLast2 AS FantPtsLongVsAvgPtsAllowedRecent,
	  FantPointsLast2 * DefensiveRatingLast2 AS FantPtsVsDefRating,
	  OffensiveRatingLast2 * PTSOffTOTrend AS OffRatingVsPTSOffTOTrend

  From base b

  left outer join [nba_game_data].[dbo].[all_team_stats_for_preds] ts_allow
  on 
 b.oppAbrv= ts_allow.TEAM_ABBREVIATION
 and b.GAME_ID = ts_allow.GAME_ID

 where b.yearSeason in (2024)

  ORDER BY b.GAME_DATE DESC
  ' 
 
  EXEC sp_executesql @sql,
  N'@playerIdParam INT, @ptsThreshParam FLOAT', 
                       @playerIdParam = @player_id, 
                       @ptsThreshParam = @pts_thresh;
  end;

