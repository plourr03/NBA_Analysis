import pandas as pd
import numpy as np
import pandas as pd
import time
import pyodbc
import matplotlib.pyplot as plt
from sklearn.model_selection import cross_val_score, KFold
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC
from sklearn.naive_bayes import GaussianNB
from xgboost import XGBClassifier
from catboost import CatBoostClassifier
from lightgbm import LGBMClassifier
from sklearn.neural_network import MLPClassifier
import warnings
from skopt import BayesSearchCV
from sklearn.model_selection import GridSearchCV, LeaveOneOut
from sklearn.feature_selection import SelectFromModel
from skopt import BayesSearchCV
from skopt.space import Real, Categorical, Integer
from sklearn.model_selection import StratifiedKFold
from hyperopt import hp, fmin, tpe, Trials, STATUS_OK
from sklearn.metrics import f1_score, accuracy_score
from random import randint
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import accuracy_score

warnings.simplefilter("ignore")
def get_pred_data(player_id, last_n_games,opp):
    server = 'localhost\SQLEXPRESS'
    database = 'nba_game_data'
    sql = f'''
    
      SELECT 
    [PLAYER_ID]
    ,[GAME_ID]

    ---last x games data ---
    ,AVG(CAST(FGM AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS FGMLast{last_n_games}
    ,AVG(CAST(FGA AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS FGALast{last_n_games}
    ,AVG(CAST(FG_PCT AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS FG_PCTLast{last_n_games}
    ,AVG(CAST(FG3M AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS FG3MLast{last_n_games}
    ,AVG(CAST(FG3A AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS FG3ALast{last_n_games}
    ,AVG(CAST(FG3_PCT AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS FG3_PCTLast{last_n_games}
    ,AVG(CAST(FTM AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS FTMLast{last_n_games}
    ,AVG(CAST(FTA AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS FTALast{last_n_games}
    ,AVG(CAST(FT_PCT AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS FT_PCTLast{last_n_games}
    ,AVG(CAST(OREB AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS OREBLast{last_n_games}
    ,AVG(CAST(DREB AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS DREBLast{last_n_games}
    ,AVG(CAST(REB AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS REBLast{last_n_games}
    ,AVG(CAST(AST AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS ASTLast{last_n_games}
    ,AVG(CAST(TOV AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS TOVLast{last_n_games}
    ,AVG(CAST(STL AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS STLLast{last_n_games}
    ,AVG(CAST(BLK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS BLKLast{last_n_games}
    ,AVG(CAST(BLKA AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS BLKALast{last_n_games}
    ,AVG(CAST(PF AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS PFLast{last_n_games}
    ,AVG(CAST(PFD AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS PFDLast{last_n_games}
    ,AVG(CAST(PTS AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS PTSLast{last_n_games}
    ,AVG(CAST(PLUS_MINUS AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS PLUS_MINUSLast{last_n_games}
    ,AVG(CAST(NBA_FANTASY_PTS AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS NBA_FANTASY_PTSLast{last_n_games}
    ,AVG(CAST(DD2 AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS DD2Last{last_n_games}
    ,AVG(CAST(TD3 AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS TD3Last{last_n_games}
    ,AVG(CAST(WNBA_FANTASY_PTS AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS WNBA_FANTASY_PTSLast{last_n_games}
    ,AVG(CAST(GP_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS GP_RANKLast{last_n_games}
    ,AVG(CAST(W_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS W_RANKLast{last_n_games}
    ,AVG(CAST(L_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS L_RANKLast{last_n_games}
    ,AVG(CAST(W_PCT_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS W_PCT_RANKLast{last_n_games}
    ,AVG(CAST(MIN_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS MIN_RANKLast{last_n_games}
    ,AVG(CAST(FGM_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS FGM_RANKLast{last_n_games}
    ,AVG(CAST(FGA_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS FGA_RANKLast{last_n_games}
    ,AVG(CAST(FG_PCT_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS FG_PCT_RANKLast{last_n_games}
    ,AVG(CAST(FG3M_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS FG3M_RANKLast{last_n_games}
    ,AVG(CAST(FG3A_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS FG3A_RANKLast{last_n_games}
    ,AVG(CAST(FG3_PCT_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS FG3_PCT_RANKLast{last_n_games}
    ,AVG(CAST(FTM_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS FTM_RANKLast{last_n_games}
    ,AVG(CAST(FTA_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS FTA_RANKLast{last_n_games}
    ,AVG(CAST(FT_PCT_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS FT_PCT_RANKLast{last_n_games}
    ,AVG(CAST(OREB_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS OREB_RANKLast{last_n_games}
    ,AVG(CAST(DREB_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS DREB_RANKLast{last_n_games}
    ,AVG(CAST(REB_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS REB_RANKLast{last_n_games}
    ,AVG(CAST(AST_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS AST_RANKLast{last_n_games}
    ,AVG(CAST(TOV_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS TOV_RANKLast{last_n_games}
    ,AVG(CAST(STL_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS STL_RANKLast{last_n_games}
    ,AVG(CAST(BLK_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS BLK_RANKLast{last_n_games}
    ,AVG(CAST(BLKA_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS BLKA_RANKLast{last_n_games}
    ,AVG(CAST(PF_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS PF_RANKLast{last_n_games}
    ,AVG(CAST(PFD_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS PFD_RANKLast{last_n_games}
    ,AVG(CAST(PTS_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS PTS_RANKLast{last_n_games}
    ,AVG(CAST(PLUS_MINUS_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS PLUS_MINUS_RANKLast{last_n_games}
    ,AVG(CAST(NBA_FANTASY_PTS_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS NBA_FANTASY_PTS_RANKLast{last_n_games}
    ,AVG(CAST(DD2_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS DD2_RANKLast{last_n_games}
    ,AVG(CAST(TD3_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS TD3_RANKLast{last_n_games}
    ,AVG(CAST(WNBA_FANTASY_PTS_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS WNBA_FANTASY_PTS_RANKLast{last_n_games}
    ,AVG(CAST(estimatedOffensiveRating AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS estimatedOffensiveRatingLast{last_n_games}
    ,AVG(CAST(offensiveRating AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS offensiveRatingLast{last_n_games}
    ,AVG(CAST(estimatedDefensiveRating AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS estimatedDefensiveRatingLast{last_n_games}
    ,AVG(CAST(defensiveRating AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS defensiveRatingLast{last_n_games}
    ,AVG(CAST(estimatedNetRating AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS estimatedNetRatingLast{last_n_games}
    ,AVG(CAST(netRating AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS netRatingLast{last_n_games}
    ,AVG(CAST(assistPercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS assistPercentageLast{last_n_games}
    ,AVG(CAST(assistToTurnover AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS assistToTurnoverLast{last_n_games}
    ,AVG(CAST(assistRatio AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS assistRatioLast{last_n_games}
    ,AVG(CAST(offensiveReboundPercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS offensiveReboundPercentageLast{last_n_games}
    ,AVG(CAST(defensiveReboundPercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS defensiveReboundPercentageLast{last_n_games}
    ,AVG(CAST(reboundPercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS reboundPercentageLast{last_n_games}
    ,AVG(CAST(turnoverRatio AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS turnoverRatioLast{last_n_games}
    ,AVG(CAST(effectiveFieldGoalPercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS effectiveFieldGoalPercentageLast{last_n_games}
    ,AVG(CAST(trueShootingPercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS trueShootingPercentageLast{last_n_games}
    ,AVG(CAST(usagePercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS usagePercentageLast{last_n_games}
    ,AVG(CAST(estimatedUsagePercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS estimatedUsagePercentageLast{last_n_games}
    ,AVG(CAST(estimatedPace AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS estimatedPaceLast{last_n_games}
    ,AVG(CAST(pace AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS paceLast{last_n_games}
    ,AVG(CAST(pacePer40 AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS pacePer40Last{last_n_games}
    ,AVG(CAST(possessions AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS possessionsLast{last_n_games}
    ,AVG(CAST(PIE AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS PIELast{last_n_games}
    ,AVG(CAST(matchupMinutes AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS matchupMinutesLast{last_n_games}
    ,AVG(CAST(partialPossessions AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS partialPossessionsLast{last_n_games}
    ,AVG(CAST(switchesOn AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS switchesOnLast{last_n_games}
    ,AVG(CAST(playerPoints AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS playerPointsLast{last_n_games}
    ,AVG(CAST(defensiveRebounds AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS defensiveReboundsLast{last_n_games}
    ,AVG(CAST(matchupAssists AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS matchupAssistsLast{last_n_games}
    ,AVG(CAST(matchupTurnovers AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS matchupTurnoversLast{last_n_games}
    ,AVG(CAST(matchupFieldGoalsMade AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS matchupFieldGoalsMadeLast{last_n_games}
    ,AVG(CAST(matchupFieldGoalsAttempted AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS matchupFieldGoalsAttemptedLast{last_n_games}
    ,AVG(CAST(matchupFieldGoalPercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS matchupFieldGoalPercentageLast{last_n_games}
    ,AVG(CAST(matchupThreePointersMade AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS matchupThreePointersMadeLast{last_n_games}
    ,AVG(CAST(matchupThreePointersAttempted AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS matchupThreePointersAttemptedLast{last_n_games}
    ,AVG(CAST(matchupThreePointerPercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS matchupThreePointerPercentageLast{last_n_games}
    ,AVG(CAST(contestedShots AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS contestedShotsLast{last_n_games}
    ,AVG(CAST(contestedShots2pt AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS contestedShots2ptLast{last_n_games}
    ,AVG(CAST(contestedShots3pt AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS contestedShots3ptLast{last_n_games}
    ,AVG(CAST(deflections AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS deflectionsLast{last_n_games}
    ,AVG(CAST(chargesDrawn AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS chargesDrawnLast{last_n_games}
    ,AVG(CAST(screenAssists AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS screenAssistsLast{last_n_games}
    ,AVG(CAST(screenAssistPoints AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS screenAssistPointsLast{last_n_games}
    ,AVG(CAST(looseBallsRecoveredOffensive AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS looseBallsRecoveredOffensiveLast{last_n_games}
    ,AVG(CAST(looseBallsRecoveredDefensive AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS looseBallsRecoveredDefensiveLast{last_n_games}
    ,AVG(CAST(looseBallsRecoveredTotal AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS looseBallsRecoveredTotalLast{last_n_games}
    ,AVG(CAST(offensiveBoxOuts AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS offensiveBoxOutsLast{last_n_games}
    ,AVG(CAST(defensiveBoxOuts AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS defensiveBoxOutsLast{last_n_games}
    ,AVG(CAST(boxOutPlayerTeamRebounds AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS boxOutPlayerTeamReboundsLast{last_n_games}
    ,AVG(CAST(boxOutPlayerRebounds AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS boxOutPlayerReboundsLast{last_n_games}
    ,AVG(CAST(boxOuts AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS boxOutsLast{last_n_games}
    ,AVG(CAST(pointsOffTurnovers AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS pointsOffTurnoversLast{last_n_games}
    ,AVG(CAST(pointsSecondChance AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS pointsSecondChanceLast{last_n_games}
    ,AVG(CAST(pointsFastBreak AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS pointsFastBreakLast{last_n_games}
    ,AVG(CAST(pointsPaint AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS pointsPaintLast{last_n_games}
    ,AVG(CAST(oppPointsOffTurnovers AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS oppPointsOffTurnoversLast{last_n_games}
    ,AVG(CAST(oppPointsSecondChance AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS oppPointsSecondChanceLast{last_n_games}
    ,AVG(CAST(oppPointsFastBreak AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS oppPointsFastBreakLast{last_n_games}
    ,AVG(CAST(oppPointsPaint AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS oppPointsPaintLast{last_n_games}
    ,AVG(CAST(blocksAgainst AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS blocksAgainstLast{last_n_games}
    ,AVG(CAST(foulsPersonal AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS foulsPersonalLast{last_n_games}
    ,AVG(CAST(foulsDrawn AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS foulsDrawnLast{last_n_games}
    ,AVG(CAST(percentageFieldGoalsAttempted2pt AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS percentageFieldGoalsAttempted2ptLast{last_n_games}
    ,AVG(CAST(percentageFieldGoalsAttempted3pt AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS percentageFieldGoalsAttempted3ptLast{last_n_games}
    ,AVG(CAST(percentagePoints2pt AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS percentagePoints2ptLast{last_n_games}
    ,AVG(CAST(percentagePointsMidrange2pt AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS percentagePointsMidrange2ptLast{last_n_games}
    ,AVG(CAST(percentagePoints3pt AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS percentagePoints3ptLast{last_n_games}
    ,AVG(CAST(percentagePointsFastBreak AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS percentagePointsFastBreakLast{last_n_games}
    ,AVG(CAST(percentagePointsFreeThrow AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS percentagePointsFreeThrowLast{last_n_games}
    ,AVG(CAST(percentagePointsOffTurnovers AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS percentagePointsOffTurnoversLast{last_n_games}
    ,AVG(CAST(percentagePointsPaint AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS percentagePointsPaintLast{last_n_games}
    ,AVG(CAST(percentageAssisted2pt AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS percentageAssisted2ptLast{last_n_games}
    ,AVG(CAST(percentageUnassisted2pt AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS percentageUnassisted2ptLast{last_n_games}
    ,AVG(CAST(percentageAssisted3pt AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS percentageAssisted3ptLast{last_n_games}
    ,AVG(CAST(percentageUnassisted3pt AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS percentageUnassisted3ptLast{last_n_games}
    ,AVG(CAST(percentageAssistedFGM AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS percentageAssistedFGMLast{last_n_games}
    ,AVG(CAST(percentageUnassistedFGM AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1} PRECEDING AND CURRENT ROW) AS percentageUnassistedFGMLast{last_n_games}
    ,AVG(CAST(speed AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1}PRECEDING AND 1 PRECEDING) AS speedLast{last_n_games}
    ,AVG(CAST(distance AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1}PRECEDING AND 1 PRECEDING) AS distanceLast{last_n_games}
    ,AVG(CAST(reboundChancesOffensive AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1}PRECEDING AND 1 PRECEDING) AS reboundChancesOffensiveLast{last_n_games}
    ,AVG(CAST(reboundChancesDefensive AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1}PRECEDING AND 1 PRECEDING) AS reboundChancesDefensiveLast{last_n_games}
    ,AVG(CAST(reboundChancesTotal AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1}PRECEDING AND 1 PRECEDING) AS reboundChancesTotalLast{last_n_games}
    ,AVG(CAST(touches AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1}PRECEDING AND 1 PRECEDING) AS touchesLast{last_n_games}
    ,AVG(CAST(secondaryAssists AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1}PRECEDING AND 1 PRECEDING) AS secondaryAssistsLast{last_n_games}
    ,AVG(CAST(freeThrowAssists AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1}PRECEDING AND 1 PRECEDING) AS freeThrowAssistsLast{last_n_games}
    ,AVG(CAST(passes AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1}PRECEDING AND 1 PRECEDING) AS passesLast{last_n_games}
    ,AVG(CAST(assists AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1}PRECEDING AND 1 PRECEDING) AS assistsLast{last_n_games}
    ,AVG(CAST(contestedFieldGoalsMade AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1}PRECEDING AND 1 PRECEDING) AS contestedFieldGoalsMadeLast{last_n_games}
    ,AVG(CAST(contestedFieldGoalsAttempted AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1}PRECEDING AND 1 PRECEDING) AS contestedFieldGoalsAttemptedLast{last_n_games}
    ,AVG(CAST(contestedFieldGoalPercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1}PRECEDING AND 1 PRECEDING) AS contestedFieldGoalPercentageLast{last_n_games}
    ,AVG(CAST(uncontestedFieldGoalsMade AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1}PRECEDING AND 1 PRECEDING) AS uncontestedFieldGoalsMadeLast{last_n_games}
    ,AVG(CAST(uncontestedFieldGoalsAttempted AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1}PRECEDING AND 1 PRECEDING) AS uncontestedFieldGoalsAttemptedLast{last_n_games}
    ,AVG(CAST(uncontestedFieldGoalsPercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1}PRECEDING AND 1 PRECEDING) AS uncontestedFieldGoalsPercentageLast{last_n_games}
    ,AVG(CAST(fieldGoalPercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1}PRECEDING AND 1 PRECEDING) AS fieldGoalPercentageLast{last_n_games}
    ,AVG(CAST(defendedAtRimFieldGoalsMade AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1}PRECEDING AND 1 PRECEDING) AS defendedAtRimFieldGoalsMadeLast{last_n_games}
    ,AVG(CAST(defendedAtRimFieldGoalsAttempted AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1}PRECEDING AND 1 PRECEDING) AS defendedAtRimFieldGoalsAttemptedLast{last_n_games}
    ,AVG(CAST(defendedAtRimFieldGoalPercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games-1}PRECEDING AND 1 PRECEDING) AS defendedAtRimFieldGoalPercentageLast{last_n_games}

      FROM BasePlayer
      WHERE PLAYER_ID = '{player_id}'
      
      order by GAME_DATE

    '''
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';')
    cursor = cnxn.cursor()
    df = pd.read_sql(sql,cnxn)
    
    sql = f'''with base as (
    SELECT  
	   lgl.[SEASON_ID] as SEASON_YEAR
      ,lgl.[TEAM_ID]
      ,lgl.[TEAM_ABBREVIATION]
      ,lgl.[TEAM_NAME]
      ,lgl.[GAME_ID]
      ,lgl.[GAME_DATE]
	  ,lgl.[oppAbrv]
	  ,lgl.[yearSeason]
      ,lgl.[MATCHUP]
      ,lgl.[WL]
      ,lgl.[MIN]
      ,lgl.[FGM]
      ,lgl.[FGA]
      ,lgl.[FG_PCT]
      ,lgl.[FG3M]
      ,lgl.[FG3A]
      ,lgl.[FG3_PCT]
      ,lgl.[FTM]
      ,lgl.[FTA]
      ,lgl.[FT_PCT]
      ,lgl.[OREB]
      ,lgl.[DREB]
      ,lgl.[REB]
      ,lgl.[AST]
      ,lgl.[STL]
      ,lgl.[BLK]
      ,lgl.[TOV]
      ,lgl.[PF]
      ,lgl.[PTS]
      ,lgl.[PLUS_MINUS]

	  ,tbsa.[estimatedOffensiveRating]
      ,tbsa.[offensiveRating]
      ,tbsa.[estimatedDefensiveRating]
      ,tbsa.[defensiveRating]
      ,tbsa.[estimatedNetRating]
      ,tbsa.[netRating]
      ,tbsa.[assistPercentage]
      ,tbsa.[assistToTurnover]
      ,tbsa.[assistRatio]
      ,tbsa.[offensiveReboundPercentage]
      ,tbsa.[defensiveReboundPercentage]
      ,tbsa.[reboundPercentage]
      ,tbsa.[estimatedTeamTurnoverPercentage]
      ,tbsa.[turnoverRatio]
      ,tbsa.[effectiveFieldGoalPercentage]
      ,tbsa.[trueShootingPercentage]
      ,tbsa.[usagePercentage]
      ,tbsa.[estimatedUsagePercentage]
      ,tbsa.[estimatedPace]
      ,tbsa.[pace]
      ,tbsa.[pacePer40]
      ,tbsa.[possessions]
      ,tbsa.[PIE]

	  ,tbsh.[contestedShots]
      ,tbsh.[contestedShots2pt]
      ,tbsh.[contestedShots3pt]
      ,tbsh.[deflections]
      ,tbsh.[chargesDrawn]
      ,tbsh.[screenAssists]
      ,tbsh.[screenAssistPoints]
      ,tbsh.[looseBallsRecoveredOffensive]
      ,tbsh.[looseBallsRecoveredDefensive]
      ,tbsh.[looseBallsRecoveredTotal]
      ,tbsh.[offensiveBoxOuts]
      ,tbsh.[defensiveBoxOuts]
      ,tbsh.[boxOutPlayerTeamRebounds]
      ,tbsh.[boxOutPlayerRebounds]
      ,tbsh.[boxOuts]

	  ,tbsm.[pointsOffTurnovers]
      ,tbsm.[pointsSecondChance]
      ,tbsm.[pointsFastBreak]
      ,tbsm.[pointsPaint]
      ,tbsm.[oppPointsOffTurnovers]
      ,tbsm.[oppPointsSecondChance]
      ,tbsm.[oppPointsFastBreak]
      ,tbsm.[oppPointsPaint]
      ,tbsm.[blocks]
      ,tbsm.[blocksAgainst]
      ,tbsm.[foulsPersonal]
      ,tbsm.[foulsDrawn]

	  ,tbst.[distance]
      ,tbst.[reboundChancesOffensive]
      ,tbst.[reboundChancesDefensive]
      ,tbst.[reboundChancesTotal]
      ,tbst.[touches]
      ,tbst.[secondaryAssists]
      ,tbst.[freeThrowAssists]
      ,tbst.[passes]
      ,tbst.[assists]
      ,tbst.[contestedFieldGoalsMade]
      ,tbst.[contestedFieldGoalsAttempted]
      ,tbst.[contestedFieldGoalPercentage]
      ,tbst.[uncontestedFieldGoalsMade]
      ,tbst.[uncontestedFieldGoalsAttempted]
      ,tbst.[uncontestedFieldGoalsPercentage]
      ,tbst.[fieldGoalPercentage]
      ,tbst.[defendedAtRimFieldGoalsMade]
      ,tbst.[defendedAtRimFieldGoalsAttempted]
      ,tbst.[defendedAtRimFieldGoalPercentage]

      ,tbss.[percentageFieldGoalsAttempted2pt]
      ,tbss.[percentageFieldGoalsAttempted3pt]
      ,tbss.[percentagePoints2pt]
      ,tbss.[percentagePointsMidrange2pt]
      ,tbss.[percentagePoints3pt]
      ,tbss.[percentagePointsFastBreak]
      ,tbss.[percentagePointsFreeThrow]
      ,tbss.[percentagePointsOffTurnovers]
      ,tbss.[percentagePointsPaint]
      ,tbss.[percentageAssisted2pt]
      ,tbss.[percentageUnassisted2pt]
      ,tbss.[percentageAssisted3pt]
      ,tbss.[percentageUnassisted3pt]
      ,tbss.[percentageAssistedFGM]
      ,tbss.[percentageUnassistedFGM]

  FROM [nba_game_data].[dbo].[LeagueGameLog] lgl 

  LEFT OUTER JOIN [nba_game_data].[dbo].[TeamBoxScoreAdvancedV3] tbsa
  on lgl.GAME_ID = cast(tbsa.gameId as int)
  and lgl.TEAM_ID = tbsa.teamId

    LEFT OUTER JOIN [nba_game_data].[dbo].[TeamBoxScoreHustleV2] tbsh
  on lgl.GAME_ID = cast(tbsh.gameId as int)
  and lgl.TEAM_ID = tbsh.teamId

    LEFT OUTER JOIN [nba_game_data].[dbo].[TeamBoxScoreMiscV3] tbsm
  on lgl.GAME_ID = cast(tbsm.gameId as int)
  and lgl.TEAM_ID = tbsm.teamId

  LEFT OUTER JOIN [nba_game_data].[dbo].[TeamBoxScorePlayerTrackV3] tbst
  on lgl.GAME_ID = cast(tbst.gameId as int)
  and lgl.TEAM_ID = tbst.teamId

  LEFT OUTER JOIN [nba_game_data].[dbo].[TeamBoxScoreScoringV3] tbss
  on lgl.GAME_ID = cast(tbss.gameId as int)
  and lgl.TEAM_ID = tbss.teamId
  ),base2 as (
  select 
  TEAM_ABBREVIATION
	  ,AVG(CAST(FGM AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS FGMTeamLast{last_n_games}
	  ,AVG(CAST(FGA AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS FGATeamLast{last_n_games}
	  ,AVG(CAST(FG_PCT AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS FG_PCTTeamLast{last_n_games}
	  ,AVG(CAST(FG3M AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS FG3MTeamLast{last_n_games}
	  ,AVG(CAST(FG3A AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS FG3ATeamLast{last_n_games}
	  ,AVG(CAST(FG3_PCT AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS FG3_PCTTeamLast{last_n_games}
	  ,AVG(CAST(FTM AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS FTMTeamLast{last_n_games}
	  ,AVG(CAST(FTA AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS FTATeamLast{last_n_games}
	  ,AVG(CAST(FT_PCT AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS FT_PCTTeamLast{last_n_games}
	  ,AVG(CAST(OREB AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS OREBTeamLast{last_n_games}
	  ,AVG(CAST(DREB AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS DREBTeamLast{last_n_games}
	  ,AVG(CAST(REB AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS REBTeamLast{last_n_games}
	  ,AVG(CAST(AST AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS ASTTeamLast{last_n_games}
	  ,AVG(CAST(STL AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS STLTeamLast{last_n_games}
	  ,AVG(CAST(BLK AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS BLKTeamLast{last_n_games}
	  ,AVG(CAST(TOV AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS TOVTeamLast{last_n_games}
	  ,AVG(CAST(PF AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS PFTeamLast{last_n_games}
	  ,AVG(CAST(PTS AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS PTSTeamLast{last_n_games}
	  ,AVG(CAST(PLUS_MINUS AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS PLUS_MINUSTeamLast{last_n_games}
	  ,AVG(CAST(estimatedOffensiveRating AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS estimatedOffensiveRatingTeamLast{last_n_games}
	  ,AVG(CAST(offensiveRating AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS offensiveRatingTeamLast{last_n_games}
	  ,AVG(CAST(estimatedDefensiveRating AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS estimatedDefensiveRatingTeamLast{last_n_games}
	  ,AVG(CAST(defensiveRating AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS defensiveRatingTeamLast{last_n_games}
	  ,AVG(CAST(estimatedNetRating AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS estimatedNetRatingTeamLast{last_n_games}
	  ,AVG(CAST(netRating AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS netRatingTeamLast{last_n_games}
	  ,AVG(CAST(assistPercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS assistPercentageTeamLast{last_n_games}
	  ,AVG(CAST(assistToTurnover AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS assistToTurnoverTeamLast{last_n_games}
	  ,AVG(CAST(assistRatio AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS assistRatioTeamLast{last_n_games}
	  ,AVG(CAST(offensiveReboundPercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS offensiveReboundPercentageTeamLast{last_n_games}
	  ,AVG(CAST(defensiveReboundPercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS defensiveReboundPercentageTeamLast{last_n_games}
	  ,AVG(CAST(reboundPercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS reboundPercentageTeamLast{last_n_games}
	  ,AVG(CAST(estimatedTeamTurnoverPercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS estimatedTeamTurnoverPercentageTeamLast{last_n_games}
	  ,AVG(CAST(turnoverRatio AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS turnoverRatioTeamLast{last_n_games}
	  ,AVG(CAST(effectiveFieldGoalPercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS effectiveFieldGoalPercentageTeamLast{last_n_games}
	  ,AVG(CAST(trueShootingPercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS trueShootingPercentageTeamLast{last_n_games}
	  ,AVG(CAST(usagePercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS usagePercentageTeamLast{last_n_games}
	  ,AVG(CAST(estimatedUsagePercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS estimatedUsagePercentageTeamLast{last_n_games}
	  ,AVG(CAST(estimatedPace AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS estimatedPaceTeamLast{last_n_games}
	  ,AVG(CAST(pace AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS paceTeamLast{last_n_games}
	  ,AVG(CAST(pacePer40 AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS pacePer40TeamLast{last_n_games}
	  ,AVG(CAST(possessions AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS possessionsTeamLast{last_n_games}
	  ,AVG(CAST(PIE AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS PIETeamLast{last_n_games}
	  ,AVG(CAST(contestedShots AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS contestedShotsTeamLast{last_n_games}
	  ,AVG(CAST(contestedShots2pt AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS contestedShots2ptTeamLast{last_n_games}
	  ,AVG(CAST(contestedShots3pt AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS contestedShots3ptTeamLast{last_n_games}
	  ,AVG(CAST(deflections AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS deflectionsTeamLast{last_n_games}
	  ,AVG(CAST(chargesDrawn AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS chargesDrawnTeamLast{last_n_games}
	  ,AVG(CAST(screenAssists AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS screenAssistsTeamLast{last_n_games}
	  ,AVG(CAST(screenAssistPoints AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS screenAssistPointsTeamLast{last_n_games}
	  ,AVG(CAST(looseBallsRecoveredOffensive AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS looseBallsRecoveredOffensiveTeamLast{last_n_games}
	  ,AVG(CAST(looseBallsRecoveredDefensive AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS looseBallsRecoveredDefensiveTeamLast{last_n_games}
	  ,AVG(CAST(looseBallsRecoveredTotal AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS looseBallsRecoveredTotalTeamLast{last_n_games}
	  ,AVG(CAST(offensiveBoxOuts AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS offensiveBoxOutsTeamLast{last_n_games}
	  ,AVG(CAST(defensiveBoxOuts AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS defensiveBoxOutsTeamLast{last_n_games}
	  ,AVG(CAST(boxOutPlayerTeamRebounds AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS boxOutPlayerTeamReboundsTeamLast{last_n_games}
	  ,AVG(CAST(boxOutPlayerRebounds AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS boxOutPlayerReboundsTeamLast{last_n_games}
	  ,AVG(CAST(boxOuts AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS boxOutsTeamLast{last_n_games}
	  ,AVG(CAST(pointsOffTurnovers AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS pointsOffTurnoversTeamLast{last_n_games}
	  ,AVG(CAST(pointsSecondChance AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS pointsSecondChanceTeamLast{last_n_games}
	  ,AVG(CAST(pointsFastBreak AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS pointsFastBreakTeamLast{last_n_games}
	  ,AVG(CAST(pointsPaint AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS pointsPaintTeamLast{last_n_games}
	  ,AVG(CAST(oppPointsOffTurnovers AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS oppPointsOffTurnoversTeamLast{last_n_games}
	  ,AVG(CAST(oppPointsSecondChance AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS oppPointsSecondChanceTeamLast{last_n_games}
	  ,AVG(CAST(oppPointsFastBreak AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS oppPointsFastBreakTeamLast{last_n_games}
	  ,AVG(CAST(oppPointsPaint AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS oppPointsPaintTeamLast{last_n_games}
	  ,AVG(CAST(blocks AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS blocksTeamLast{last_n_games}
	  ,AVG(CAST(blocksAgainst AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS blocksAgainstTeamLast{last_n_games}
	  ,AVG(CAST(foulsPersonal AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS foulsPersonalTeamLast{last_n_games}
	  ,AVG(CAST(foulsDrawn AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS foulsDrawnTeamLast{last_n_games}
	  ,AVG(CAST(distance AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS distanceTeamLast{last_n_games}
	  ,AVG(CAST(reboundChancesOffensive AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS reboundChancesOffensiveTeamLast{last_n_games}
	  ,AVG(CAST(reboundChancesDefensive AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS reboundChancesDefensiveTeamLast{last_n_games}
	  ,AVG(CAST(reboundChancesTotal AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS reboundChancesTotalTeamLast{last_n_games}
	  ,AVG(CAST(touches AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS touchesTeamLast{last_n_games}
	  ,AVG(CAST(secondaryAssists AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS secondaryAssistsTeamLast{last_n_games}
	  ,AVG(CAST(freeThrowAssists AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS freeThrowAssistsTeamLast{last_n_games}
	  ,AVG(CAST(passes AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS passesTeamLast{last_n_games}
	  ,AVG(CAST(assists AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS assistsTeamLast{last_n_games}
	  ,AVG(CAST(contestedFieldGoalsMade AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS contestedFieldGoalsMadeTeamLast{last_n_games}
	  ,AVG(CAST(contestedFieldGoalsAttempted AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS contestedFieldGoalsAttemptedTeamLast{last_n_games}
	  ,AVG(CAST(contestedFieldGoalPercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS contestedFieldGoalPercentageTeamLast{last_n_games}
	  ,AVG(CAST(uncontestedFieldGoalsMade AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS uncontestedFieldGoalsMadeTeamLast{last_n_games}
	  ,AVG(CAST(uncontestedFieldGoalsAttempted AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS uncontestedFieldGoalsAttemptedTeamLast{last_n_games}
	  ,AVG(CAST(uncontestedFieldGoalsPercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS uncontestedFieldGoalsPercentageTeamLast{last_n_games}
	  ,AVG(CAST(fieldGoalPercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS fieldGoalPercentageTeamLast{last_n_games}
	  ,AVG(CAST(defendedAtRimFieldGoalsMade AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS defendedAtRimFieldGoalsMadeTeamLast{last_n_games}
	  ,AVG(CAST(defendedAtRimFieldGoalsAttempted AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS defendedAtRimFieldGoalsAttemptedTeamLast{last_n_games}
	  ,AVG(CAST(defendedAtRimFieldGoalPercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS defendedAtRimFieldGoalPercentageTeamLast{last_n_games}
	  ,AVG(CAST(percentageFieldGoalsAttempted2pt AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS percentageFieldGoalsAttempted2ptTeamLast{last_n_games}
	  ,AVG(CAST(percentageFieldGoalsAttempted3pt AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS percentageFieldGoalsAttempted3ptTeamLast{last_n_games}
	  ,AVG(CAST(percentagePoints2pt AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS percentagePoints2ptTeamLast{last_n_games}
	  ,AVG(CAST(percentagePointsMidrange2pt AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS percentagePointsMidrange2ptTeamLast{last_n_games}
	  ,AVG(CAST(percentagePoints3pt AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS percentagePoints3ptTeamLast{last_n_games}
	  ,AVG(CAST(percentagePointsFastBreak AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS percentagePointsFastBreakTeamLast{last_n_games}
	  ,AVG(CAST(percentagePointsFreeThrow AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS percentagePointsFreeThrowTeamLast{last_n_games}
	  ,AVG(CAST(percentagePointsOffTurnovers AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS percentagePointsOffTurnoversTeamLast{last_n_games}
	  ,AVG(CAST(percentagePointsPaint AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS percentagePointsPaintTeamLast{last_n_games}
	  ,AVG(CAST(percentageAssisted2pt AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS percentageAssisted2ptTeamLast{last_n_games}
	  ,AVG(CAST(percentageUnassisted2pt AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS percentageUnassisted2ptTeamLast{last_n_games}
	  ,AVG(CAST(percentageAssisted3pt AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS percentageAssisted3ptTeamLast{last_n_games}
	  ,AVG(CAST(percentageUnassisted3pt AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS percentageUnassisted3ptTeamLast{last_n_games}
	  ,AVG(CAST(percentageAssistedFGM AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS percentageAssistedFGMTeamLast{last_n_games}
	  ,AVG(CAST(percentageUnassistedFGM AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND CURRENT ROW) AS percentageUnassistedFGMTeamLast{last_n_games}
    , rank() over (Partition by TEAM_ID order by GAME_DATE desc) as ranking
  FROM base)
  
      select * from base2 where ranking=1 and TRIM(TEAM_ABBREVIATION) = '{opp}'
    '''
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';')
    cursor = cnxn.cursor()
    data = pd.read_sql(sql,cnxn)
    data = data.drop(columns = ['TEAM_ABBREVIATION','ranking'])
    df = pd.concat([df,data], axis=1 ).bfill().ffill()
    return df


def get_player_last_n_data(player_id, last_n_games):
    server = 'localhost\SQLEXPRESS'
    database = 'nba_game_data'
    sql = f'''
    
      SELECT 
    [PLAYER_ID]
    ,[GAME_ID]
    ,oppAbrv
    

    ---last x games data ---
    ,AVG(CAST(FGM AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FGMLast{last_n_games}
    ,AVG(CAST(FGA AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FGALast{last_n_games}
    ,AVG(CAST(FG_PCT AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FG_PCTLast{last_n_games}
    ,AVG(CAST(FG3M AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FG3MLast{last_n_games}
    ,AVG(CAST(FG3A AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FG3ALast{last_n_games}
    ,AVG(CAST(FG3_PCT AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FG3_PCTLast{last_n_games}
    ,AVG(CAST(FTM AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FTMLast{last_n_games}
    ,AVG(CAST(FTA AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FTALast{last_n_games}
    ,AVG(CAST(FT_PCT AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FT_PCTLast{last_n_games}
    ,AVG(CAST(OREB AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS OREBLast{last_n_games}
    ,AVG(CAST(DREB AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS DREBLast{last_n_games}
    ,AVG(CAST(REB AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS REBLast{last_n_games}
    ,AVG(CAST(AST AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS ASTLast{last_n_games}
    ,AVG(CAST(TOV AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS TOVLast{last_n_games}
    ,AVG(CAST(STL AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS STLLast{last_n_games}
    ,AVG(CAST(BLK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS BLKLast{last_n_games}
    ,AVG(CAST(BLKA AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS BLKALast{last_n_games}
    ,AVG(CAST(PF AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS PFLast{last_n_games}
    ,AVG(CAST(PFD AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS PFDLast{last_n_games}
    ,AVG(CAST(PTS AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS PTSLast{last_n_games}
    ,AVG(CAST(PLUS_MINUS AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS PLUS_MINUSLast{last_n_games}
    ,AVG(CAST(NBA_FANTASY_PTS AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS NBA_FANTASY_PTSLast{last_n_games}
    ,AVG(CAST(DD2 AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS DD2Last{last_n_games}
    ,AVG(CAST(TD3 AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS TD3Last{last_n_games}
    ,AVG(CAST(WNBA_FANTASY_PTS AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS WNBA_FANTASY_PTSLast{last_n_games}
    ,AVG(CAST(GP_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS GP_RANKLast{last_n_games}
    ,AVG(CAST(W_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS W_RANKLast{last_n_games}
    ,AVG(CAST(L_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS L_RANKLast{last_n_games}
    ,AVG(CAST(W_PCT_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS W_PCT_RANKLast{last_n_games}
    ,AVG(CAST(MIN_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS MIN_RANKLast{last_n_games}
    ,AVG(CAST(FGM_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FGM_RANKLast{last_n_games}
    ,AVG(CAST(FGA_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FGA_RANKLast{last_n_games}
    ,AVG(CAST(FG_PCT_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FG_PCT_RANKLast{last_n_games}
    ,AVG(CAST(FG3M_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FG3M_RANKLast{last_n_games}
    ,AVG(CAST(FG3A_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FG3A_RANKLast{last_n_games}
    ,AVG(CAST(FG3_PCT_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FG3_PCT_RANKLast{last_n_games}
    ,AVG(CAST(FTM_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FTM_RANKLast{last_n_games}
    ,AVG(CAST(FTA_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FTA_RANKLast{last_n_games}
    ,AVG(CAST(FT_PCT_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FT_PCT_RANKLast{last_n_games}
    ,AVG(CAST(OREB_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS OREB_RANKLast{last_n_games}
    ,AVG(CAST(DREB_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS DREB_RANKLast{last_n_games}
    ,AVG(CAST(REB_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS REB_RANKLast{last_n_games}
    ,AVG(CAST(AST_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS AST_RANKLast{last_n_games}
    ,AVG(CAST(TOV_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS TOV_RANKLast{last_n_games}
    ,AVG(CAST(STL_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS STL_RANKLast{last_n_games}
    ,AVG(CAST(BLK_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS BLK_RANKLast{last_n_games}
    ,AVG(CAST(BLKA_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS BLKA_RANKLast{last_n_games}
    ,AVG(CAST(PF_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS PF_RANKLast{last_n_games}
    ,AVG(CAST(PFD_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS PFD_RANKLast{last_n_games}
    ,AVG(CAST(PTS_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS PTS_RANKLast{last_n_games}
    ,AVG(CAST(PLUS_MINUS_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS PLUS_MINUS_RANKLast{last_n_games}
    ,AVG(CAST(NBA_FANTASY_PTS_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS NBA_FANTASY_PTS_RANKLast{last_n_games}
    ,AVG(CAST(DD2_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS DD2_RANKLast{last_n_games}
    ,AVG(CAST(TD3_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS TD3_RANKLast{last_n_games}
    ,AVG(CAST(WNBA_FANTASY_PTS_RANK AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS WNBA_FANTASY_PTS_RANKLast{last_n_games}
    ,AVG(CAST(estimatedOffensiveRating AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS estimatedOffensiveRatingLast{last_n_games}
    ,AVG(CAST(offensiveRating AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS offensiveRatingLast{last_n_games}
    ,AVG(CAST(estimatedDefensiveRating AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS estimatedDefensiveRatingLast{last_n_games}
    ,AVG(CAST(defensiveRating AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS defensiveRatingLast{last_n_games}
    ,AVG(CAST(estimatedNetRating AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS estimatedNetRatingLast{last_n_games}
    ,AVG(CAST(netRating AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS netRatingLast{last_n_games}
    ,AVG(CAST(assistPercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS assistPercentageLast{last_n_games}
    ,AVG(CAST(assistToTurnover AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS assistToTurnoverLast{last_n_games}
    ,AVG(CAST(assistRatio AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS assistRatioLast{last_n_games}
    ,AVG(CAST(offensiveReboundPercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS offensiveReboundPercentageLast{last_n_games}
    ,AVG(CAST(defensiveReboundPercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS defensiveReboundPercentageLast{last_n_games}
    ,AVG(CAST(reboundPercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS reboundPercentageLast{last_n_games}
    ,AVG(CAST(turnoverRatio AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS turnoverRatioLast{last_n_games}
    ,AVG(CAST(effectiveFieldGoalPercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS effectiveFieldGoalPercentageLast{last_n_games}
    ,AVG(CAST(trueShootingPercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS trueShootingPercentageLast{last_n_games}
    ,AVG(CAST(usagePercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS usagePercentageLast{last_n_games}
    ,AVG(CAST(estimatedUsagePercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS estimatedUsagePercentageLast{last_n_games}
    ,AVG(CAST(estimatedPace AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS estimatedPaceLast{last_n_games}
    ,AVG(CAST(pace AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS paceLast{last_n_games}
    ,AVG(CAST(pacePer40 AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS pacePer40Last{last_n_games}
    ,AVG(CAST(possessions AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS possessionsLast{last_n_games}
    ,AVG(CAST(PIE AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS PIELast{last_n_games}
    ,AVG(CAST(matchupMinutes AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS matchupMinutesLast{last_n_games}
    ,AVG(CAST(partialPossessions AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS partialPossessionsLast{last_n_games}
    ,AVG(CAST(switchesOn AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS switchesOnLast{last_n_games}
    ,AVG(CAST(playerPoints AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS playerPointsLast{last_n_games}
    ,AVG(CAST(defensiveRebounds AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS defensiveReboundsLast{last_n_games}
    ,AVG(CAST(matchupAssists AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS matchupAssistsLast{last_n_games}
    ,AVG(CAST(matchupTurnovers AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS matchupTurnoversLast{last_n_games}
    ,AVG(CAST(matchupFieldGoalsMade AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS matchupFieldGoalsMadeLast{last_n_games}
    ,AVG(CAST(matchupFieldGoalsAttempted AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS matchupFieldGoalsAttemptedLast{last_n_games}
    ,AVG(CAST(matchupFieldGoalPercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS matchupFieldGoalPercentageLast{last_n_games}
    ,AVG(CAST(matchupThreePointersMade AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS matchupThreePointersMadeLast{last_n_games}
    ,AVG(CAST(matchupThreePointersAttempted AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS matchupThreePointersAttemptedLast{last_n_games}
    ,AVG(CAST(matchupThreePointerPercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS matchupThreePointerPercentageLast{last_n_games}
    ,AVG(CAST(contestedShots AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS contestedShotsLast{last_n_games}
    ,AVG(CAST(contestedShots2pt AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS contestedShots2ptLast{last_n_games}
    ,AVG(CAST(contestedShots3pt AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS contestedShots3ptLast{last_n_games}
    ,AVG(CAST(deflections AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS deflectionsLast{last_n_games}
    ,AVG(CAST(chargesDrawn AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS chargesDrawnLast{last_n_games}
    ,AVG(CAST(screenAssists AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS screenAssistsLast{last_n_games}
    ,AVG(CAST(screenAssistPoints AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS screenAssistPointsLast{last_n_games}
    ,AVG(CAST(looseBallsRecoveredOffensive AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS looseBallsRecoveredOffensiveLast{last_n_games}
    ,AVG(CAST(looseBallsRecoveredDefensive AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS looseBallsRecoveredDefensiveLast{last_n_games}
    ,AVG(CAST(looseBallsRecoveredTotal AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS looseBallsRecoveredTotalLast{last_n_games}
    ,AVG(CAST(offensiveBoxOuts AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS offensiveBoxOutsLast{last_n_games}
    ,AVG(CAST(defensiveBoxOuts AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS defensiveBoxOutsLast{last_n_games}
    ,AVG(CAST(boxOutPlayerTeamRebounds AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS boxOutPlayerTeamReboundsLast{last_n_games}
    ,AVG(CAST(boxOutPlayerRebounds AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS boxOutPlayerReboundsLast{last_n_games}
    ,AVG(CAST(boxOuts AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS boxOutsLast{last_n_games}
    ,AVG(CAST(pointsOffTurnovers AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS pointsOffTurnoversLast{last_n_games}
    ,AVG(CAST(pointsSecondChance AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS pointsSecondChanceLast{last_n_games}
    ,AVG(CAST(pointsFastBreak AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS pointsFastBreakLast{last_n_games}
    ,AVG(CAST(pointsPaint AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS pointsPaintLast{last_n_games}
    ,AVG(CAST(oppPointsOffTurnovers AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS oppPointsOffTurnoversLast{last_n_games}
    ,AVG(CAST(oppPointsSecondChance AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS oppPointsSecondChanceLast{last_n_games}
    ,AVG(CAST(oppPointsFastBreak AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS oppPointsFastBreakLast{last_n_games}
    ,AVG(CAST(oppPointsPaint AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS oppPointsPaintLast{last_n_games}
    ,AVG(CAST(blocksAgainst AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS blocksAgainstLast{last_n_games}
    ,AVG(CAST(foulsPersonal AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS foulsPersonalLast{last_n_games}
    ,AVG(CAST(foulsDrawn AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS foulsDrawnLast{last_n_games}
    ,AVG(CAST(percentageFieldGoalsAttempted2pt AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentageFieldGoalsAttempted2ptLast{last_n_games}
    ,AVG(CAST(percentageFieldGoalsAttempted3pt AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentageFieldGoalsAttempted3ptLast{last_n_games}
    ,AVG(CAST(percentagePoints2pt AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentagePoints2ptLast{last_n_games}
    ,AVG(CAST(percentagePointsMidrange2pt AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentagePointsMidrange2ptLast{last_n_games}
    ,AVG(CAST(percentagePoints3pt AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentagePoints3ptLast{last_n_games}
    ,AVG(CAST(percentagePointsFastBreak AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentagePointsFastBreakLast{last_n_games}
    ,AVG(CAST(percentagePointsFreeThrow AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentagePointsFreeThrowLast{last_n_games}
    ,AVG(CAST(percentagePointsOffTurnovers AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentagePointsOffTurnoversLast{last_n_games}
    ,AVG(CAST(percentagePointsPaint AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentagePointsPaintLast{last_n_games}
    ,AVG(CAST(percentageAssisted2pt AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentageAssisted2ptLast{last_n_games}
    ,AVG(CAST(percentageUnassisted2pt AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentageUnassisted2ptLast{last_n_games}
    ,AVG(CAST(percentageAssisted3pt AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentageAssisted3ptLast{last_n_games}
    ,AVG(CAST(percentageUnassisted3pt AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentageUnassisted3ptLast{last_n_games}
    ,AVG(CAST(percentageAssistedFGM AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentageAssistedFGMLast{last_n_games}
    ,AVG(CAST(percentageUnassistedFGM AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentageUnassistedFGMLast{last_n_games}
    ,AVG(CAST(speed AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS speedLast{last_n_games}
    ,AVG(CAST(distance AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS distanceLast{last_n_games}
    ,AVG(CAST(reboundChancesOffensive AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS reboundChancesOffensiveLast{last_n_games}
    ,AVG(CAST(reboundChancesDefensive AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS reboundChancesDefensiveLast{last_n_games}
    ,AVG(CAST(reboundChancesTotal AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS reboundChancesTotalLast{last_n_games}
    ,AVG(CAST(touches AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS touchesLast{last_n_games}
    ,AVG(CAST(secondaryAssists AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS secondaryAssistsLast{last_n_games}
    ,AVG(CAST(freeThrowAssists AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS freeThrowAssistsLast{last_n_games}
    ,AVG(CAST(passes AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS passesLast{last_n_games}
    ,AVG(CAST(assists AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS assistsLast{last_n_games}
    ,AVG(CAST(contestedFieldGoalsMade AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS contestedFieldGoalsMadeLast{last_n_games}
    ,AVG(CAST(contestedFieldGoalsAttempted AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS contestedFieldGoalsAttemptedLast{last_n_games}
    ,AVG(CAST(contestedFieldGoalPercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS contestedFieldGoalPercentageLast{last_n_games}
    ,AVG(CAST(uncontestedFieldGoalsMade AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS uncontestedFieldGoalsMadeLast{last_n_games}
    ,AVG(CAST(uncontestedFieldGoalsAttempted AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS uncontestedFieldGoalsAttemptedLast{last_n_games}
    ,AVG(CAST(uncontestedFieldGoalsPercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS uncontestedFieldGoalsPercentageLast{last_n_games}
    ,AVG(CAST(fieldGoalPercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS fieldGoalPercentageLast{last_n_games}
    ,AVG(CAST(defendedAtRimFieldGoalsMade AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS defendedAtRimFieldGoalsMadeLast{last_n_games}
    ,AVG(CAST(defendedAtRimFieldGoalsAttempted AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS defendedAtRimFieldGoalsAttemptedLast{last_n_games}
    ,AVG(CAST(defendedAtRimFieldGoalPercentage AS FLOAT)) OVER (PARTITION BY PLAYER_ID ORDER BY GAME_DATE ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS defendedAtRimFieldGoalPercentageLast{last_n_games}


      FROM BasePlayer
      WHERE PLAYER_ID = '{player_id}'
      order by GAME_DATE

    '''
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';')
    cursor = cnxn.cursor()
    df = pd.read_sql(sql,cnxn)
    
    sql = f'''with base as (
    SELECT  
	   lgl.[SEASON_ID] as SEASON_YEAR
      ,lgl.[TEAM_ID]
      ,lgl.[TEAM_ABBREVIATION]
      ,lgl.[TEAM_NAME]
      ,lgl.[GAME_ID]
      ,lgl.[GAME_DATE]
	  ,lgl.[oppAbrv]
	  ,lgl.[yearSeason]
      ,lgl.[MATCHUP]
      ,lgl.[WL]
      ,lgl.[MIN]
      ,lgl.[FGM]
      ,lgl.[FGA]
      ,lgl.[FG_PCT]
      ,lgl.[FG3M]
      ,lgl.[FG3A]
      ,lgl.[FG3_PCT]
      ,lgl.[FTM]
      ,lgl.[FTA]
      ,lgl.[FT_PCT]
      ,lgl.[OREB]
      ,lgl.[DREB]
      ,lgl.[REB]
      ,lgl.[AST]
      ,lgl.[STL]
      ,lgl.[BLK]
      ,lgl.[TOV]
      ,lgl.[PF]
      ,lgl.[PTS]
      ,lgl.[PLUS_MINUS]

	  ,tbsa.[estimatedOffensiveRating]
      ,tbsa.[offensiveRating]
      ,tbsa.[estimatedDefensiveRating]
      ,tbsa.[defensiveRating]
      ,tbsa.[estimatedNetRating]
      ,tbsa.[netRating]
      ,tbsa.[assistPercentage]
      ,tbsa.[assistToTurnover]
      ,tbsa.[assistRatio]
      ,tbsa.[offensiveReboundPercentage]
      ,tbsa.[defensiveReboundPercentage]
      ,tbsa.[reboundPercentage]
      ,tbsa.[estimatedTeamTurnoverPercentage]
      ,tbsa.[turnoverRatio]
      ,tbsa.[effectiveFieldGoalPercentage]
      ,tbsa.[trueShootingPercentage]
      ,tbsa.[usagePercentage]
      ,tbsa.[estimatedUsagePercentage]
      ,tbsa.[estimatedPace]
      ,tbsa.[pace]
      ,tbsa.[pacePer40]
      ,tbsa.[possessions]
      ,tbsa.[PIE]

	  ,tbsh.[contestedShots]
      ,tbsh.[contestedShots2pt]
      ,tbsh.[contestedShots3pt]
      ,tbsh.[deflections]
      ,tbsh.[chargesDrawn]
      ,tbsh.[screenAssists]
      ,tbsh.[screenAssistPoints]
      ,tbsh.[looseBallsRecoveredOffensive]
      ,tbsh.[looseBallsRecoveredDefensive]
      ,tbsh.[looseBallsRecoveredTotal]
      ,tbsh.[offensiveBoxOuts]
      ,tbsh.[defensiveBoxOuts]
      ,tbsh.[boxOutPlayerTeamRebounds]
      ,tbsh.[boxOutPlayerRebounds]
      ,tbsh.[boxOuts]

	  ,tbsm.[pointsOffTurnovers]
      ,tbsm.[pointsSecondChance]
      ,tbsm.[pointsFastBreak]
      ,tbsm.[pointsPaint]
      ,tbsm.[oppPointsOffTurnovers]
      ,tbsm.[oppPointsSecondChance]
      ,tbsm.[oppPointsFastBreak]
      ,tbsm.[oppPointsPaint]
      ,tbsm.[blocks]
      ,tbsm.[blocksAgainst]
      ,tbsm.[foulsPersonal]
      ,tbsm.[foulsDrawn]

	  ,tbst.[distance]
      ,tbst.[reboundChancesOffensive]
      ,tbst.[reboundChancesDefensive]
      ,tbst.[reboundChancesTotal]
      ,tbst.[touches]
      ,tbst.[secondaryAssists]
      ,tbst.[freeThrowAssists]
      ,tbst.[passes]
      ,tbst.[assists]
      ,tbst.[contestedFieldGoalsMade]
      ,tbst.[contestedFieldGoalsAttempted]
      ,tbst.[contestedFieldGoalPercentage]
      ,tbst.[uncontestedFieldGoalsMade]
      ,tbst.[uncontestedFieldGoalsAttempted]
      ,tbst.[uncontestedFieldGoalsPercentage]
      ,tbst.[fieldGoalPercentage]
      ,tbst.[defendedAtRimFieldGoalsMade]
      ,tbst.[defendedAtRimFieldGoalsAttempted]
      ,tbst.[defendedAtRimFieldGoalPercentage]

      ,tbss.[percentageFieldGoalsAttempted2pt]
      ,tbss.[percentageFieldGoalsAttempted3pt]
      ,tbss.[percentagePoints2pt]
      ,tbss.[percentagePointsMidrange2pt]
      ,tbss.[percentagePoints3pt]
      ,tbss.[percentagePointsFastBreak]
      ,tbss.[percentagePointsFreeThrow]
      ,tbss.[percentagePointsOffTurnovers]
      ,tbss.[percentagePointsPaint]
      ,tbss.[percentageAssisted2pt]
      ,tbss.[percentageUnassisted2pt]
      ,tbss.[percentageAssisted3pt]
      ,tbss.[percentageUnassisted3pt]
      ,tbss.[percentageAssistedFGM]
      ,tbss.[percentageUnassistedFGM]

  FROM [nba_game_data].[dbo].[LeagueGameLog] lgl 

  LEFT OUTER JOIN [nba_game_data].[dbo].[TeamBoxScoreAdvancedV3] tbsa
  on lgl.GAME_ID = cast(tbsa.gameId as int)
  and lgl.TEAM_ID = tbsa.teamId

    LEFT OUTER JOIN [nba_game_data].[dbo].[TeamBoxScoreHustleV2] tbsh
  on lgl.GAME_ID = cast(tbsh.gameId as int)
  and lgl.TEAM_ID = tbsh.teamId

    LEFT OUTER JOIN [nba_game_data].[dbo].[TeamBoxScoreMiscV3] tbsm
  on lgl.GAME_ID = cast(tbsm.gameId as int)
  and lgl.TEAM_ID = tbsm.teamId

  LEFT OUTER JOIN [nba_game_data].[dbo].[TeamBoxScorePlayerTrackV3] tbst
  on lgl.GAME_ID = cast(tbst.gameId as int)
  and lgl.TEAM_ID = tbst.teamId

  LEFT OUTER JOIN [nba_game_data].[dbo].[TeamBoxScoreScoringV3] tbss
  on lgl.GAME_ID = cast(tbss.gameId as int)
  and lgl.TEAM_ID = tbss.teamId
  )
  select 
	   [TEAM_ABBREVIATION]
      ,[GAME_ID]
	  ,AVG(CAST(FGM AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FGMTeamLast{last_n_games}
	  ,AVG(CAST(FGA AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FGATeamLast{last_n_games}
	  ,AVG(CAST(FG_PCT AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FG_PCTTeamLast{last_n_games}
	  ,AVG(CAST(FG3M AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FG3MTeamLast{last_n_games}
	  ,AVG(CAST(FG3A AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FG3ATeamLast{last_n_games}
	  ,AVG(CAST(FG3_PCT AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FG3_PCTTeamLast{last_n_games}
	  ,AVG(CAST(FTM AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FTMTeamLast{last_n_games}
	  ,AVG(CAST(FTA AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FTATeamLast{last_n_games}
	  ,AVG(CAST(FT_PCT AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS FT_PCTTeamLast{last_n_games}
	  ,AVG(CAST(OREB AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS OREBTeamLast{last_n_games}
	  ,AVG(CAST(DREB AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS DREBTeamLast{last_n_games}
	  ,AVG(CAST(REB AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS REBTeamLast{last_n_games}
	  ,AVG(CAST(AST AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS ASTTeamLast{last_n_games}
	  ,AVG(CAST(STL AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS STLTeamLast{last_n_games}
	  ,AVG(CAST(BLK AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS BLKTeamLast{last_n_games}
	  ,AVG(CAST(TOV AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS TOVTeamLast{last_n_games}
	  ,AVG(CAST(PF AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS PFTeamLast{last_n_games}
	  ,AVG(CAST(PTS AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS PTSTeamLast{last_n_games}
	  ,AVG(CAST(PLUS_MINUS AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS PLUS_MINUSTeamLast{last_n_games}
	  ,AVG(CAST(estimatedOffensiveRating AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS estimatedOffensiveRatingTeamLast{last_n_games}
	  ,AVG(CAST(offensiveRating AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS offensiveRatingTeamLast{last_n_games}
	  ,AVG(CAST(estimatedDefensiveRating AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS estimatedDefensiveRatingTeamLast{last_n_games}
	  ,AVG(CAST(defensiveRating AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS defensiveRatingTeamLast{last_n_games}
	  ,AVG(CAST(estimatedNetRating AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS estimatedNetRatingTeamLast{last_n_games}
	  ,AVG(CAST(netRating AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS netRatingTeamLast{last_n_games}
	  ,AVG(CAST(assistPercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS assistPercentageTeamLast{last_n_games}
	  ,AVG(CAST(assistToTurnover AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS assistToTurnoverTeamLast{last_n_games}
	  ,AVG(CAST(assistRatio AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS assistRatioTeamLast{last_n_games}
	  ,AVG(CAST(offensiveReboundPercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS offensiveReboundPercentageTeamLast{last_n_games}
	  ,AVG(CAST(defensiveReboundPercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS defensiveReboundPercentageTeamLast{last_n_games}
	  ,AVG(CAST(reboundPercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS reboundPercentageTeamLast{last_n_games}
	  ,AVG(CAST(estimatedTeamTurnoverPercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS estimatedTeamTurnoverPercentageTeamLast{last_n_games}
	  ,AVG(CAST(turnoverRatio AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS turnoverRatioTeamLast{last_n_games}
	  ,AVG(CAST(effectiveFieldGoalPercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS effectiveFieldGoalPercentageTeamLast{last_n_games}
	  ,AVG(CAST(trueShootingPercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS trueShootingPercentageTeamLast{last_n_games}
	  ,AVG(CAST(usagePercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS usagePercentageTeamLast{last_n_games}
	  ,AVG(CAST(estimatedUsagePercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS estimatedUsagePercentageTeamLast{last_n_games}
	  ,AVG(CAST(estimatedPace AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS estimatedPaceTeamLast{last_n_games}
	  ,AVG(CAST(pace AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS paceTeamLast{last_n_games}
	  ,AVG(CAST(pacePer40 AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS pacePer40TeamLast{last_n_games}
	  ,AVG(CAST(possessions AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS possessionsTeamLast{last_n_games}
	  ,AVG(CAST(PIE AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS PIETeamLast{last_n_games}
	  ,AVG(CAST(contestedShots AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS contestedShotsTeamLast{last_n_games}
	  ,AVG(CAST(contestedShots2pt AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS contestedShots2ptTeamLast{last_n_games}
	  ,AVG(CAST(contestedShots3pt AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS contestedShots3ptTeamLast{last_n_games}
	  ,AVG(CAST(deflections AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS deflectionsTeamLast{last_n_games}
	  ,AVG(CAST(chargesDrawn AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS chargesDrawnTeamLast{last_n_games}
	  ,AVG(CAST(screenAssists AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS screenAssistsTeamLast{last_n_games}
	  ,AVG(CAST(screenAssistPoints AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS screenAssistPointsTeamLast{last_n_games}
	  ,AVG(CAST(looseBallsRecoveredOffensive AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS looseBallsRecoveredOffensiveTeamLast{last_n_games}
	  ,AVG(CAST(looseBallsRecoveredDefensive AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS looseBallsRecoveredDefensiveTeamLast{last_n_games}
	  ,AVG(CAST(looseBallsRecoveredTotal AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS looseBallsRecoveredTotalTeamLast{last_n_games}
	  ,AVG(CAST(offensiveBoxOuts AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS offensiveBoxOutsTeamLast{last_n_games}
	  ,AVG(CAST(defensiveBoxOuts AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS defensiveBoxOutsTeamLast{last_n_games}
	  ,AVG(CAST(boxOutPlayerTeamRebounds AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS boxOutPlayerTeamReboundsTeamLast{last_n_games}
	  ,AVG(CAST(boxOutPlayerRebounds AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS boxOutPlayerReboundsTeamLast{last_n_games}
	  ,AVG(CAST(boxOuts AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS boxOutsTeamLast{last_n_games}
	  ,AVG(CAST(pointsOffTurnovers AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS pointsOffTurnoversTeamLast{last_n_games}
	  ,AVG(CAST(pointsSecondChance AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS pointsSecondChanceTeamLast{last_n_games}
	  ,AVG(CAST(pointsFastBreak AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS pointsFastBreakTeamLast{last_n_games}
	  ,AVG(CAST(pointsPaint AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS pointsPaintTeamLast{last_n_games}
	  ,AVG(CAST(oppPointsOffTurnovers AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS oppPointsOffTurnoversTeamLast{last_n_games}
	  ,AVG(CAST(oppPointsSecondChance AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS oppPointsSecondChanceTeamLast{last_n_games}
	  ,AVG(CAST(oppPointsFastBreak AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS oppPointsFastBreakTeamLast{last_n_games}
	  ,AVG(CAST(oppPointsPaint AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS oppPointsPaintTeamLast{last_n_games}
	  ,AVG(CAST(blocks AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS blocksTeamLast{last_n_games}
	  ,AVG(CAST(blocksAgainst AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS blocksAgainstTeamLast{last_n_games}
	  ,AVG(CAST(foulsPersonal AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS foulsPersonalTeamLast{last_n_games}
	  ,AVG(CAST(foulsDrawn AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS foulsDrawnTeamLast{last_n_games}
	  ,AVG(CAST(distance AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS distanceTeamLast{last_n_games}
	  ,AVG(CAST(reboundChancesOffensive AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS reboundChancesOffensiveTeamLast{last_n_games}
	  ,AVG(CAST(reboundChancesDefensive AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS reboundChancesDefensiveTeamLast{last_n_games}
	  ,AVG(CAST(reboundChancesTotal AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS reboundChancesTotalTeamLast{last_n_games}
	  ,AVG(CAST(touches AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS touchesTeamLast{last_n_games}
	  ,AVG(CAST(secondaryAssists AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS secondaryAssistsTeamLast{last_n_games}
	  ,AVG(CAST(freeThrowAssists AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS freeThrowAssistsTeamLast{last_n_games}
	  ,AVG(CAST(passes AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS passesTeamLast{last_n_games}
	  ,AVG(CAST(assists AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS assistsTeamLast{last_n_games}
	  ,AVG(CAST(contestedFieldGoalsMade AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS contestedFieldGoalsMadeTeamLast{last_n_games}
	  ,AVG(CAST(contestedFieldGoalsAttempted AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS contestedFieldGoalsAttemptedTeamLast{last_n_games}
	  ,AVG(CAST(contestedFieldGoalPercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS contestedFieldGoalPercentageTeamLast{last_n_games}
	  ,AVG(CAST(uncontestedFieldGoalsMade AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS uncontestedFieldGoalsMadeTeamLast{last_n_games}
	  ,AVG(CAST(uncontestedFieldGoalsAttempted AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS uncontestedFieldGoalsAttemptedTeamLast{last_n_games}
	  ,AVG(CAST(uncontestedFieldGoalsPercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS uncontestedFieldGoalsPercentageTeamLast{last_n_games}
	  ,AVG(CAST(fieldGoalPercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS fieldGoalPercentageTeamLast{last_n_games}
	  ,AVG(CAST(defendedAtRimFieldGoalsMade AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS defendedAtRimFieldGoalsMadeTeamLast{last_n_games}
	  ,AVG(CAST(defendedAtRimFieldGoalsAttempted AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS defendedAtRimFieldGoalsAttemptedTeamLast{last_n_games}
	  ,AVG(CAST(defendedAtRimFieldGoalPercentage AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS defendedAtRimFieldGoalPercentageTeamLast{last_n_games}
	  ,AVG(CAST(percentageFieldGoalsAttempted2pt AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentageFieldGoalsAttempted2ptTeamLast{last_n_games}
	  ,AVG(CAST(percentageFieldGoalsAttempted3pt AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentageFieldGoalsAttempted3ptTeamLast{last_n_games}
	  ,AVG(CAST(percentagePoints2pt AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentagePoints2ptTeamLast{last_n_games}
	  ,AVG(CAST(percentagePointsMidrange2pt AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentagePointsMidrange2ptTeamLast{last_n_games}
	  ,AVG(CAST(percentagePoints3pt AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentagePoints3ptTeamLast{last_n_games}
	  ,AVG(CAST(percentagePointsFastBreak AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentagePointsFastBreakTeamLast{last_n_games}
	  ,AVG(CAST(percentagePointsFreeThrow AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentagePointsFreeThrowTeamLast{last_n_games}
	  ,AVG(CAST(percentagePointsOffTurnovers AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentagePointsOffTurnoversTeamLast{last_n_games}
	  ,AVG(CAST(percentagePointsPaint AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentagePointsPaintTeamLast{last_n_games}
	  ,AVG(CAST(percentageAssisted2pt AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentageAssisted2ptTeamLast{last_n_games}
	  ,AVG(CAST(percentageUnassisted2pt AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentageUnassisted2ptTeamLast{last_n_games}
	  ,AVG(CAST(percentageAssisted3pt AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentageAssisted3ptTeamLast{last_n_games}
	  ,AVG(CAST(percentageUnassisted3pt AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentageUnassisted3ptTeamLast{last_n_games}
	  ,AVG(CAST(percentageAssistedFGM AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentageAssistedFGMTeamLast{last_n_games}
	  ,AVG(CAST(percentageUnassistedFGM AS FLOAT)) OVER (PARTITION BY TEAM_ID,  SEASON_YEAR  ORDER BY GAME_DATE  ROWS BETWEEN {last_n_games} PRECEDING AND 1 PRECEDING) AS percentageUnassistedFGMTeamLast{last_n_games}

      FROM base
      '''
    team = pd.read_sql(sql,cnxn)

    return df.merge(team, left_on=['oppAbrv','GAME_ID'], right_on=['TEAM_ABBREVIATION','GAME_ID'])








def find_best_threshold(probabilities, true_labels):
    best_threshold = 0.5
    best_accuracy = 0
    for threshold in np.linspace(0, 1, 100):
        predictions = np.where(probabilities > threshold, 1, 0)
        accuracy = accuracy_score(true_labels, predictions)
        if accuracy > best_accuracy:
            best_accuracy = accuracy
            best_threshold = threshold
    return best_threshold


def perform_hyper_param_tuning_bayes_lr(final_X, final_y, seed):
    def objective(params):
        # Unpack the parameters
        params = {
            'C': params['C'],
            'penalty': ['l2', 'none'][params['penalty']],
            'solver': ['lbfgs', 'saga', 'newton-cg'][params['solver']],
            'class_weight': ['balanced', None][params['class_weight']],
            'fit_intercept': [True, False][params['fit_intercept']],
            'tol': params['tol'],
            'max_iter': params['max_iter']
        }

        # Create a Logistic Regression model with given parameters
        lr = LogisticRegression(**params, random_state=seed)

        # Using Stratified K-Fold cross-validation
        cv = LeaveOneOut()

        # Evaluate the model using cross-validation and return the mean F1 score
        f1_scores = cross_val_score(lr, final_X, final_y, cv=cv,scoring='precision', n_jobs=-1)
        mean_f1 = np.mean(f1_scores)

        # Hyperopt tries to minimize the objective, so return the negative F1 score
        return {'loss': -mean_f1, 'status': STATUS_OK}

    # Define the enhanced parameter space for Logistic Regression
    search_space = {
        'C': hp.loguniform('C', np.log(1e-6), np.log(1e+6)),
        'penalty': hp.choice('penalty', range(2)),
        'solver': hp.choice('solver', range(3)),
        'class_weight': hp.choice('class_weight', range(2)),
        'fit_intercept': hp.choice('fit_intercept', range(2)),
        'tol': hp.loguniform('tol', np.log(1e-6), np.log(1e-3)),
        'max_iter': hp.choice('max_iter', range(100, 1001))  # Range of integers
    }

    # Perform the hyperparameter tuning
    trials = Trials()
    best_params = fmin(fn=objective,
                       space=search_space,
                       algo=tpe.suggest,
                       max_evals=200,  # Adjust the number of evaluations
                       trials=trials)

    # Map indices to actual parameters
    best_params_mapped = {
        'C': best_params['C'],
        'penalty': ['l2', 'none'][best_params['penalty']],
        'solver': ['lbfgs', 'saga', 'newton-cg'][best_params['solver']],
        'class_weight': ['balanced', None][best_params['class_weight']],
        'fit_intercept': [True, False][best_params['fit_intercept']],
        'tol': best_params['tol'],
        'max_iter': best_params['max_iter']
    }

    # Extracting the best score
    best_score = -min([trial['result']['loss'] for trial in trials.trials])
   

    return best_params_mapped, best_score



def perform_hyper_param_tuning_bayes_nb(final_X, final_y, seed):
    def objective(params):
        # Create a GaussianNB model with given parameters
        gnb = GaussianNB(**params)

        # Using Stratified K-Fold cross-validation
        cv = LeaveOneOut()

        # Evaluate the model using cross-validation and return the mean F1 score
        f1_scores = cross_val_score(gnb, final_X, final_y, cv=cv,scoring='precision', n_jobs=-1)
        mean_f1 = np.mean(f1_scores)

        # Hyperopt tries to minimize the objective, so return the negative F1 score
        return {'loss': -mean_f1, 'status': STATUS_OK}

    # Define the parameter space for GaussianNB
    search_space = {
        'var_smoothing': hp.loguniform('var_smoothing', np.log(1e-10), np.log(1e-1))
    }

    # Perform the hyperparameter tuning
    trials = Trials()
    best_params = fmin(fn=objective,
                       space=search_space,
                       algo=tpe.suggest,
                       max_evals=350,  # Adjust the number of evaluations
                       trials=trials)

    # Extracting the best score
    best_score = -min([trial['result']['loss'] for trial in trials.trials])

    
    return best_params, best_score

def perform_hyper_param_tuning_bayes_dt(final_X, final_y,seed):
    def objective(params):
        # Create a DecisionTreeClassifier model with given parameters
        dt = DecisionTreeClassifier(**params, random_state=seed)

        # Using Stratified K-Fold cross-validation
        cv = LeaveOneOut()

        # Evaluate the model using cross-validation and return the mean F1 score
        f1_scores = cross_val_score(dt, final_X, final_y, cv=cv, n_jobs=-1)
        mean_f1 = np.mean(f1_scores)

        # Hyperopt tries to minimize the objective, so return the negative F1 score
        return {'loss': -mean_f1, 'status': STATUS_OK}

    # Define the parameter space for DecisionTreeClassifier
    search_space = {
        'max_depth': hp.choice('max_depth', range(1, 15)),
        'min_samples_split': hp.uniform('min_samples_split', 0.1, 1.0),
        'min_samples_leaf': hp.uniform('min_samples_leaf', 0.1, 0.5),
        'criterion': hp.choice('criterion', ['gini', 'entropy'])
    }

    # Perform the hyperparameter tuning
    trials = Trials()
    best_params = fmin(fn=objective,
                       space=search_space,
                       algo=tpe.suggest,
                       max_evals=100,  # Adjust the number of evaluations
                       trials=trials)
    maps = ['gini', 'entropy']
    best_params['criterion'] = maps[best_params['criterion']]
    # Extracting the best score
    best_score = -min([trial['result']['loss'] for trial in trials.trials])


    return best_params, best_score

def perform_hyper_param_tuning_bayes_rf(final_X, final_y, seed):
    def objective(params):
        # Create a RandomForestClassifier model with given parameters
        rf = RandomForestClassifier(**params, random_state=seed, n_jobs=-1)
        # Using Leave-One-Out cross-validation
        cv = StratifiedKFold(n_splits=round(len(final_X)/2), random_state = seed, shuffle  = True)

        # Evaluate the model using cross-validation and return the mean ROC AUC score
        roc_auc_scores = cross_val_score(rf, final_X, final_y, cv=cv,scoring='precision', n_jobs=-1)
        mean_roc_auc = np.mean(roc_auc_scores)
        
        # Hyperopt tries to minimize the objective, so return the negative ROC AUC score
        return {'loss': -mean_roc_auc, 'status': STATUS_OK}


    # Define the parameter space for RandomForestClassifier
    search_space = {
        'n_estimators': hp.choice('n_estimators', range(10, 1001)),
        'max_depth': hp.choice('max_depth', range(1, 15)),
        'min_samples_split': hp.uniform('min_samples_split', 0.1, 1.0),
        'min_samples_leaf': hp.uniform('min_samples_leaf', 0.1, 0.5),
        'max_features': hp.choice('max_features', ['auto', 'sqrt', 'log2', None])
    }

    # Perform the hyperparameter tuning
    trials = Trials()
    best_params = fmin(fn=objective,
                       space=search_space,
                       algo=tpe.suggest,
                       max_evals=100,  # Adjust the number of evaluations
                       trials=trials)

    # Extracting the best score
    best_score = -min([trial['result']['loss'] for trial in trials.trials])

    
    return best_params, best_score

def perform_hyper_param_tuning_bayes_xgb(final_X, final_y, seed):
    def objective(params):
        # Create an XGBClassifier model with given parameters
        xgb = XGBClassifier(**params)

        # Using Stratified K-Fold cross-validation
        cv = LeaveOneOut()

        # Evaluate the model using cross-validation and return the mean F1 score
        f1_scores = cross_val_score(xgb, final_X, final_y, cv=cv,scoring='precision', n_jobs=-1)
        mean_f1 = np.mean(f1_scores)

        # Hyperopt tries to minimize the objective, so return the negative F1 score
        return {'loss': -mean_f1, 'status': STATUS_OK}

    # Define the parameter space for XGBClassifier
    search_space = {
        'n_estimators': hp.choice('n_estimators', range(50, 1001)),
        'max_depth': hp.choice('max_depth', range(3, 15)),
        'learning_rate': hp.uniform('learning_rate', 0.01, 0.3),
        'subsample': hp.uniform('subsample', 0.7, 1.0),
        'colsample_bytree': hp.uniform('colsample_bytree', 0.7, 1.0),
        'gamma': hp.uniform('gamma', 0.0, 5.0)
    }

    # Perform the hyperparameter tuning
    trials = Trials()
    best_params = fmin(fn=objective,
                       space=search_space,
                       algo=tpe.suggest,
                       max_evals=100,  # Adjust the number of evaluations
                       trials=trials)

    # Extracting the best score
    best_score = -min([trial['result']['loss'] for trial in trials.trials])

    return best_params, best_score

def perform_hyper_param_tuning_bayes_catboost(final_X, final_y,seed):
    def objective(params):
        # Create a CatBoostClassifier model with given parameters
        catboost = CatBoostClassifier(**params, random_seed=seed, verbose=0,thread_count=-1)

        # Using Stratified K-Fold cross-validation
        cv = StratifiedKFold(n_splits=round(len(final_X)/2), random_state = seed, shuffle  = True)

        # Evaluate the model using cross-validation and return the mean F1 score
        f1_scores = cross_val_score(catboost, final_X, final_y, cv=cv,scoring='precision', n_jobs=-1)
        mean_f1 = np.mean(f1_scores)

        # Hyperopt tries to minimize the objective, so return the negative F1 score
        return {'loss': -mean_f1, 'status': STATUS_OK}

    # Define the parameter space for CatBoostClassifier
    search_space = {
        'iterations': hp.choice('iterations', range(50, 1001)),
        'depth': hp.choice('depth', range(4, 11)),
        'learning_rate': hp.uniform('learning_rate', 0.01, 0.3),
        'l2_leaf_reg': hp.uniform('l2_leaf_reg', 1, 10),
        'border_count': hp.choice('border_count', range(32, 255)),
    }

    # Perform the hyperparameter tuning
    trials = Trials()
    best_params = fmin(fn=objective,
                       space=search_space,
                       algo=tpe.suggest,
                       max_evals=100,  # Adjust the number of evaluations
                       trials=trials)

    # Extracting the best score
    best_score = -min([trial['result']['loss'] for trial in trials.trials])

    return best_params, best_score


def perform_hyper_param_tuning_bayes_lgbm(final_X, final_y,seed):
    def objective(params):
        # Create an LGBMClassifier model with given parameters
        lgbm = LGBMClassifier(**params, random_state=seed)

        # Using Stratified K-Fold cross-validation
        cv = StratifiedKFold(n_splits=10, random_state = seed, shuffle  = True)

        # Evaluate the model using cross-validation and return the mean F1 score
        f1_scores = cross_val_score(lgbm, final_X, final_y, cv=cv,scoring='precision',  n_jobs=-1)
        mean_f1 = np.mean(f1_scores)

        # Hyperopt tries to minimize the objective, so return the negative F1 score
        return {'loss': -mean_f1, 'status': STATUS_OK}

    # Define the parameter space for LGBMClassifier
    search_space = {
        'num_leaves': hp.choice('num_leaves', range(20, 150)),
        'max_depth': hp.choice('max_depth', range(-1, 16)),  # -1 for no limit
        'learning_rate': hp.uniform('learning_rate', 0.01, 0.3),
        'n_estimators': hp.choice('n_estimators', range(50, 1001)),
        'subsample': hp.uniform('subsample', 0.5, 1.0),
        'colsample_bytree': hp.uniform('colsample_bytree', 0.5, 1.0),
    }

    # Perform the hyperparameter tuning
    trials = Trials()
    best_params = fmin(fn=objective,
                       space=search_space,
                       algo=tpe.suggest,
                       max_evals=2,  # Adjust the number of evaluations
                       trials=trials)

    # Extracting the best score
    best_score = -min([trial['result']['loss'] for trial in trials.trials])

    return best_params, best_score

def perform_hyper_param_tuning_bayes_svc(final_X, final_y,seed):
    def objective(params):
        # Create an SVC model with given parameters
        svc = SVC(**params, random_state=seed)

        # Using Stratified K-Fold cross-validation
        cv = LeaveOneOut()

        # Evaluate the model using cross-validation and return the mean F1 score
        f1_scores = cross_val_score(svc, final_X, final_y, cv=cv,scoring='precision', n_jobs=-1)
        mean_f1 = np.mean(f1_scores)

        # Hyperopt tries to minimize the objective, so return the negative F1 score
        return {'loss': -mean_f1, 'status': STATUS_OK}

    # Define the parameter space for SVC
    search_space = {
        'C': hp.loguniform('C', np.log(1e-3), np.log(1e3)),
        'kernel': hp.choice('kernel', ['linear', 'sigmoid']),
        'gamma': hp.loguniform('gamma', np.log(1e-4), np.log(1e1))
    }

    # Perform the hyperparameter tuning
    trials = Trials()
    best_params = fmin(fn=objective,
                       space=search_space,
                       algo=tpe.suggest,
                       max_evals=100,  # Adjust the number of evaluations
                       trials=trials)
    maps = ['linear', 'rbf', 'poly', 'sigmoid']
    best_params['kernel'] = maps[best_params['kernel']]
    # Extracting the best score
    best_score = -min([trial['result']['loss'] for trial in trials.trials])

    return best_params, best_score

def perform_hyper_param_tuning_knn(final_X, final_y, seed):
    def objective(params):
        # Create a KNeighborsClassifier model with given parameters
        knn = KNeighborsClassifier(**params)

        # Using Stratified K-Fold cross-validation
        cv = LeaveOneOut()

        # Evaluate the model using cross-validation and return the mean F1 score
        f1_scores = cross_val_score(knn, final_X, final_y, cv=cv,scoring='precision',  n_jobs=-1)
        mean_f1 = np.mean(f1_scores)

        # Hyperopt tries to minimize the objective, so return the negative F1 score
        return {'loss': -mean_f1, 'status': STATUS_OK}

    # Define the parameter space for KNN
    search_space = {
        'n_neighbors': hp.choice('n_neighbors', range(1, 30)),  # Number of neighbors
        'metric': hp.choice('metric', ['euclidean', 'manhattan', 'minkowski']),  # Distance metric
        'weights': hp.choice('weights', ['uniform', 'distance'])  # Weighting function
    }

    # Perform the hyperparameter tuning
    trials = Trials()
    best_params = fmin(fn=objective,
                       space=search_space,
                       algo=tpe.suggest,
                       max_evals=250,  # Adjust the number of evaluations
                       trials=trials)
    best_params['metric'] = ['euclidean', 'manhattan', 'minkowski'][best_params['metric']]
    best_params['weights'] = ['uniform', 'distance'][best_params['weights']]

    # Extracting the best score
    best_score = -min([trial['result']['loss'] for trial in trials.trials])

    return best_params, best_score

def perform_hyper_param_tuning_bayes_mlp(final_X, final_y,seed):
    def objective(params):
        # Create an MLPClassifier model with given parameters
        mlp = MLPClassifier(**params, max_iter=1000, random_state=seed)

        # Using Stratified K-Fold cross-validation
        cv = StratifiedKFold(n_splits=8,random_state = seed, shuffle  = True)

        # Evaluate the model using cross-validation and return the mean F1 score
        f1_scores = cross_val_score(mlp, final_X, final_y, cv=cv,scoring='precision', n_jobs=-1)
        mean_f1 = np.mean(f1_scores)

        # Hyperopt tries to minimize the objective, so return the negative F1 score
        return {'loss': -mean_f1, 'status': STATUS_OK}

    # Define the parameter space for MLPClassifier
    search_space = {
        'hidden_layer_sizes': hp.choice('hidden_layer_sizes', [(50,), (100,), (50, 50), (100, 50)]),
        'activation': hp.choice('activation', ['tanh', 'relu']),
        'solver': hp.choice('solver', ['sgd', 'adam']),
        'alpha': hp.loguniform('alpha', np.log(1e-4), np.log(1e-2)),
        'learning_rate_init': hp.loguniform('learning_rate_init', np.log(1e-4), np.log(1e-1)),
    }

    # Perform the hyperparameter tuning
    trials = Trials()
    best_params = fmin(fn=objective,
                       space=search_space,
                       algo=tpe.suggest,
                       max_evals=100,  # Adjust the number of evaluations
                       trials=trials)

    # Extracting the best score
    best_score = -min([trial['result']['loss'] for trial in trials.trials])



def perform_hyper_param_tuning_bayes_sgd(final_X, final_y, seed):
    def objective(params):
        # Unpack the parameters
        params = {
            'alpha': params['alpha'],
            'penalty': ['l2', 'l1', 'elasticnet'][params['penalty']],
            'fit_intercept': [True, False][params['fit_intercept']],
            'tol': params['tol'],
            'max_iter': params['max_iter']
        }

        # Create an SGD Classifier model with given parameters
        sgd = SGDClassifier(**params, random_state=seed)

        # Using Stratified K-Fold cross-validation
        cv = LeaveOneOut()

        # Evaluate the model using cross-validation and return the mean F1 score
        f1_scores = cross_val_score(sgd, final_X, final_y, cv=cv, scoring='precision', n_jobs=-1)
        mean_f1 = np.mean(f1_scores)

        # Hyperopt tries to minimize the objective, so return the negative F1 score
        return {'loss': -mean_f1, 'status': STATUS_OK}

    # Define the enhanced parameter space for SGD Classifier
    search_space = {
        'alpha': hp.loguniform('alpha', np.log(1e-6), np.log(1e-2)),
        'penalty': hp.choice('penalty', range(3)),
        'fit_intercept': hp.choice('fit_intercept', range(2)),
        'tol': hp.loguniform('tol', np.log(1e-6), np.log(1e-3)),
        'max_iter': hp.choice('max_iter', range(100, 1001))  # Range of integers
    }

    # Perform the hyperparameter tuning
    trials = Trials()
    best_params = fmin(fn=objective,
                       space=search_space,
                       algo=tpe.suggest,
                       max_evals=200,  # Adjust the number of evaluations
                       trials=trials)

    # Map indices to actual parameters
    best_params_mapped = {
        'alpha': best_params['alpha'],
        'penalty': ['l2', 'l1', 'elasticnet'][best_params['penalty']],
        'fit_intercept': [True, False][best_params['fit_intercept']],
        'tol': best_params['tol'],
        'max_iter': best_params['max_iter']
    }

    # Extracting the best score
    best_score = -min([trial['result']['loss'] for trial in trials.trials])
    print("Best F1 Score:", best_score)

    return best_params_mapped, best_score

