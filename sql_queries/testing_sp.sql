
--EXEC seasonPtsDataSet @pts_thresh = 13.5, @player_id = 203076, @line_type = 'REB'

--EXEC seasonPtsDataSet @pts_thresh = 13.5, @player_id = 203076, @line_type = N'REB+PTS';
--EXEC seasonPtsDataSetPred @pts_thresh = 37.5, @player_id = 1628389, @line_type = N'REB',@opp ='BOS';
--Line: 37.5, PLAYER_ID: 1628389.0, line_Type: PTS+REB+AST

EXEC seasonPtsDataSetByCluster @pts_thresh = 13.5, @cluster_id = 9, @line_type = 'REB',@cluster_type ='REB';

--seasonPtsDataSetByCluster
