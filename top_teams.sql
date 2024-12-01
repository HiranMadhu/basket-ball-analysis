-- Create database
CREATE DATABASE IF NOT EXISTS game_data;

-- Use the game_data database
USE game_data;

-- Create the table for the game data
CREATE TABLE IF NOT EXISTS game_events (
    event_id STRING,
    event_num STRING,
    game_id STRING,
    home_description STRING,
    pctimestring STRING,
    period INT,
    player1_id STRING,
    player1_name STRING,
    player1_team_abbreviation STRING,
    player1_team_city STRING,
    player1_team_id STRING,
    player1_team_nickname STRING,
    player2_id STRING,
    player2_name STRING,
    player2_team_abbreviation STRING,
    player2_team_city STRING,
    player2_team_id STRING,
    player2_team_nickname STRING,
    player3_id STRING,
    player3_name STRING,
    player3_team_abbreviation STRING,
    player3_team_city STRING,
    player3_team_id STRING,
    player3_team_nickname STRING,
    score STRING, 
    score_margin STRING, 
    visitor_description STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

-- Load data into the table 
LOAD DATA LOCAL INPATH '/opt/dataset' INTO TABLE game_events;

-- Add new columns for home_score and visitor_score
ALTER TABLE game_events ADD COLUMNS (home_score INT, visitor_score INT);

INSERT OVERWRITE TABLE game_events
SELECT
    event_id,
    event_num,
    game_id,
    home_description,
    pctimestring,
    period,
    player1_id,
    player1_name,
    player1_team_abbreviation,
    player1_team_city,
    player1_team_id,
    player1_team_nickname,
    player2_id,
    player2_name,
    player2_team_abbreviation,
    player2_team_city,
    player2_team_id,
    player2_team_nickname,
    player3_id,
    player3_name,
    player3_team_abbreviation,
    player3_team_city,
    player3_team_id,
    player3_team_nickname,
    score,
    score_margin,
    visitor_description,
    CASE 
        WHEN score IS NULL OR score = '' THEN NULL
        WHEN LENGTH(SPLIT(score, '-')[0]) > 0 THEN CAST(TRIM(SPLIT(score, '-')[0]) AS INT)
        ELSE NULL
    END AS home_score,
    CASE 
        WHEN score IS NULL OR score = '' THEN NULL
        WHEN LENGTH(SPLIT(score, '-')[1]) > 0 THEN CAST(TRIM(SPLIT(score, '-')[1]) AS INT)
        ELSE NULL
    END AS visitor_score 
FROM game_events;

WITH final_scores AS (
    SELECT 
        game_id,
        -- Get the home and visitor team names from the first non-null record
        FIRST_VALUE(player1_team_nickname) OVER (PARTITION BY game_id ORDER BY event_id DESC) AS home_team,
        FIRST_VALUE(player2_team_nickname) OVER (PARTITION BY game_id ORDER BY event_id DESC) AS visitor_team,
        -- Get the home and visitor scores from the last record (final score)
        FIRST_VALUE(home_score) OVER (PARTITION BY game_id ORDER BY event_id DESC) AS home_score,
        FIRST_VALUE(visitor_score) OVER (PARTITION BY game_id ORDER BY event_id DESC) AS visitor_score
    FROM game_events
    WHERE home_score IS NOT NULL AND visitor_score IS NOT NULL
),
team_points AS (
    SELECT home_team AS team_name, SUM(home_score) AS total_points
    FROM final_scores
    WHERE home_team IS NOT NULL AND home_team != ''
    GROUP BY home_team
    UNION ALL
    SELECT visitor_team AS team_name, SUM(visitor_score) AS total_points
    FROM final_scores
    WHERE visitor_team IS NOT NULL AND visitor_team != ''
    GROUP BY visitor_team
)

SELECT team_name, SUM(total_points) AS total_points
FROM team_points
GROUP BY team_name
ORDER BY total_points DESC
LIMIT 5;