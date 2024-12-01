-- Use the game_data database
USE game_data;

-- Step 1: Get the final scores for each game and quarter
WITH final_scores_per_period AS (
    SELECT 
        game_id,
        period,
        MAX(home_score) AS home_score,       
        MAX(visitor_score) AS visitor_score  
    FROM game_events
    WHERE home_score IS NOT NULL AND visitor_score IS NOT NULL
    GROUP BY game_id, period
),

-- Step 2: Calculate the points scored in each quarter
points_per_period AS (
    SELECT 
        game_id,
        period,
        home_score - COALESCE(LAG(home_score) OVER (PARTITION BY game_id ORDER BY period), 0) AS home_points,
        visitor_score - COALESCE(LAG(visitor_score) OVER (PARTITION BY game_id ORDER BY period), 0) AS visitor_points
    FROM final_scores_per_period
),

-- Step 3: Sum the points scored per period across all games
total_scores AS (
    SELECT 
        period,
        SUM(home_points + visitor_points) AS total_points,
        COUNT(DISTINCT game_id) AS num_games 
    FROM points_per_period
    GROUP BY period
)

-- Step 4: Calculate the average points scored per quarter
SELECT 
    period AS quarter,
    total_points,
    num_games,
    CAST(total_points / num_games AS INT) AS average_points 
FROM total_scores
ORDER BY quarter;
