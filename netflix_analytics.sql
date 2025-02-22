-- Daily, Weekly, and Monthly Active Users (DAU, WAU, MAU)

-- Daily Active Users (DAU)
SELECT DATE(last_watched) AS watch_date, COUNT(DISTINCT profile_id) AS active_users
FROM watch_history
GROUP BY DATE(last_watched)
ORDER BY watch_date DESC;




-- Weekly Active Users (WAU)
SELECT TO_CHAR(last_watched, 'IYYY-IW') AS week, COUNT(DISTINCT profile_id) AS active_users
FROM watch_history
GROUP BY TO_CHAR(last_watched, 'IYYY-IW')
ORDER BY week DESC;




-- Monthly Active Users (MAU)
SELECT TO_CHAR(last_watched, 'YYYY-MM') AS month, COUNT(DISTINCT profile_id) AS active_users
FROM watch_history
GROUP BY TO_CHAR(last_watched, 'YYYY-MM')
ORDER BY month DESC;



-- Watch Time Per User
-- We need to calculate the total minutes watched per user.
WITH total_content AS (
    select content_id, duration from movies_shows UNION ALL select content_id, duration from episodes
),
content_duration AS (
    select content_id, sum(duration) as duration from total_content group by content_id
)
SELECT profile_id, SUM(c.duration * (w.progress_percentage/100)) AS total_watch_time
FROM watch_history as w
JOIN content_duration as c
ON w.content_id = c.content_id
GROUP BY profile_id
ORDER BY total_watch_time DESC;

-- Most-Watched Movies & TV Shows
-- We count how many times each content_id has been watched.
SELECT content_id, COUNT(*) AS watch_count
FROM watch_history
GROUP BY content_id
ORDER BY watch_count DESC
LIMIT 10;




-- If we want the most-watched genres, we need to join with the content table.
SELECT c.genre_id, COUNT(w.content_id) AS watch_count
FROM watch_history w
JOIN content_genres c ON w.content_id = c.content_id
GROUP BY c.genre_id
ORDER BY watch_count DESC
LIMIT 5;




-- Churn Rate
-- Churn rate is the percentage of users who canceled their subscription.
WITH total_users AS (
    SELECT COUNT(DISTINCT user_id)::NUMERIC AS total FROM subscriptions
),
churned_users AS (
    SELECT COUNT(DISTINCT user_id)::NUMERIC AS churned
    FROM subscriptions
    WHERE end_date < CURRENT_DATE AND end_date >= (CURRENT_DATE - INTERVAL '30 days')
)
SELECT 
    (churned/ total)*100 AS churn_rate
FROM churned_users, total_users;





-- Trending Content (Last 7 Days)
--If we want to find top trending content, we filter watch history for the last 7 days.
SELECT c.title, COUNT(w.profile_id) AS watch_count
FROM watch_history w
JOIN movies_shows c ON w.content_id = c.content_id
WHERE w.last_watched >= (CURRENT_DATE - INTERVAL '7 days')
GROUP BY c.title
ORDER BY watch_count DESC
LIMIT 10;
