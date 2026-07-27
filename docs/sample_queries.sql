-- ST5011CEM sample queries
-- All application queries use bound parameters (?) rather than
-- string concatenation, preventing SQL injection.

-- Operator compliance league table
SELECT op.operator_name                                  AS operator,
           COUNT(DISTINCT r.route_id)                        AS routes_operated,
           COUNT(*)                                          AS observations,
           ROUND(AVG(o.delay_min), 3)                        AS mean_delay_min,
           ROUND(100.0*SUM(o.on_time_2min)/COUNT(*), 2)      AS reliability_pct,
           CASE WHEN 100.0*SUM(o.on_time_2min)/COUNT(*) >= 85
                THEN 'COMPLIANT' ELSE 'BELOW THRESHOLD' END  AS status
    FROM delay_observations o
    JOIN routes    r  ON o.route_id    = r.route_id
    JOIN operators op ON r.operator_id = op.operator_id
    GROUP BY op.operator_name
    HAVING COUNT(*) >= ?
    ORDER BY reliability_pct DESC

-- Stops with highest mean delay
SELECT s.stop_id,
           ROUND(s.latitude, 5)                  AS lat,
           ROUND(s.longitude, 5)                 AS lon,
           COUNT(*)                              AS observations,
           ROUND(AVG(o.delay_min), 3)            AS mean_delay_min
    FROM delay_observations o
    JOIN stops s ON o.stop_pk = s.stop_pk
    GROUP BY s.stop_id, s.latitude, s.longitude
    HAVING COUNT(*) >= ?
    ORDER BY mean_delay_min DESC
    LIMIT ?

-- Network performance by hour
SELECT CAST(strftime('%H', observed_at) AS INTEGER)      AS hour,
           COUNT(*)                                          AS observations,
           ROUND(AVG(delay_min), 3)                          AS mean_delay_min,
           ROUND(100.0*SUM(on_time_2min)/COUNT(*), 2)        AS reliability_pct
    FROM delay_observations
    GROUP BY hour
    ORDER BY hour

