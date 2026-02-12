WITH session_pairs AS (
    SELECT 
        charger_name,
        port_id,
        timestamp,
        port_status,
        LEAD(timestamp) OVER (PARTITION BY charger_name, port_id ORDER BY timestamp) AS next_timestamp,
        LEAD(port_status) OVER (PARTITION BY charger_name, port_id ORDER BY timestamp) AS next_status
    FROM charger_data
)
SELECT 
    charger_name,
    port_id,
    timestamp AS session_start,
    next_timestamp AS session_end,
    (julianday(next_timestamp) - julianday(timestamp)) * 24 * 60 AS duration_minutes
FROM session_pairs
WHERE port_status = 'SESSION' 
    AND next_status = 'AVAILABLE';

