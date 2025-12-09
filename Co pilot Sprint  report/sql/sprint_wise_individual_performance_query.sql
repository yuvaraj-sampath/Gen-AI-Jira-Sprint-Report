/*
SPRINT-WISE INDIVIDUAL PERFORMANCE METRICS QUERY
Calculates metrics for each team member in each sprint separately.
*/

-- Get all current sprints
WITH recent_sprint AS (
  SELECT 
    sprint_name,
    sprint_start_date,
    sprint_end_date
  FROM 
    `vz-it-np-j0nv-dev-odpr-0.od_dq.jira_capacity_planning_v3`
  WHERE 
    sprint_end_date > CURRENT_TIMESTAMP()
),

-- Get latest record per issue for each sprint
base_data AS (
  SELECT 
    bd.sprint_name,
    bd.sprint_start_date,
    bd.sprint_end_date,
    bd.assignee,
    bd.vzid,
    bd.manager,
    bd.email,
    bd.issue_key,
    CAST(bd.story_points AS FLOAT64) AS story_points,
    CAST(MAX(sp.total_capacity_story_points) AS FLOAT64) AS total_capacity_story_points,
    bd.status,
    bd.priority,
    bd.issue_type,
    bd.created,
    ROW_NUMBER() OVER ( PARTITION BY bd.issue_key, bd.sprint_name ORDER BY bd.execution_timestamp DESC ) as rn 
  FROM 
    `vz-it-np-j0nv-dev-odpr-0.od_dq.jira_capacity_planning_v3` bd
  INNER JOIN 
    recent_sprint rs 
  ON 
    bd.sprint_name = rs.sprint_name
  INNER JOIN 
    `vz-it-np-j0nv-dev-odpr-0.od_dq.sprint_planning_sheet` sp
  ON
    TRIM(UPPER(bd.vzid)) = TRIM(UPPER(sp.vzid))
    AND bd.sprint_name = sp.current_sprint
  WHERE 
    bd.backlog = 'N'
  GROUP BY 
    bd.sprint_name,
    bd.sprint_start_date,
    bd.sprint_end_date,
    bd.assignee,
    bd.vzid,
    bd.manager,
    bd.email,
    bd.issue_key,
    story_points,
    bd.status,
    bd.priority,
    bd.issue_type,
    bd.created,
    bd.execution_timestamp
)

-- SPRINT-WISE INDIVIDUAL PERFORMANCE METRICS
SELECT
  sprint_name,
  assignee,
  email,
  manager,
  MAX(total_capacity_story_points) AS individual_capacity,
  SUM(story_points) AS assigned_points,
  COUNT(DISTINCT issue_key) AS total_issues_count,
  COUNTIF(status IN ('Done', 'APPROVE DEFINITION OF DONE')) AS completed_issues_count,
  ROUND(
    CASE WHEN SUM(story_points) > 0 THEN (COUNTIF(status IN ('Done', 'APPROVE DEFINITION OF DONE')) / SUM(story_points)) * 100 ELSE 0 END, 2
  ) AS completion_rate,
  COUNTIF(priority IN ('Highest', 'Very High', 'High')) AS high_priority_count,
  COUNTIF(priority = 'Medium') AS medium_priority_count,
  COUNTIF(priority IN('Lowest', 'Low')) AS low_priority_count,
  COUNTIF(issue_type = 'VZAgile Story') AS stories_count,
  COUNTIF(issue_type = 'Task') AS tasks_count,
  COUNTIF(issue_type = 'Bug') AS bugs_count,
  COUNTIF(issue_type NOT IN ('VZAgile Story', 'Task', 'Bug')) AS others_count
FROM base_data
WHERE rn = 1
GROUP BY sprint_name, assignee, email, manager
ORDER BY sprint_name, assignee