/*
MANAGER-WISE METRICS QUERY
Calculates metrics for each manager, across all current sprints (not per sprint).
*/

-- Get all current sprints
WITH current_sprints AS (
  SELECT 
    sprint_name,
    sprint_start_date,
    sprint_end_date
  FROM 
    `vz-it-np-j0nv-dev-odpr-0.od_dq.jira_capacity_planning_v3`
  WHERE 
    sprint_end_date > CURRENT_TIMESTAMP()
),

-- Get latest record per issue for all current sprints
base_data_with_manager AS (
  SELECT 
    bd.sprint_name,
    bd.sprint_start_date,
    bd.sprint_end_date,
    bd.assignee,
    bd.vzid,
    sp.manager,
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
    current_sprints cs 
  ON 
    bd.sprint_name = cs.sprint_name
  INNER JOIN 
    `vz-it-np-j0nv-dev-odpr-0.od_dq.sprint_planning_sheet` sp
  ON
    TRIM(UPPER(bd.vzid)) = TRIM(UPPER(sp.vzid))
    AND bd.sprint_name = sp.current_sprint
    AND bd.manager = sp.manager
  WHERE 
    bd.backlog = 'N'
  GROUP BY 
    bd.sprint_name,
    bd.sprint_start_date,
    bd.sprint_end_date,
    bd.assignee,
    bd.vzid,
    sp.manager,
    bd.email,
    bd.issue_key,
    story_points,
    bd.status,
    bd.priority,
    bd.issue_type,
    bd.created,
    bd.execution_timestamp
)

-- MANAGER-WISE METRICS
SELECT
  manager,
  -- Team Capacity: Sum of all individual capacities for the manager across all current sprints
  (SELECT SUM(total_capacity_story_points)
   FROM `vz-it-np-j0nv-dev-odpr-0.od_dq.sprint_planning_sheet` sp
   WHERE sp.manager = base_data_with_manager.manager
     AND sp.current_sprint IN (SELECT sprint_name FROM current_sprints)) AS team_capacity,

  -- Assigned Points: Total story points assigned to the manager's team across all sprints
  SUM(story_points) AS assigned_points,

  -- Team Members Count: Distinct assignees under this manager across all sprints
  COUNT(DISTINCT assignee) AS team_members_count,

  -- Total Issues Count: All issues assigned to this manager's team across all sprints
  COUNT(DISTINCT issue_key) AS total_issues_count,

  -- Completed Issues Count
  COUNTIF(status IN ('Done', 'APPROVE DEFINITION OF DONE')) AS completed_issues_count,

  -- Priority Breakdown
  COUNTIF(priority IN ('Highest', 'Very High', 'High')) AS high_priority_count,
  COUNTIF(priority = 'Medium') AS medium_priority_count,
  COUNTIF(priority IN('Lowest', 'Low')) AS low_priority_count,

  -- Issue Type Distribution
  COUNTIF(issue_type = 'VZAgile Story') AS stories_count,
  COUNTIF(issue_type = 'Task') AS tasks_count,
  COUNTIF(issue_type = 'Bug') AS bugs_count,
  COUNTIF(issue_type NOT IN ('VZAgile Story', 'Task', 'Bug')) AS others_count,

  -- Team Efficiency: (Completed Issues / Assigned Points) * 100
  ROUND(
    CASE 
      WHEN SUM(story_points) > 0 THEN (COUNTIF(status IN ('Done', 'APPROVE DEFINITION OF DONE')) / SUM(story_points)) * 100
      ELSE 0 
    END, 2
  ) AS team_completion_rate,

  -- Team Capacity Utilization: (Assigned Points / Team Capacity) * 100
  ROUND(
    CASE 
      WHEN (SELECT SUM(total_capacity_story_points) FROM `vz-it-np-j0nv-dev-odpr-0.od_dq.sprint_planning_sheet` sp WHERE sp.manager = base_data_with_manager.manager AND sp.current_sprint IN (SELECT sprint_name FROM current_sprints)) > 0 
        THEN (SUM(story_points) / (SELECT SUM(total_capacity_story_points) FROM `vz-it-np-j0nv-dev-odpr-0.od_dq.sprint_planning_sheet` sp WHERE sp.manager = base_data_with_manager.manager AND sp.current_sprint IN (SELECT sprint_name FROM current_sprints))) * 100
      ELSE 0 
    END, 2
  ) AS team_capacity_utilization,

  -- Overallocated/Underallocated/Balanced
  CASE 
    WHEN SUM(story_points) > (SELECT SUM(total_capacity_story_points) FROM `vz-it-np-j0nv-dev-odpr-0.od_dq.sprint_planning_sheet` sp WHERE sp.manager = base_data_with_manager.manager AND sp.current_sprint IN (SELECT sprint_name FROM current_sprints)) THEN 'OVERALLOCATED'
    WHEN SUM(story_points) < ((SELECT SUM(total_capacity_story_points) FROM `vz-it-np-j0nv-dev-odpr-0.od_dq.sprint_planning_sheet` sp WHERE sp.manager = base_data_with_manager.manager AND sp.current_sprint IN (SELECT sprint_name FROM current_sprints)) * 0.9) THEN 'UNDERALLOCATED'
    ELSE 'BALANCED'
  END AS team_allocation_status,

  -- Unused Capacity
  CASE 
    WHEN SUM(story_points) < ((SELECT SUM(total_capacity_story_points) FROM `vz-it-np-j0nv-dev-odpr-0.od_dq.sprint_planning_sheet` sp WHERE sp.manager = base_data_with_manager.manager AND sp.current_sprint IN (SELECT sprint_name FROM current_sprints)) * 0.8) THEN (SELECT SUM(total_capacity_story_points) FROM `vz-it-np-j0nv-dev-odpr-0.od_dq.sprint_planning_sheet` sp WHERE sp.manager = base_data_with_manager.manager AND sp.current_sprint IN (SELECT sprint_name FROM current_sprints)) - SUM(story_points)
    ELSE 0
  END AS unused_capacity

FROM base_data_with_manager
WHERE rn = 1
GROUP BY manager
ORDER BY manager;