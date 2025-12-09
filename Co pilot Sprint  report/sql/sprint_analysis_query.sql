/* 
SQL Query: Manager level breakdown in a single recent sprint

COMPREHENSIVE SPRINT ANALYSIS QUERY WITH SEPARATE MANAGER MAPPING TABLE
This query calculates ALL required metrics per MANAGER for the most recent sprint
Uses vz-it-np-j0nv-dev-odpr-0.od_dq.sprint_planning_sheet for manager relationships
*/

-- Get the most recent sprint from the main table
WITH recent_sprint AS (
  SELECT 
    sprint_name,
    sprint_start_date,
    sprint_end_date
  FROM 
    `vz-it-np-j0nv-dev-odpr-0.od_dq.jira_capacity_planning_v3`
  WHERE 
    sprint_end_date > CURRENT_TIMESTAMP()
    --sprint_name = 'CSG_EPM-FIT,EC_2025_S04_EC' --'CSG_POP_2025_S07'
  /*
    WHERE sprint_end_date IS NOT NULL
    ORDER BY sprint_end_date DESC
    LIMIT 1 
  */
),

-- Get latest record per issue using ROW_NUMBER and join with Sprint planning table for manager and capacity details
base_data_with_manager AS (
  SELECT 
    sprint_name,
    sprint_start_date,
    sprint_end_date,
    assignee,
    vzid,
    manager,
    email,
    issue_key,
    story_points,
    total_capacity_story_points,
    status,
    priority,
    issue_type,
    created
  FROM (
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
      recent_sprint rs 
    ON 
      bd.sprint_name = rs.sprint_name
    -- JOIN with sprint planning sheet for manager relationships
    INNER JOIN 
      `vz-it-np-j0nv-dev-odpr-0.od_dq.sprint_planning_sheet` sp
    ON
      TRIM(UPPER(bd.vzid)) = TRIM(UPPER(sp.vzid))
      AND bd.sprint_name = sp.current_sprint
      AND bd.manager = sp.manager
    WHERE 
      bd.backlog = 'N'  -- Exclude backlog items
      /*
      AND bd.assignee IS NOT NULL
      AND bd.assignee != ''
      AND sp.manager IS NOT NULL
      AND sp.manager != '' 
      */
    
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
  WHERE rn = 1  
),

-- CALCULATE SPRINT METRICS PER MANAGER
manager_sprint_summary AS (
  SELECT
    manager,
    sprint_name,
    MIN(sprint_start_date) AS sprint_start_date,
    MAX(sprint_end_date) AS sprint_end_date,
    
    -- SPRINT METRICS: Team-Level (Sprint Health) PER MANAGER --

    -- Team Capacity: Sum of individual capacities for this manager's team for sprint wise
    (SELECT SUM(total_capacity_story_points) 
     FROM `vz-it-np-j0nv-dev-odpr-0.od_dq.sprint_planning_sheet` sp 
     WHERE sp.manager = base_data_with_manager.manager 
     AND sp.current_sprint = base_data_with_manager.sprint_name) AS team_capacity,
    
    -- Assigned Points: Total story points assigned to this manager's team
    SUM(story_points) AS assigned_points,
    
    -- TASK DISTRIBUTION FOR THIS MANAGER'S TEAM
    -- Team Members Count: Count of distinct assignees under this manager
    COUNT(DISTINCT assignee) AS team_members_count,
    
    -- Total Issues Count: Count of all issues assigned to this manager's team
    COUNT(DISTINCT issue_key) AS total_issues_count,

    -- Completed Story Points: Story points for completed issues by this manager's team
    COUNTIF(status IN ('Done', 'APPROVE DEFINITION OF DONE')) AS completed_issues_count,
    --SUM(CASE WHEN status IN ('Done', 'APPROVE DEFINITION OF DONE') THEN story_points ELSE 0 END) AS completed_story_points,

    -- PRIORITY BREAKDOWN FOR THIS MANAGER'S TEAM 
    COUNTIF(priority IN ('Highest', 'Very High', 'High')) AS high_priority_count,
    COUNTIF(priority = 'Medium') AS medium_priority_count,
    COUNTIF(priority IN('Lowest', 'Low')) AS low_priority_count,
    
    -- ISSUE TYPE DISTRIBUTION FOR THIS MANAGER'S TEAM
    COUNTIF(issue_type = 'VZAgile Story') AS stories_count,
    COUNTIF(issue_type = 'Task') AS tasks_count,
    COUNTIF(issue_type = 'Bug') AS bugs_count,
    COUNTIF(issue_type NOT IN ('VZAgile Story', 'Task', 'Bug')) AS others_count
    
  FROM base_data_with_manager
  GROUP BY manager, sprint_name
),

-- INDIVIDUAL PERFORMANCE METRICS PER MANAGER'S TEAM
individual_performance AS (
  SELECT
    manager,
    assignee AS individual_name,
    email,
    
    -- Capacity Points: Individual's total capacity for the sprint (from sprint planning sheet)
    MAX(total_capacity_story_points) AS capacity_points,
    
    -- Assigned Points: Sum of story points assigned to this individual
    SUM(story_points) AS assigned_points,
    
    -- Completed Story Points: Sum of story points for completed issues
    SUM(CASE WHEN status IN ('Done', 'APPROVE DEFINITION OF DONE') THEN story_points ELSE 0 END) AS completed_story_points,
    
    -- Rate of Completion: (Completed Story Points / Assigned Story Points) * 100
    ROUND(
      CASE 
        WHEN SUM(story_points) > 0 
          THEN (COUNTIF(status IN ('DONE', 'APPROVE DEFINITION OF DONE')))/ SUM(story_points) * 100
        --THEN (SUM(CASE WHEN status IN ('Done', 'APPROVE DEFINITION OF DONE') THEN story_points ELSE 0 END) / SUM(story_points)) * 100
        ELSE 0 
      END, 2
    ) AS rate_of_completion,
    
    -- Overallocated or Underallocated for individual (assigned points > capacity points)
    CASE 
      WHEN SUM(story_points) > MAX(total_capacity_story_points) THEN 'OVERALLOCATED'
      WHEN SUM(story_points) < (MAX(total_capacity_story_points) * 0.7) THEN 'UNDERALLOCATED'
      ELSE 'BALANCED'
    END AS individual_allocation_status
    
  FROM base_data_with_manager
  GROUP BY manager, assignee, email
)

-- MAIN RESULT: ALL REQUIRED METRICS PER MANAGER
SELECT
  -- SPRINT BASIC INFO
  mss.sprint_name,
  mss.sprint_start_date,
  mss.sprint_end_date,
  mss.manager AS manager_name,
  
  -- SPRINT METRICS: Team-Level (Sprint Health) FOR THIS MANAGER
  mss.team_capacity,
  mss.assigned_points,
  
  -- Team Efficiency: (Completed Story Points / Assigned Story Points) * 100
  ROUND(
    CASE 
      WHEN mss.assigned_points > 0 THEN (mss.completed_issues_count / mss.assigned_points) * 100
      ELSE 0 
    END, 2
  ) AS team_efficiency,
  
  -- Team Capacity Utilization: (Assigned Points / Team Capacity) * 100
  ROUND(
    CASE 
      WHEN mss.team_capacity > 0 THEN (mss.assigned_points / mss.team_capacity) * 100
      ELSE 0 
    END, 2
  ) AS team_capacity_utilization,
  
  -- TASK DISTRIBUTION FOR THIS MANAGER'S TEAM
  mss.team_members_count,
  mss.total_issues_count,
  mss.completed_issues_count,
  
  -- PRIORITY BREAKDOWN FOR THIS MANAGER'S TEAM
  mss.high_priority_count,
  mss.medium_priority_count,
  mss.low_priority_count,
  
  -- ISSUE TYPE DISTRIBUTION FOR THIS MANAGER'S TEAM
  mss.stories_count,
  mss.tasks_count,
  mss.bugs_count,
  mss.others_count,
  
  -- INSIGHTS AND RECOMMENDATIONS FOR THIS MANAGER'S TEAM
  -- Overallocated or Underallocated for team (Team capacity vs Assigned points)
  CASE 
    WHEN mss.assigned_points > mss.team_capacity THEN 'OVERALLOCATED'
    WHEN mss.assigned_points < (mss.team_capacity * 0.7) THEN 'UNDERALLOCATED'
    ELSE 'BALANCED'
  END AS team_allocation_status,
  
  -- Unused Capacity (assigned points < team capacity * 0.7)
  CASE 
    WHEN mss.assigned_points < (mss.team_capacity * 0.7) THEN mss.team_capacity - mss.assigned_points
    ELSE 0
  END AS unused_capacity,
  
  -- Count of overallocated individuals in this manager's team
  (SELECT AS STRUCT COUNT(*) AS count,
    ARRAY_AGG(STRUCT(ip.individual_name))
   FROM individual_performance ip 
   WHERE ip.manager = mss.manager 
   AND ip.individual_allocation_status = 'OVERALLOCATED') AS overallocated_individuals,

  -- TEAM MEMBERS PERFORMANCE FOR THIS MANAGER (JSON format for easy parsing)
 /* 
  ARRAY_AGG(
    STRUCT(
      ip.individual_name,
      ip.email,
      ip.capacity_points,
      ip.assigned_points,
      ip.completed_story_points,
      ip.rate_of_completion,
      ip.individual_allocation_status
    ) 
    ORDER BY ip.rate_of_completion DESC
  ) AS team_members_performance
*/

FROM manager_sprint_summary mss
LEFT JOIN individual_performance ip ON mss.manager = ip.manager
GROUP BY 
  mss.sprint_name,
  mss.sprint_start_date,
  mss.sprint_end_date,
  mss.manager,
  mss.team_capacity,
  mss.assigned_points,
  mss.completed_issues_count,
  mss.team_members_count,
  mss.total_issues_count,
  mss.high_priority_count,
  mss.medium_priority_count,
  mss.low_priority_count,
  mss.stories_count,
  mss.tasks_count,
  mss.bugs_count,
  mss.others_count
ORDER BY mss.manager;

