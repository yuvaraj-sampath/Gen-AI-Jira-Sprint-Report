        WITH recent_sprint AS (
          SELECT 
            sprint_name,
            sprint_start_date,
            sprint_end_date
          FROM `vz-it-np-j0nv-dev-odpr-0.od_dq.jira_capacity_planning_v3`
                     WHERE 
          sprint_end_date > CURRENT_TIMESTAMP()
          --sprint_name = 'CSG_EPM-FIT,EC_2025_S04_EC' --'CSG_POP_2025_S07'
        /*
          WHERE sprint_end_date IS NOT NULL
          ORDER BY sprint_end_date DESC
          LIMIT 1 
        */
        ),

        base_data_with_manager AS (
          SELECT DISTINCT
            bd.assignee,
            bd.email,
            sps.manager,
            CAST(bd.story_points AS FLOAT64) AS story_points,
            CAST(sps.total_capacity_story_points AS FLOAT64) AS total_capacity_story_points,
            bd.status
          FROM `vz-it-np-j0nv-dev-odpr-0.od_dq.jira_capacity_planning_v3` bd
          INNER JOIN recent_sprint rs ON bd.sprint_name = rs.sprint_name
          INNER JOIN `vz-it-np-j0nv-dev-odpr-0.od_dq.sprint_planning_sheet` sps 
            ON TRIM(UPPER(bd.vzid)) = TRIM(UPPER(sps.vzid))
            AND bd.sprint_name = sps.current_sprint
          WHERE
            bd.backlog = 'N'
            AND sps.manager = '{manager_name}'
            AND bd.assignee IS NOT NULL
            AND bd.assignee != ''
        )

        SELECT
          manager,
          assignee,
          email,
          MAX(total_capacity_story_points) AS capacity,
          SUM(story_points) AS story_points,
          SUM(CASE WHEN status IN ('Done', 'APPROVE DEFINITION OF DONE') THEN story_points ELSE 0 END) AS completed_story_points,
          ROUND(
            CASE 
              WHEN SUM(story_points) > 0 
              THEN (SUM(CASE WHEN status IN ('Done', 'APPROVE DEFINITION OF DONE') THEN story_points ELSE 0 END) / SUM(story_points)) * 100
              ELSE 0 
            END, 2
          ) AS completion_rate
        FROM base_data_with_manager
        GROUP BY assignee, manager, email
        ORDER BY completion_rate DESC