import os
import re
import smtplib
import pandas as pd
import requests
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from google.cloud import bigquery
from jinja2 import Template

# ...existing regex mapping and helper...
SPRINT_MANAGER_EMAILS_REGEX = {
    r'^CSG_EPM-FIT,EC_2025_S\d+_EC$': [
        'venkata.kodali@verizon.com', 'bhavani.mandalika@verizon.com', 'bhujith.kumar@verizon.com'
    ],
    r'^CSG_EPM-FIT,EC_2025_S\d+_FIT$': [
        'maruthidevi.valiveti@verizon.com', 'bhavani.mandalika@verizon.com', 'bhujith.kumar@verizon.com'
    ],
    r'^CSG_EPM 1\.2 PBC_2025_Sprint\d+$': [
        'saranya.banukumar@verizon.com', 'bhavani.mandalika@verizon.com', 'bhujith.kumar@verizon.com'
    ],
    r'^CSG_POP_2025_S\d+$': [
        'venkata.kodali@verizon.com', 'bhavani.mandalika@verizon.com', 'bhujith.kumar@verizon.com'
    ]
    # Add more patterns as needed
}

def get_managers_for_sprint(sprint_name):
    for pattern, managers in SPRINT_MANAGER_EMAILS_REGEX.items():
        if re.match(pattern, sprint_name):
            return managers
    return []

class SprintManagerWiseMailReport:
    def __init__(self):
        self.client = bigquery.Client()
        self.smtp_server = 'vzsmtp.verizon.com'
        self.smtp_port = 25
        self.sender_email = 'Sprint Analysis Report <sprint.report@verizon.com>'
        self.api_url = "https://aihive.ebiz.verizon.com/aihivemw/vegas"
        self.authorization_token = "AGSjDWPQVrSWE8TIxDInKD740LUnmlNk"
        #self.authorization_token = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VjYXNlX3RpdGxlIjoiRFEgcnVsZSBHZW5yYXRpb24iLCJ2emVpZCI6IjU2OTk0ODI2ODgiLCJpYXQiOjE3NTM2OTYxNTMsImV4cCI6MTc1NjI4ODE1M30.pSQMH-m58zjD4mSSXiETpjvawjCzS7h6tCiYBlNuELQ"
        self.headers = {
            "Authorization": self.authorization_token,
            "Content-Type": "application/json"
        }
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_colwidth', None)

    def execute_query(self, sql_file):
        sql_path = os.path.join(os.path.dirname(__file__), 'sql', sql_file)
        with open(sql_path, 'r', encoding='utf-8') as f:
            query = f.read()
        query_job = self.client.query(query)
        results = list(query_job.result())
        rows = [dict(row) for row in results]
        return pd.DataFrame(rows)

    def get_individual_performance(self, sql_file, filter_key, filter_value):
        sql_path = os.path.join(os.path.dirname(__file__), 'sql', sql_file)
        with open(sql_path, 'r', encoding='utf-8') as f:
            query_template = f.read()
        query = f"""
        WITH filtered AS (
            {query_template}
        )
        SELECT * FROM filtered WHERE {filter_key} = '{filter_value}'
        """
        query_job = self.client.query(query)
        results = list(query_job.result())
        rows = [dict(row) for row in results]
        return pd.DataFrame(rows)

    def get_llm_insights(self, data, team_members_df, is_manager_wise=False):
        prompt_path = os.path.join(os.path.dirname(__file__), 'prompts', 'llm_insights_prompt.txt')
        with open(prompt_path, 'r', encoding='utf-8') as f:
            prompt_template = f.read()
        prompt = prompt_template.format(
            sprint_name=data.get('sprint_name', ''),
            sprint_start_date=data.get('sprint_start_date', ''),
            team_capacity=data['team_capacity'],
            assigned_points=data['assigned_points'],
            team_completion_rate=data['team_completion_rate'],
            #team_efficiency=data.get('team_efficiency', ''), 
            scope_creep=data.get('scope_creep',''),
            team_members_count=data['team_members_count'],
            total_issues_count=data['total_issues_count'],
            completed_issues_count=data['completed_issues_count'],
            high_priority_count=data['high_priority_count'],
            medium_priority_count=data['medium_priority_count'],
            low_priority_count=data['low_priority_count'],
            stories_count=data['stories_count'],
            tasks_count=data['tasks_count'],
            bugs_count=data['bugs_count'],
            others_count=data['others_count'],
            team_members_table=team_members_df.to_string(index=False)
        )
        payload = {
            "input": prompt,
            "usecase_context_id": "gemini-2-flash-001",
            "llm_parameter": {
                "max_output_tokens": 8094,
                "temperature": 0,
                "top_p": 0.7,
                "top_k": 15
            }
        }
        try:
            response = requests.post(self.api_url, json=payload, headers=self.headers)
            if response.status_code in [200, 201]:
                response_data = response.json()
                return response_data.get('response', {}).get('answer', '')
            else:
                print(f"Failed to get LLM response. Status code: {response.status_code}")
                return "AI insights unavailable at this time."
        except Exception as e:
            print(f"Error getting LLM response: {str(e)}")
            return "AI insights unavailable at this time."

    def load_html_template(self):
        template_path = os.path.join(os.path.dirname(__file__), 'templates', 'comprehensive_sprint_report.html')
        with open(template_path, 'r', encoding='utf-8') as f:
            return f.read()

    def send_mail(self, recipients, subject, html_content):
        # Flatten recipients if needed
        if recipients and isinstance(recipients[0], list):
            recipients = [email for sublist in recipients for email in sublist]
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = self.sender_email
        msg['To'] = ', '.join(recipients)
        msg.attach(MIMEText(html_content, 'html'))
        with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
            server.sendmail(self.sender_email, recipients, msg.as_string())
        print(f"✅ Email sent to: {', '.join(recipients)}")

    def run_sprint_wise(self, test_recipients=None):

        print("🚀 Sending Sprint-wise Mail Reports...")

        metrics_df = self.execute_query('sprint_wise_metrics.sql')

        if metrics_df.empty:
            print("❌ No sprint data found.")
            return False
        for idx, sprint_row in metrics_df.iterrows():
            sprint_data = sprint_row.to_dict()
            sprint_name = sprint_data['sprint_name']
            recipients = get_managers_for_sprint(sprint_name)
            if test_recipients:
                recipients = test_recipients
            if not recipients:
                print(f"⚠️ No recipients found for sprint: {sprint_name}")
                continue
            individual_df = self.get_individual_performance('sprint_wise_individual_performance_query.sql', 'sprint_name', sprint_name)
            if individual_df.empty:
                print(f"⚠️ No individual data for sprint: {sprint_name}")
                continue

            # Calculate team completion rate
            assigned_points = sprint_data.get('assigned_points', 0)
            completed_points = sprint_data.get('completed_issues_count', 0)
            team_completion_rate = round((completed_points / assigned_points) * 100, 2) if assigned_points else 0

            # Generate LLM insights
            llm_insights = self.get_llm_insights(sprint_data, individual_df)
            match = re.search(r"<html_output>(.*?)</html_output>", llm_insights, re.DOTALL)
            trimmed_llm_insights = match.group(1).strip() if match else llm_insights
            template_data = {
                'sprint_name': sprint_name,
                'sprint_start_date': sprint_data['sprint_start_date'],
                'sprint_start_formatted': sprint_data['sprint_start_date'].strftime('%B %d, %Y') if hasattr(sprint_data['sprint_start_date'], 'strftime') else sprint_data['sprint_start_date'],
                'sprint_end_formatted': sprint_data['sprint_end_date'].strftime('%B %d, %Y') if hasattr(sprint_data['sprint_end_date'], 'strftime') else sprint_data['sprint_end_date'], 
                'team_capacity': sprint_data['team_capacity'],
                'assigned_points':sprint_data['assigned_points'],
                'completed_issues_count':sprint_data['completed_issues_count'],
                'team_completion_rate':sprint_data['team_completion_rate'],
                'team_members_count':sprint_data['team_members_count'],
                'team_efficiency': sprint_data.get('team_efficiency', 0),
                'team_capacity_utilization': sprint_data.get('team_capacity_utilization', 0),
                'team_members_count': sprint_data.get('team_members_count', 0),
                'total_issues_count': sprint_data.get('total_issues_count', 0),
                'high_priority_count': sprint_data.get('high_priority_count', 0),
                'medium_priority_count': sprint_data.get('medium_priority_count', 0),
                'low_priority_count': sprint_data.get('low_priority_count', 0),
                'stories_count': sprint_data.get('stories_count', 0),
                'tasks_count': sprint_data.get('tasks_count', 0),
                'bugs_count': sprint_data.get('bugs_count', 0),
                'others_count': sprint_data.get('others_count', 0),
                'llm_insights': trimmed_llm_insights,
                'team_members': [
                    {
                        'assignee_name': row['assignee'], #.split('@')[0] if '@' in row['assignee'] else row['assignee'],
                        'email': row['email'],
                        'individual_capacity': row['individual_capacity'],
                        'assigned_points': row['assigned_points'],
                        'completion_rate': row['completion_rate']
                    }
                    for _, row in individual_df.iterrows()
                ]
            }
            html_template = self.load_html_template()
            template = Template(html_template)
            html_content = template.render({**template_data, 'min': min})
            subject = f"Sprint Analysis Report for {sprint_name}"
            self.send_mail(recipients, subject, html_content)
        print("✨ Sprint-wise Mail Reports Completed!")
        return True

    def run_manager_wise(self, test_recipients=None):

        print("🚀 Sending Manager-wise Mail Reports...")

        metrics_df = self.execute_query('manager_wise_metrics.sql')
        if metrics_df.empty:
            print("❌ No manager data found.")
            return False
        
        for idx, manager_row in metrics_df.iterrows():
            manager_data = manager_row.to_dict()
            manager_name = manager_data['manager']
            print(f"\nProcessing manager: {manager_name}")

            recipients = test_recipients if test_recipients else [manager_data.get('manager_email', 'yuvaraj.s@verizon.com')]
           # individual_df = self.get_individual_performance('individual_performance_query.sql', 'manager', manager_name) 
            individual_df = self.get_individual_performance('sprint_wise_individual_performance_query.sql', 'manager', manager_name)  
            if individual_df.empty:
                print(f"⚠️ No individual data for manager: {manager_name}")
                continue

            # Calculate team completion rate
            assigned_points = manager_data.get('assigned_points', 0)
            completed_points = manager_data.get('completed_issues_count', 0)
            team_completion_rate = round((completed_points / assigned_points) * 100, 2) if assigned_points else 0

            # Generate LLM insights
            llm_insights = self.get_llm_insights(manager_data, individual_df, is_manager_wise=True)
            match = re.search(r"<html_output>(.*?)</html_output>", llm_insights, re.DOTALL)
            trimmed_llm_insights = match.group(1).strip() if match else llm_insights
            template_data = {
                'sprint_name': manager_data.get('sprint_name', ''),
                'sprint_start_date': manager_data.get('sprint_start_date',''),
                'team_capacity': manager_data['team_capacity'],
                'assigned_points':manager_data['assigned_points'],
                'completed_issues_count':manager_data['completed_issues_count'],
                'team_completion_rate':manager_data['team_completion_rate'],
                'team_members_count':manager_data['team_members_count'],
                'team_capacity_utilization': manager_data.get('team_capacity_utilization', 0),
                'team_members_count': manager_data.get('team_members_count', 0),
                'total_issues_count': manager_data.get('total_issues_count', 0),
                'high_priority_count': manager_data.get('high_priority_count', 0),
                'medium_priority_count': manager_data.get('medium_priority_count', 0),
                'low_priority_count': manager_data.get('low_priority_count', 0),
                'stories_count': manager_data.get('stories_count', 0),
                'tasks_count': manager_data.get('tasks_count', 0),
                'bugs_count': manager_data.get('bugs_count', 0),
                'others_count': manager_data.get('others_count', 0),
                'llm_insights': trimmed_llm_insights,
                'team_members': [
                    {
                        'assignee_name': row['assignee'], #.split('@')[0] if '@' in row['assignee'] else row['assignee'],
                        'email': row['email'],
                        'individual_capacity': row.get('individual_capacity', row.get('capacity', 0)),
                        'assigned_points': row.get('assigned_points', row.get('story_points', 0)),
                        'completion_rate': row['completion_rate']
                    }
                    for _, row in individual_df.iterrows()
                ]
            }
            html_template = self.load_html_template()
            template = Template(html_template)
            html_content = template.render({**template_data, 'min': min})
            subject = f"Manager-wise Sprint Analysis Report for {manager_name}"
            self.send_mail(recipients, subject, html_content)
        print("✨ Manager-wise Mail Reports Completed!")
        return True

if __name__ == "__main__":
    mailer = SprintManagerWiseMailReport()
    # For testing, provide a list of emails
    test_recipients = [
        'yuvaraj.s@verizon.com',
        # Add more emails for testing as needed
    ]
    mailer.run_sprint_wise(test_recipients=test_recipients)
    mailer.run_manager_wise(test_recipients=test_recipients)
