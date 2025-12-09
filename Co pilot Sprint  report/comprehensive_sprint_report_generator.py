"""
This Code will generate a sprint report for a sprint and prepare individual metric for each managers resource.

"""

import os
import smtplib
import pandas as pd
import requests 
import re
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from google.cloud import bigquery
from jinja2 import Template

class SprintReportMailer:
    def __init__(self):
        """Initialize the Sprint Report Mailer with BigQuery client, email configuration, and LLM API setup."""
        self.client = bigquery.Client()
        self.smtp_server = 'vzsmtp.verizon.com'
        self.smtp_port = 25
        self.sender_email = 'Jira.sprint.report@verizon.com'
        
        # LLM API Configuration
        self.api_url = "https://aihive.ebiz.verizon.com/aihivemw/vegas"
        self.authorization_token = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VjYXNlX3RpdGxlIjoiRFEgcnVsZSBHZW5yYXRpb24iLCJ2emVpZCI6IjU2OTk0ODI2ODgiLCJpYXQiOjE3NTM2OTYxNTMsImV4cCI6MTc1NjI4ODE1M30.pSQMH-m58zjD4mSSXiETpjvawjCzS7h6tCiYBlNuELQ"
        self.headers = {
            "Authorization": self.authorization_token,
            "Content-Type": "application/json"
        }
        
        # Display options for pandas DataFrame
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_colwidth', None)

    def execute_sprint_query(self):
        """Execute the SQL query to get sprint metrics for all managers."""
        sql_path = os.path.join(os.path.dirname(__file__), 'sql', 'sprint_analysis_query.sql')
        with open(sql_path, 'r', encoding='utf-8') as f:
            query = f.read()
        query_job = self.client.query(query) 
        results = list(query_job.result())
        rows = [dict(row) for row in results]
        return pd.DataFrame(rows)

    def get_individual_performance_data(self, manager_name):
        """Get detailed individual performance data for a specific manager.""" 
        
        sql_path = os.path.join(os.path.dirname(__file__), 'sql', 'individual_performance_query.sql')
        with open(sql_path, 'r', encoding='utf-8') as f:
            query = f.read()
        query = (query.format(manager_name=manager_name)) 
        query_job = self.client.query(query)
        results = list(query_job.result())
        rows = [dict(row) for row in results]
        return pd.DataFrame(rows)

    def get_llm_insights(self, manager_data, team_members_df):

        """
        Generate AI-powered insights using LLM API, or return a placeholder if disabled.
        Set use_llm=True to enable LLM integration.
        """ 
        use_llm = True
        if not use_llm:
            return "<b>LLM insights are not enabled for this test run.</b>"

        # Read prompt template from file
        prompt_path = os.path.join(os.path.dirname(__file__), 'prompts', 'llm_insights_prompt.txt')
        with open(prompt_path, 'r', encoding='utf-8') as f:
            prompt_template = f.read()
        prompt = prompt_template.format(
            manager_name=manager_data['manager_name'],
            team_capacity=manager_data['team_capacity'],
            assigned_points=manager_data['assigned_points'],
            team_efficiency=manager_data['team_efficiency'],
            team_members_count=manager_data['team_members_count'],
            total_issues_count=manager_data['total_issues_count'],
            completed_issues_count=manager_data['completed_issues_count'],
            high_priority_count=manager_data['high_priority_count'],
            medium_priority_count=manager_data['medium_priority_count'],
            low_priority_count=manager_data['low_priority_count'],
            stories_count=manager_data['stories_count'],
            tasks_count=manager_data['tasks_count'],
            bugs_count=manager_data['bugs_count'],
            others_count=manager_data['others_count'],
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
        """Load the HTML template from the templates folder."""
        template_path = os.path.join(os.path.dirname(__file__), 'templates', 'comprehensive_sprint_report.html')
        try:
            with open(template_path, 'r', encoding='utf-8') as f:
                return f.read()
        except FileNotFoundError:
            print(f"Template file not found: {template_path}")
            raise        

    def send_mail(self, recipients, subject, html_content):
        """Send email with the sprint report."""
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = self.sender_email
        msg['To'] = ", ".join(recipients)
        msg.attach(MIMEText(html_content, 'html'))
        with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
            server.sendmail(self.sender_email, recipients, msg.as_string())
        print(f"✅ Email sent to: {', '.join(recipients)}")

    def run(self, recipients):
        """Main method to generate and send reports for all managers."""
        try:
            print("🚀 Starting Sprint Report Generation Process...")
            
            # Get sprint data for all managers
            metrics_df = self.execute_sprint_query()
            
            if metrics_df.empty:
                print("❌ No sprint data found. Exiting...")
                return False

            print(f"📊 Found data for {len(metrics_df)} managers")
            
            # Process each manager with predefined email
            for idx, manager_row in metrics_df.iterrows():
                manager_data = manager_row.to_dict() 

                #uncomment the below line and comment the next if else for PROD and without test condition
                #recipients = [manager_data['manager_email']]


                if recipients is not None:
                    current_recipients = recipients
                else:
                    current_recipients = [manager_data['manager_email']]


                # Get individual team member performance data
                team_members_df = self.get_individual_performance_data(manager_data['manager_name'])
                
                if team_members_df.empty:
                    print(f"⚠️  No team member data found for manager: {manager_data['manager_name']}")
                    continue

                print(f"🤖 Generating AI insights...")
                # Generate AI insights
                 
                llm_insights = self.get_llm_insights(manager_data, team_members_df) 

                #trimming the LLM insights to extract only the HTML content
                match = re.search(r"<html_output>(.*?)</html_output>", llm_insights, re.DOTALL)
                trimmed_llm_insights = match.group(1).strip() if match else llm_insights

                """ #printing for testing purpose to check the LLM output
                print(llm_insights) 
                print("🔍 Trimmed LLM insights:")
                print(trimmed_llm_insights)
                """

                # Prepare template data
                template_data = {
                    'sprint_name': manager_data['sprint_name'],
                    'sprint_start_formatted': manager_data['sprint_start_date'].strftime('%B %d, %Y'),
                    'sprint_end_formatted': manager_data['sprint_end_date'].strftime('%B %d, %Y'),
                    'manager_first_name': manager_data['manager_name'].split()[0],
                    'team_capacity': manager_data['team_capacity'],
                    'assigned_points': manager_data['assigned_points'],
                    'team_efficiency': manager_data['team_efficiency'],
                    'team_capacity_utilization': manager_data['team_capacity_utilization'],
                    'team_members_count': manager_data['team_members_count'],
                    'total_issues_count': manager_data['total_issues_count'],
                    'completed_issues_count': manager_data['completed_issues_count'],
                    'high_priority_count': manager_data['high_priority_count'],
                    'medium_priority_count': manager_data['medium_priority_count'],
                    'low_priority_count': manager_data['low_priority_count'],
                    'stories_count': manager_data['stories_count'],
                    'tasks_count': manager_data['tasks_count'],
                    'bugs_count': manager_data['bugs_count'],
                    'others_count': manager_data['others_count'],
                    'llm_insights': trimmed_llm_insights,
                    'team_members': [
                        {
                            'assignee_name': row['assignee'].split('@')[0] if '@' in row['assignee'] else row['assignee'],
                            'email': row['email'],
                            'capacity': row['capacity'],
                            'story_points': row['story_points'],
                            'completion_rate': row['completion_rate']
                        }
                        for _, row in team_members_df.iterrows()
                    ]
                }

                # Load and render template
                html_template = self.load_html_template()
                template = Template(html_template)
                html_content = template.render({**template_data, 'min': min})
                # Send email
                subject = f"Sprint Analysis Report for  {manager_data['sprint_name']} - {manager_data['manager_name']}"
                self.send_mail(current_recipients, subject, html_content)

            print(f"\n✨ Sprint Report Generation Completed!")
            
            return True

        except Exception as e:
            #  print(f"❌ Critical error in report generation: {str(e)}") --comment the below 3 lines
            import traceback
            print(f"\n❌ Critical error in report generation:")
            traceback.print_exc()
            return False


def main():
    """Main function to run the sprint report generator."""

     # For testing, uncomment and set recipients
 
    recipients = [
        'yuvaraj.s@verizon.com'
        #,'siva.sai.gajula@verizon.com'
        #,'bhujith.kumar@verizon.com' 
        #,'saranya.banukumar@verizon.com'
        #,'bhavani.mandalika@verizon.com'
        #,'maruthidevi.valiveti@verizon.com '
        #,'venkata.kodali@verizon.com '
        
        # Add more emails for testing as needed
    ]

    generator = SprintReportMailer()

    # For testing: generator.run(recipients)
    generator.run(recipients)
    # For production: generator.run()
    # generator.run()


if __name__ == "__main__":
    main()