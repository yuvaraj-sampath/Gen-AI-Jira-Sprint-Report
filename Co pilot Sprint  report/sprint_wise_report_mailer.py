import os
import re
import smtplib
import pandas as pd
import requests
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from google.cloud import bigquery
from jinja2 import Template



SPRINT_MANAGER_EMAILS_REGEX = {
    r'^CSG_EPM-FIT,EC_2025_S\d+_EC$':     [ 'venkata.kodali@verizon.com',       'bhavani.mandalika@verizon.com', 'bhujith.kumar@verizon.com' ],
    r'^CSG_EPM-FIT,EC_2025_S\d+_FIT$':    [ 'maruthidevi.valiveti@verizon.com', 'bhavani.mandalika@verizon.com', 'bhujith.kumar@verizon.com' ],
    r'^CSG_EPM 1\.2 PBC_2025_Sprint\d+$': [ 'saranya.banukumar@verizon.com',    'bhavani.mandalika@verizon.com', 'bhujith.kumar@verizon.com' ],
    r'^CSG_POP_2025_S\d+$':               [ 'venkata.kodali@verizon.com',       'bhavani.mandalika@verizon.com', 'bhujith.kumar@verizon.com' ]
    # Add more patterns as needed
}

def get_managers_for_sprint(sprint_name):
    for pattern, managers in SPRINT_MANAGER_EMAILS_REGEX.items():
        if re.match(pattern, sprint_name):
            return managers
    return []

class SprintWiseReportMailer:
    def __init__(self):
        self.client = bigquery.Client()
        self.smtp_server = 'vzsmtp.verizon.com'
        self.smtp_port = 25
        self.sender_email = 'Sprint Analysis Report <sprint.report@verizon.com>'
        # LLM API Configuration
        self.api_url = "https://aihive.ebiz.verizon.com/aihivemw/vegas"
        self.authorization_token = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VjYXNlX3RpdGxlIjoiRFEgcnVsZSBHZW5yYXRpb24iLCJ2emVpZCI6IjU2OTk0ODI2ODgiLCJpYXQiOjE3NTM2OTYxNTMsImV4cCI6MTc1NjI4ODE1M30.pSQMH-m58zjD4mSSXiETpjvawjCzS7h6tCiYBlNuELQ"
        self.headers = {
            "Authorization": self.authorization_token,
            "Content-Type": "application/json"
        }
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_colwidth', None)

    def execute_sprint_wise_query(self):
        sql_path = os.path.join(os.path.dirname(__file__), 'sql', 'sprint_wise_metrics.sql')
        with open(sql_path, 'r', encoding='utf-8') as f:
            query = f.read()
        query_job = self.client.query(query)
        results = list(query_job.result())
        rows = [dict(row) for row in results]
        return pd.DataFrame(rows)

    def get_individual_performance(self, sprint_name):
        sql_path = os.path.join(os.path.dirname(__file__), 'sql', 'sprint_wise_individual_performance_query.sql')
        with open(sql_path, 'r', encoding='utf-8') as f:
            query_template = f.read()
        query = f"""
        WITH filtered AS (
            {query_template}
        )
        SELECT * FROM filtered WHERE sprint_name = '{sprint_name}'
        """
        query_job = self.client.query(query)
        results = list(query_job.result())
        rows = [dict(row) for row in results]
        return pd.DataFrame(rows)

    def get_llm_insights(self, sprint_data, team_members_df):
        use_llm = True
        if not use_llm:
            return "<b>LLM insights are not enabled for this test run.</b>"
        prompt_path = os.path.join(os.path.dirname(__file__), 'prompts', 'llm_insights_prompt.txt')
        with open(prompt_path, 'r', encoding='utf-8') as f:
            prompt_template = f.read()
        prompt = prompt_template.format(
            sprint_name=sprint_data['sprint_name'],
            sprint_start_date=sprint_data['sprint_start_date'],
            team_capacity=sprint_data['team_capacity'],
            assigned_points=sprint_data['assigned_points'],
            team_completion_rate=sprint_data['team_completion_rate'],
            team_members_count=sprint_data['team_members_count'],
            total_issues_count=sprint_data['total_issues_count'],
            completed_issues_count=sprint_data['completed_issues_count'],
            scope_creep=sprint_data['scope_creep'],
            high_priority_count=sprint_data['high_priority_count'],
            medium_priority_count=sprint_data['medium_priority_count'],
            low_priority_count=sprint_data['low_priority_count'],
            stories_count=sprint_data['stories_count'],
            tasks_count=sprint_data['tasks_count'],
            bugs_count=sprint_data['bugs_count'],
            others_count=sprint_data['others_count'],
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
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = self.sender_email
        msg['To'] = ",".join(recipients)
        msg.attach(MIMEText(html_content, 'html'))
        with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
            server.sendmail(self.sender_email, recipients, msg.as_string())
        #print(f"✅ Email sent to: {', '.join(recipients)}")

    def run(self, test_mode=False, test_recipient=None):

        #try:
            
            print("🚀 Starting Sprint Wise Report Generation Process...") 

            # Get sprint data for all sprints
            metrics_df = self.execute_sprint_wise_query()
            if metrics_df.empty:
                print("❌ No sprint data found.")
                return False 
            
            print(f"\n 📊 Found data for {len(metrics_df)} Sprints")

            mail_count = 0
            for idx, sprint_row in metrics_df.iterrows():
                sprint_data = sprint_row.to_dict()
                sprint_name = sprint_data['sprint_name']
                recipients = get_managers_for_sprint(sprint_name)

                if test_mode and test_recipient:
                    recipients = test_recipient
                if not recipients:
                    print(f"\n ⚠️ No recipients found for sprint: {sprint_name}")
                    continue

                individual_df = self.get_individual_performance(sprint_name)

                if individual_df.empty:
                    print(f"\n ⚠️ No individual data for sprint: {sprint_name}")
                    continue

                # Generate LLM insights for the sprint
                print(f" \n 🤖 Generating LLM insights for sprint: {sprint_name}")
                llm_insights = self.get_llm_insights(sprint_data, individual_df)

                #trimming the LLM insights to extract only the HTML content
                match = re.search(r"<html_output>(.*?)</html_output>", llm_insights, re.DOTALL)
                trimmed_llm_insights = match.group(1).strip() if match else llm_insights 

                template_data = {
                    'sprint_name': sprint_name,
                    'sprint_start_formatted': sprint_data['sprint_start_date'].strftime('%B %d, %Y') if hasattr(sprint_data['sprint_start_date'], 'strftime') else sprint_data['sprint_start_date'],
                    'sprint_end_formatted': sprint_data['sprint_end_date'].strftime('%B %d, %Y') if hasattr(sprint_data['sprint_end_date'], 'strftime') else sprint_data['sprint_end_date'],
                    'team_capacity': sprint_data['team_capacity'],
                    'assigned_points': sprint_data['assigned_points'],
                    'team_completion_rate': sprint_data['team_completion_rate'],
                    'team_capacity_utilization': sprint_data['team_capacity_utilization'],
                    'team_members_count': sprint_data['team_members_count'],
                    'total_issues_count': sprint_data['total_issues_count'],
                    'completed_issues_count': sprint_data['completed_issues_count'],
                    'high_priority_count': sprint_data['high_priority_count'],
                    'medium_priority_count': sprint_data['medium_priority_count'],
                    'low_priority_count': sprint_data['low_priority_count'],
                    'stories_count': sprint_data['stories_count'],
                    'tasks_count': sprint_data['tasks_count'],
                    'bugs_count': sprint_data['bugs_count'],
                    'others_count': sprint_data['others_count'],
                    'llm_insights': trimmed_llm_insights,
                    'team_members': [
                        {
                            'assignee_name': row['assignee'],  #.split('@')[0] if '@' in row['assignee'] else row['assignee'],
                            'email': row['email'],
                            'individual_capacity': row['individual_capacity'],
                            'assigned_points': row['assigned_points'],
                            'completion_rate': row['completion_rate']
                        }
                        for _, row in individual_df.iterrows()
                    ]
                }

                # Load and render template
                html_template = self.load_html_template()
                template = Template(html_template)
                html_content = template.render({**template_data, 'min': min})

                # Send email
                subject = f"Comprehensive Sprint Performance Summary - {sprint_name}"
                self.send_mail(recipients, subject, html_content)
                mail_count += 1
                print(f" ✅ Email sent to: {', '.join(recipients)}")
            print(f"\n✨ Sprint Wise Report Generation Completed! Total mails sent: {mail_count}")
            return True
    """ 
        except Exception as e:
            #print(f"❌ Critical error in report generation: {str(e)}")
            import traceback
            print(f"\n❌ Critical error in report generation:")
            traceback.print_exc()
            return False
    """
            
if __name__ == "__main__":
    mailer = SprintWiseReportMailer()
    # Actual Prod: mailer.run()
    # For testing, use: mailer.run(test_mode=True, test_recipient=test_recipients)
    
   
    test_recipients = [
        'yuvaraj.s@verizon.com'
        # ,'siva.sai.gajula@verizon.com',
        ]
    """  
        'bhujith.kumar@verizon.com',
        'saranya.banukumar@verizon.com',
        'bhavani.mandalika@verizon.com',
        'maruthidevi.valiveti@verizon.com',
        'venkata.kodali@verizon.com' 
        # Add more emails for testing as needed
     """
    mailer.run(test_mode=True, test_recipient=test_recipients)
    
   
