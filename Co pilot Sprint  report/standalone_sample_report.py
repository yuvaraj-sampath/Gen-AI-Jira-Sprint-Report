import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def send_report(recipients, subject):
    # Path to the HTML template
    template_path = os.path.join(os.path.dirname(__file__), 'templates', 'sprint_report.html')
    with open(template_path, 'r', encoding='utf-8') as f:
        html_content = f.read()

    # SMTP configuration for Verizon
    smtp_server = 'vzsmtp.verizon.com'
    smtp_port = 25
    sender_email = 'Jira.sprint.report@example.com'  

    # Create the email message
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = ", ".join(recipients)
    msg.attach(MIMEText(html_content, 'html'))

    # Send the email
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.sendmail(sender_email, recipients, msg.as_string())
    print(f"Email sent to: {', '.join(recipients)}")

def main():
    # List your recipients here
    recipients = [
        'yuvaraj.s@verizon.com' 
        #, 'siva.sai.gajula@verizon.com'
        #,'bhujith.kumar@verizon.com'
    ]
    subject = "Your Team's Actual Sprint Report"
    send_report(recipients, subject)

if __name__ == "__main__":
    main()