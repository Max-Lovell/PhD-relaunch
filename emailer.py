import os
import glob
import json
import pandas as pd
import datetime as dt
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from collections import defaultdict
import smtplib
import paramiko
from emails import Emails  # Assumes emails.py is in the same repo

# --- CONFIGURATION (Loaded from GitHub Secrets) ---
SSH_HOST = "unix.sussex.ac.uk"  # Or the specific IP/Host you use
SSH_USER = os.environ.get("SUSSEX_USER")
SSH_PASS = os.environ.get("SUSSEX_PASS")
REMOTE_PATH = "/its/home/mel29/metacog/webservice/"

GMAIL_USER = os.environ.get("GMAIL_USER")
GMAIL_PASS = os.environ.get("GMAIL_PASS")  # App Password, NOT login password


# --- 1. DOWNLOAD FILES VIA SFTP ---
def download_data():
    local_dir = "webservice"
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    print("Connecting to Sussex Server...")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh.connect(SSH_HOST, username=SSH_USER, password=SSH_PASS)
        sftp = ssh.open_sftp()

        # List files in remote directory
        files = sftp.listdir(REMOTE_PATH)
        count = 0
        for file in files:
            if file.endswith(".json"):
                remote_file = os.path.join(REMOTE_PATH, file).replace("\\", "/")
                local_file = os.path.join(local_dir, file)
                sftp.get(remote_file, local_file)
                count += 1

        print(f"Downloaded {count} JSON files.")
        sftp.close()
        ssh.close()
    except Exception as e:
        print(f"SSH Connection Failed: {e}")
        exit(1)


# --- 2. PROCESS DATA ---
def process_and_send():
    download_data()

    survey_names = ['pre', 'day_1', 'day_2', 'day_3', 'day_4', 'day_5',
                    'day_6', 'day_7', 'day_8', 'day_9', 'day_10', 'post']

    # Survey IDs (from your original uploaded file)
    surveys = {
        'pre': ['SV_6liqwhsa4LJndL7'],
        'day_1': ['SV_efYBX7JoyriIFed', 'SV_6yxDEodJtXUpYUZ'],
        'day_2': ['SV_8tRRT3RDUgZjLmJ', 'SV_eyX5P5Dss4qbcIR'],
        'day_3': ['SV_7P4fccFwsHijQFf', 'SV_2gJKPLNxfUEnGWV'],
        'day_4': ['SV_cHZ2uoqcwNYHtWJ', 'SV_246gE20EMlJPFvT'],
        'day_5': ['SV_4JGPLlcIXn9ikjH', 'SV_eaiZkPQlw92Rks5'],
        'day_6': ['SV_6X6g76pH3IPip0N', 'SV_4SkXvAykwSHD2Sx'],
        'day_7': ['SV_b8SuzWGpqSAk549', 'SV_8ffEaZSjf0lxdGd'],
        'day_8': ['SV_br3SCsLmRnzwEIt', 'SV_00dnZDoRJhItER7'],
        'day_9': ['SV_0d4h7wpObx8Fwnb', 'SV_4Vdaii451unzstD'],
        'day_10': ['SV_cUXO3opuPOK9lDn', 'SV_b9mrVYWMbVU7CXH'],
        'post': ['SV_a99FGPniDPAO6b3'],
        'materials': ['SV_1Y1cpZndfWrXoEu']
    }

    # 1. LOAD DATA
    file_list = glob.glob('webservice/*.json')
    datal = []

    for file in file_list:
        with open(file) as f:
            try:
                datad = json.load(f)
                for key, value in surveys.items():
                    if datad["survey"] in value:
                        entry = {'email': datad["email"]}
                        if "condition" in datad:
                            entry['condition'] = datad["condition"]
                        entry[key] = datad["date"]
                        datal.append(entry)
            except json.JSONDecodeError:
                continue

    datac = defaultdict(dict)
    for d in datal:
        datac[d["email"]].update(d)

    datafl = []
    for d in list(datac.values()):
        datafl.append(pd.DataFrame([d]))

    if not datafl:
        print("No data found to process.")
        return

    dataset = pd.concat(datafl)
    # Ensure all columns exist
    for col in survey_names:
        if col not in dataset.columns:
            dataset[col] = None

    # 2. CONNECT TO GMAIL
    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.login(GMAIL_USER, GMAIL_PASS)
    except Exception as e:
        print(f"Failed to login to Gmail: {e}")
        return

    emails_sent = 0

    # 3. ITERATE PARTICIPANTS
    for index, row in dataset.iterrows():
        row_na = row.dropna()
        if len(row_na) <= 2: continue

        # Get last completion date
        try:
            last_date_str = row_na.iloc[-1]
            datef = dt.datetime.strptime(last_date_str, "%m/%d/%Y")
        except (ValueError, TypeError):
            continue

        today = dt.datetime.today()
        elapsed = today - datef
        last_survey = row_na.index[-1]

        # Skip if completed post-test or invalid control range
        if last_survey == 'post':
            continue
        if row.get('condition') == 'control' and (elapsed.days < 20 or elapsed.days > 23):
            continue

        try:
            survey_index = survey_names.index(last_survey)
            next_survey = survey_names[survey_index + 1]
        except (ValueError, IndexError):
            continue

        # Determine Survey ID
        if next_survey == 'post' or elapsed.days > 4 or (row.get('condition') == 'control' and elapsed.days >= 20):
            survey_ID = str(surveys['post'][0])
        elif row.get('condition') == 'mental':
            survey_ID = str(surveys[next_survey][0])
        elif row.get('condition') == 'world':
            survey_ID = str(surveys[next_survey][1])
        else:
            continue

        survey_url = "https://universityofsussex.eu.qualtrics.com/jfe/form/" + survey_ID + "?RecipientEmail=" + row[
            'email']

        # Determine Email Body Type
        email_body = None

        if row.get('condition') == 'control':
            if elapsed.days in [20, 23]:
                email_body = 'control_post'
        elif row.get('condition') in ['mental', 'world']:
            if next_survey == 'post' or elapsed.days > 4:
                email_body = 'post'
            elif next_survey == 'day_1':
                email_body = 'day_1'
            elif next_survey == 'day_2':
                email_body = 'day_2'
            elif next_survey in ['day_3', 'day_4', 'day_5', 'day_6', 'day_7', 'day_8', 'day_9', 'day_10']:
                email_body = 'day_3'

        if not email_body: continue

        # --- CORE LOGIC BLOCK: WHEN TO SEND ---
        reminder = ""
        should_send = False

        # A. CONTROL GROUP
        if row.get('condition') == 'control':
            if elapsed.days == 20:
                reminder = 'reminder_post'
                should_send = True
            elif elapsed.days == 23:
                reminder = 'reminder_post_final'
                should_send = True

        # B. POST-COURSE SURVEY (Standard completion)
        elif next_survey == 'post':
            if elapsed.days == 1:
                reminder = ''
                should_send = True
            elif elapsed.days == 2:
                reminder = 'reminder_post'
                should_send = True
            elif elapsed.days == 3:
                reminder = 'reminder_post_final'
                should_send = True

        # C. DAY 1 (First Day Logic)
        elif next_survey == 'day_1':
            if elapsed.days == 1:
                reminder = ''  # Standard email
                should_send = True
            elif elapsed.days == 2:
                reminder = 'reminder_days'  # Reminder 1
                should_send = True
            elif elapsed.days == 3:
                reminder = 'reminder_days_final'  # Reminder 2 (Using final text, or swap to standard reminder if preferred)
                should_send = True
            elif elapsed.days == 4:
                reminder = 'reminder_days_final'  # Reminder 3 (Final)
                should_send = True
            elif elapsed.days == 5:
                reminder = ''
                email_body = 'dropout'  # Trigger Dropout
                should_send = True
            elif elapsed.days == 8:
                reminder = 'reminder_dropout'  # Dropout follow-up
                email_body = 'dropout'
                should_send = True

        # D. DAILY PRACTICE (Days 2-10)
        else:
            if elapsed.days == 1:
                reminder = ''  # Standard email
                should_send = True
            elif elapsed.days == 2:
                reminder = 'reminder_days'  # Reminder 1
                should_send = True
            elif elapsed.days == 3:
                reminder = 'reminder_days'  # Reminder 2
                should_send = True
            elif elapsed.days == 4:
                reminder = 'reminder_days_final'  # Reminder 3 (Final)
                should_send = True
            elif elapsed.days == 5:
                reminder = ''
                email_body = 'dropout'  # Trigger Dropout
                should_send = True
            elif elapsed.days == 8:
                reminder = 'reminder_dropout'  # Dropout follow-up
                email_body = 'dropout'
                should_send = True

        if not should_send:
            continue

        # 4. CONSTRUCT AND SEND
        footer = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        email_class = Emails(reminder, email_body, survey_url, footer)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = "A reminder for the Learning Mindfulness Online course"
        msg["From"] = GMAIL_USER
        msg["To"] = row['email']

        msg.attach(MIMEText(email_class.make_plaintext(), "plain"))
        msg.attach(MIMEText(email_class.make_email(), "html"))

        try:
            server.sendmail(GMAIL_USER, row['email'], msg.as_string())
            print(
                f"Sent: {row['email']} | Survey: {next_survey} | Day: {elapsed.days} | Type: {reminder or 'Standard'}")
            emails_sent += 1
        except Exception as e:
            print(f"Error sending to {row['email']}: {e}")

    server.quit()
    print(f"Run Complete. Total emails sent: {emails_sent}")

if __name__ == "__main__":
    process_and_send()