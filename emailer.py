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

# --- LOAD DOTENV (For Local Dev Only) ---
from dotenv import load_dotenv
load_dotenv() # This does nothing if no .env file is present (like on GitHub)

# --- CONFIGURATION (Loaded from GitHub Secrets) ---
SSH_HOST = "unix.sussex.ac.uk"
SSH_USER = os.environ.get("SUSSEX_USER")
SSH_PASS = os.environ.get("SUSSEX_PASS")

# List both folder paths here
REMOTE_PATHS = [
    "/its/home/mel29/metacog/webservice/",  # Study 1: Mental States
    "/its/home/mel29/breath/webservice/"  # Study 2: Breath Awareness
]

GMAIL_USER = os.environ.get("GMAIL_USER")
GMAIL_PASS = os.environ.get("GMAIL_PASS")


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

        total_downloaded = 0

        # Loop through BOTH folders
        for remote_path in REMOTE_PATHS:
            print(f"Checking folder: {remote_path}")
            try:
                all_files = sftp.listdir(remote_path)
                json_files = [f for f in all_files if f.endswith(".json")]

                for file in json_files:
                    # Create full paths
                    remote_file = os.path.join(remote_path, file).replace("\\", "/")
                    local_file = os.path.join(local_dir, file)

                    # Download
                    sftp.get(remote_file, local_file)
                    total_downloaded += 1

            except FileNotFoundError:
                print(f"Warning: Could not find folder {remote_path}")
                continue

        print(f"Download complete. Total files gathered: {total_downloaded}")
        sftp.close()
        ssh.close()
    except Exception as e:
        print(f"SSH Connection Failed: {e}")
        exit(1)

# --- 2. PROCESS DATA ---
def process_and_send():
    # 1. DOWNLOAD DATA (Ensure you have the SSH/Paramiko setup above this)
    download_data()

    survey_names = ['pre', 'day_1', 'day_2', 'day_3', 'day_4', 'day_5',
                    'day_6', 'day_7', 'day_8', 'day_9', 'day_10', 'post']

    # --- STUDY CONFIGURATIONS ---

    # Study 1: World Mental States
    SURVEYS_MENTAL = {
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

    # Study 2: Breath Awareness
    SURVEYS_BREATH = {
        'pre': ['SV_77GRlMXRzCRvbgO'],
        'day_1': ['SV_0UqMgw7opwUBsp0', 'SV_eVD50YlnreqHPeu'],
        'day_2': ['SV_1KTcbhp97aMsT8G', 'SV_8wWXdKWwB6PmRpQ'],
        'day_3': ['SV_9WQC7OK4XgA6u7s', 'SV_3F4yThgDP5no2fs'],
        'day_4': ['SV_2071gXkwXXxHhrM', 'SV_eeULxjJiThIUJbo'],
        'day_5': ['SV_bQ5EPRvJaZWEKaO', 'SV_1AnnMndzN0tu4yG'],
        'day_6': ['SV_2ccwKIVLKbjjuTk', 'SV_5tmjjk9Dpxufv6u'],
        'day_7': ['SV_errJ339HYHmYmh0', 'SV_7Xaly0kNutBbaiW'],
        'day_8': ['SV_b7uWUqNYHPgufXw', 'SV_7OiRNiarIJr8OJo'],
        'day_9': ['SV_1RfiSGF82QU4uhw', 'SV_eUUZbaMxDq1ExQG'],
        'day_10': ['SV_0pjWPj6lWsKPsVg', 'SV_8dzuF81uf8qQ8Oq'],
        'post': ['SV_23lPSvXFISsaPtk'],
        'materials': ['SV_6D0mranYslx7qHc']
    }

    # Combine for file scanning
    ALL_SURVEYS = {k: SURVEYS_MENTAL.get(k, []) + SURVEYS_BREATH.get(k, []) for k in
                   set(SURVEYS_MENTAL) | set(SURVEYS_BREATH)}

    # Helper: Determine which study a participant is in
    def get_study_config(survey_id):
        for k, v in SURVEYS_MENTAL.items():
            if survey_id in v: return 'mental', SURVEYS_MENTAL
        for k, v in SURVEYS_BREATH.items():
            if survey_id in v: return 'breath', SURVEYS_BREATH
        return None, None

    # 2. LOAD AND PARSE DATA
    file_list = glob.glob('webservice/*.json')
    datal = []

    for file in file_list:
        with open(file) as f:
            try:
                datad = json.load(f)
                # Check if survey is in our master list
                found = False
                for key, value in ALL_SURVEYS.items():
                    if datad["survey"] in value:
                        found = True
                        entry = {'email': datad["email"]}
                        if "condition" in datad:
                            entry['condition'] = datad["condition"]
                        entry[key] = datad["date"]
                        datal.append(entry)
                        break
            except (json.JSONDecodeError, KeyError):
                continue

    if not datal:
        print("No participant data found.")
        return

    # Consolidate data by email
    datac = defaultdict(dict)
    for d in datal:
        datac[d["email"]].update(d)

    datafl = []
    for d in list(datac.values()):
        datafl.append(pd.DataFrame([d]))

    dataset = pd.concat(datafl)
    for col in survey_names:
        if col not in dataset.columns:
            dataset[col] = None

    # 3. CONNECT TO GMAIL
    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.login(GMAIL_USER, GMAIL_PASS)
    except Exception as e:
        print(f"Failed to login to Gmail: {e}")
        return

    emails_sent = 0

    # 4. PROCESS PARTICIPANTS
    total_participants = len(dataset)
    for i, (index, row) in enumerate(dataset.iterrows(), 1):

        row_na = row.dropna()
        if len(row_na) <= 2: continue

        # Get Date
        try:
            last_date_str = row_na.iloc[-1]
            datef = dt.datetime.strptime(last_date_str, "%m/%d/%Y")
        except (ValueError, TypeError):
            continue

        today = dt.datetime.today()
        elapsed = today - datef
        last_survey = row_na.index[-1]

        # Identify Study Config
        study_type, current_surveys = get_study_config(
            row_na.iloc[-2])  # -2 is usually the Survey ID string in the raw row
        # Better safety: Check the Survey ID from the file logic, but simplified here:
        # We re-infer study type based on the last survey completed
        # (Since the dataframe only has dates, we rely on logic or careful reconstruction.
        #  For safety in this script, we need the ID.
        #  Since the dataframe structure lost the exact ID, we rely on the 'condition' or context.
        #  ACTUALLY: The best way is to check 'condition' or assume Mental if not clearer.
        #  FIX: We will assume 'mental' unless we match specific logic, or ideally,
        #  we should have stored the ID in the dataframe.
        #  Workaround: Use the survey maps on 'last_survey' key if specific unique dates aren't there.)

        # BETTER APPROACH: Logic based on condition if possible, or assume common config.
        # Since IDs are lost in the dataframe (it only stores dates), we must rely on the fact
        # that logic is shared, EXCEPT for control group.
        # We will try to guess study based on condition if possible, or defaulting to 'breath' logic
        # if strictly "3 consecutive reminders" is desired for all.

        # HOWEVER, to be precise, let's look at 'condition'.
        # If we can't distinguish, we default to the stricter schedule (Breath).

        if last_survey == 'post': continue

        # Calculate Next Survey
        try:
            survey_index = survey_names.index(last_survey)
            next_survey = survey_names[survey_index + 1]
        except (ValueError, IndexError):
            continue

        # Select Survey ID (Try Mental first, then Breath)
        # This part is tricky without the original ID, but we will try to match condition
        is_mental = False
        if row.get('condition') == 'world': is_mental = True  # Only Mental has 'world'

        # Determine proper ID list to use
        if is_mental:
            active_dict = SURVEYS_MENTAL
            study_mode = 'mental'
        else:
            active_dict = SURVEYS_BREATH
            study_mode = 'breath'  # Default to Breath logic for Control

        # Get Survey ID
        if next_survey == 'post' or elapsed.days > 4 or (row.get('condition') == 'control' and elapsed.days >= 20):
            survey_ID = str(active_dict['post'][0])
        elif row.get('condition') == 'mental':
            survey_ID = str(active_dict[next_survey][0])
        elif row.get('condition') == 'world':
            # Safe access for world
            ids = active_dict[next_survey]
            survey_ID = str(ids[1] if len(ids) > 1 else ids[0])
        else:
            # Default / Control
            survey_ID = str(active_dict[next_survey][0])

        survey_url = "https://universityofsussex.eu.qualtrics.com/jfe/form/" + survey_ID + "?RecipientEmail=" + row[
            'email']

        # Determine Email Body
        email_body = None
        if row.get('condition') == 'control':
            if elapsed.days >= 20 and elapsed.days <= 23:
                email_body = 'control_post'
        elif row.get('condition') in ['mental', 'world']:
            if next_survey == 'post' or elapsed.days > 4:
                email_body = 'post'
            elif next_survey == 'day_1':
                email_body = 'day_1'
            elif next_survey == 'day_2':
                email_body = 'day_2'
            else:
                email_body = 'day_3'

        if not email_body: continue

        # --- TIMING LOGIC ---
        reminder = ""
        should_send = False

        # A. CONTROL GROUP
        if row.get('condition') == 'control':
            if study_mode == 'mental':
                # Study 1: Days 20 & 23
                if elapsed.days == 20:
                    reminder = 'reminder_post'
                    should_send = True
                elif elapsed.days == 23:
                    reminder = 'reminder_post_final'
                    should_send = True
            else:
                # Study 2: Days 20, 21, 22, 23 ("reminders for 3 consecutive days")
                if elapsed.days == 20:
                    reminder = 'reminder_post'
                    should_send = True
                elif elapsed.days == 21:
                    reminder = 'reminder_post'
                    should_send = True
                elif elapsed.days == 22:
                    reminder = 'reminder_post'
                    should_send = True
                elif elapsed.days == 23:
                    reminder = 'reminder_post_final'
                    should_send = True

        # B. POST-TEST REMINDERS
        elif next_survey == 'post':
            # "single reminder to take the post-test 3 days later"
            if elapsed.days == 1:
                reminder = ''
                should_send = True
            elif elapsed.days == 2:
                reminder = 'reminder_post'
                should_send = True
            elif elapsed.days == 3:
                reminder = 'reminder_post'
                should_send = True
            elif elapsed.days == 4:
                reminder = 'reminder_post_final'
                should_send = True

        # C. DAILY PRACTICE (Day 1 & Days 2-10)
        # "reminder is sent each morning for 3 days"
        else:
            if elapsed.days == 1:
                reminder = ''
                should_send = True
            elif elapsed.days == 2:
                reminder = 'reminder_days'  # Reminder 1
                should_send = True
            elif elapsed.days == 3:
                reminder = 'reminder_days'  # Reminder 2
                should_send = True
            elif elapsed.days == 4:
                reminder = 'reminder_days_final'  # Reminder 3
                should_send = True
            elif elapsed.days == 5:
                reminder = ''
                email_body = 'dropout'
                should_send = True
            elif elapsed.days == 8:
                reminder = 'reminder_dropout'
                email_body = 'dropout'
                should_send = True

        if not should_send:
            continue

        # SEND EMAIL
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
            print(f"Sent: {row['email']} | Survey: {next_survey} | Day: {elapsed.days}")
            emails_sent += 1
        except Exception as e:
            print(f"Error sending to {row['email']}: {e}")

    server.quit()
    print(f"Run Complete. Total emails sent: {emails_sent}")

if __name__ == "__main__":
    process_and_send()