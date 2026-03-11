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

load_dotenv()

# --- CONFIGURATION (Loaded from GitHub Secrets) ---
SSH_HOST = "unix.sussex.ac.uk"
SSH_USER = os.environ.get("SUSSEX_USER")
SSH_PASS = os.environ.get("SUSSEX_PASS")

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

        for remote_path in REMOTE_PATHS:
            print(f"Checking folder: {remote_path}")
            try:
                all_files = sftp.listdir(remote_path)
                json_files = [f for f in all_files if f.endswith(".json")]

                for file in json_files:
                    remote_file = os.path.join(remote_path, file).replace("\\", "/")
                    local_file = os.path.join(local_dir, file)
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
    # download_data() # COMMENT OUT FOR TESTING

    survey_names = ['pre', 'day_1', 'day_2', 'day_3', 'day_4', 'day_5',
                    'day_6', 'day_7', 'day_8', 'day_9', 'day_10', 'post']

    # --- STUDY CONFIGURATIONS ---
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

    # --- Create Reverse Lookup Maps ---
    survey_to_study = {}
    for survey_list in SURVEYS_MENTAL.values():
        for s_id in survey_list:
            survey_to_study[s_id] = 'mental'

    for survey_list in SURVEYS_BREATH.values():
        for s_id in survey_list:
            survey_to_study[s_id] = 'breath'

    # Combine for file phase checking
    ALL_SURVEYS = {k: SURVEYS_MENTAL.get(k, []) + SURVEYS_BREATH.get(k, []) for k in
                   set(SURVEYS_MENTAL) | set(SURVEYS_BREATH)}

    # 2. LOAD AND PARSE DATA
    file_list = glob.glob('webservice/*.json')
    datal = []

    for file in file_list:
        with open(file) as f:
            try:
                datad = json.load(f)
                survey_id = datad.get("survey")

                # Check if this survey belongs to either of our studies
                if survey_id in survey_to_study:
                    study_type = survey_to_study[survey_id]

                    for key, value in ALL_SURVEYS.items():
                        if survey_id in value:
                            entry = {
                                'email': datad["email"],
                                'study': study_type,  # <-- Tag the exact study here
                                key: datad["date"]
                            }
                            if "condition" in datad:
                                entry['condition'] = datad["condition"]
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

    dataset = pd.concat([pd.DataFrame([d]) for d in datac.values()], ignore_index=True)

    # FIX: Force exact chronological column order so .iloc[-1] is always the latest step
    base_cols = ['email', 'study']
    if 'condition' in dataset.columns:
        base_cols.append('condition')

    ordered_cols = base_cols + survey_names
    final_cols = [col for col in ordered_cols if col in dataset.columns]
    dataset = dataset.reindex(columns=final_cols)

    # 3. CONNECT TO GMAIL
    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.login(GMAIL_USER, GMAIL_PASS)
    except Exception as e:
        print(f"Failed to login to Gmail: {e}")
        return

    emails_sent = 0

    # 4. PROCESS PARTICIPANTS
    for i, row in dataset.iterrows():
        row_na = row.dropna()
        # If length is just the base columns (email, study, condition), they have no valid survey dates logged
        if len(row_na) <= len(base_cols): continue

        try:
            last_date_str = row_na.iloc[-1]
            datef = dt.datetime.strptime(last_date_str, "%m/%d/%Y")
        except (ValueError, TypeError):
            continue

        today = dt.datetime.today()
        elapsed = today - datef
        last_survey = row_na.index[-1]

        if last_survey == 'post': continue

        try:
            survey_index = survey_names.index(last_survey)
            next_survey = survey_names[survey_index + 1]
        except (ValueError, IndexError):
            continue

        # Use the natively tagged study and condition
        study_mode = row.get('study')
        condition = row.get('condition', 'unknown')
        is_control_group = (condition == 'control')

        if study_mode == 'mental':
            active_dict = SURVEYS_MENTAL
        else:
            active_dict = SURVEYS_BREATH

        # Get Survey ID
        if next_survey == 'post' or elapsed.days > 4 or (is_control_group and elapsed.days >= 20):
            survey_ID = str(active_dict['post'][0])
        elif condition == 'mental':
            survey_ID = str(active_dict[next_survey][0])
        elif condition == 'world':
            ids = active_dict[next_survey]
            survey_ID = str(ids[1] if len(ids) > 1 else ids[0])
        else:
            survey_ID = str(active_dict[next_survey][0])

        survey_url = f"https://universityofsussex.eu.qualtrics.com/jfe/form/{survey_ID}?RecipientEmail={row['email']}"

        # Determine Email Body
        email_body = None
        if is_control_group:
            if 20 <= elapsed.days <= 23:
                email_body = 'control_post'
        elif condition in ['mental', 'world']:
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

        if is_control_group:
            if study_mode == 'mental':
                # Control group logic for Mental
                if elapsed.days == 20:
                    reminder, should_send = 'reminder_post', True
                elif elapsed.days == 23:
                    reminder, should_send = 'reminder_post_final', True
            else:
                # Control group logic for Breath: 20 days, then 3 consecutive reminders
                if elapsed.days in [20, 21, 22]:
                    reminder, should_send = 'reminder_post', True
                elif elapsed.days == 23:
                    reminder, should_send = 'reminder_post_final', True

        elif next_survey == 'post':
            # Qualtrics sends the immediate post-test invite on Day 1.
            # Python waits 3 days, and sends a SINGLE reminder on Day 4.
            if elapsed.days == 4:
                reminder, should_send = 'reminder_post_final', True

        else:
            # Daily practice reminders (Days 2-10).
            # Qualtrics handles the initial Day 1 send.
            # Python handles the reminders for the next 3 days.
            if elapsed.days in [2, 3]: # TODO: maybe just needs to be 3 here not 2 and 3??
                reminder, should_send = 'reminder_days', True
            elif elapsed.days == 4:
                reminder, should_send = 'reminder_days_final', True
            # If still incomplete on Day 5, send post-test (dropout protocol)
            elif elapsed.days == 5:
                reminder, email_body, should_send = '', 'dropout', True
            # Single reminder 3 days later for the dropout post-test
            elif elapsed.days == 8:
                reminder, email_body, should_send = 'reminder_dropout', 'dropout', True

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
            print(f"Sent: {row['email']} | Survey (might be post-test...): {next_survey} | Target ID: {survey_ID} | Day: {elapsed.days}")
            emails_sent += 1
        except Exception as e:
            print(f"Error sending to {row['email']}: {e}")

    server.quit()
    print(f"Run Complete. Total emails sent: {emails_sent}")


if __name__ == "__main__":
    process_and_send()