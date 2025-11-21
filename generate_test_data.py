import json
import datetime as dt
import os

# Create the directory if it doesn't exist
if not os.path.exists('webservice'):
    os.makedirs('webservice')


# Helper to get a date string relative to today
def get_date(days_ago):
    d = dt.datetime.now() - dt.timedelta(days=days_ago)
    return d.strftime("%m/%d/%Y")

gmail_stub = 'max.lovell77'
# Define test scenarios
scenarios = [
    # 1. Happy Path: Finished 'day_1' yesterday (1 day ago) -> Should get Day 2 Link
    {"filename": "test_happy_path.json", "email": gmail_stub+"+happy@gmail.com",
     "survey": "SV_efYBX7JoyriIFed", "date": get_date(1), "condition": "mental"},

    # 2. Reminder Path: Finished 'day_2' 3 days ago -> Should get Reminder 2
    {"filename": "test_reminder.json", "email": gmail_stub+"+remind@gmail.com",
     "survey": "SV_8tRRT3RDUgZjLmJ", "date": get_date(3), "condition": "mental"},

    # 3. Dropout Path: Finished 'day_4' 5 days ago -> Should get Dropout Email
    {"filename": "test_dropout.json", "email": gmail_stub+"+drop@gmail.com",
     "survey": "SV_cHZ2uoqcwNYHtWJ", "date": get_date(5), "condition": "mental"},

    # 4. Control Group: Finished 'pre' 20 days ago -> Should get Post-Test Link
    {"filename": "test_control.json", "email": gmail_stub+"+control@gmail.com",
     "survey": "SV_6liqwhsa4LJndL7", "date": get_date(20), "condition": "control"},

    # 5. Too Old: Finished 'day_1' 50 days ago -> Should be IGNORED
    {"filename": "test_ignore.json", "email": gmail_stub+"+ignore@gmail.com",
     "survey": "SV_efYBX7JoyriIFed", "date": get_date(50), "condition": "mental"},
]

# Write files
for s in scenarios:
    data = {
        "email": s["email"],
        "survey": s["survey"],
        "date": s["date"],
        "condition": s["condition"]
    }
    with open(f'webservice/{s["filename"]}', 'w') as f:
        json.dump(data, f)

print(f"Created {len(scenarios)} test files in /webservice folder.")