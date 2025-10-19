import pandas as pd
from math import ceil
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
import os

# === CONFIGURATION ===
input_csv = "students.csv"
output_excel = "student_rooms.xlsx"

rooms = ["EG106", "EG306", "EG405", "ED202", "ED011", "EG207", "EG208", "PR706", "EG105", "PR705"]
capacities = [18,18,18,20,20,18,18,20,18,18]

# === GOOGLE API SETUP ===
SCOPES = ['https://www.googleapis.com/auth/drive.file']

def authenticate_google():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return creds

# === ROOM ASSIGNMENT ===
df = pd.read_csv(input_csv)
if len(rooms) != len(capacities):
    raise ValueError("Rooms and capacities must match in length.")

students = df.to_dict("records")
assignments = []
total_capacity = sum(capacities)
num_intervals = ceil(len(students) / total_capacity)

student_index = 0
for interval in range(num_intervals):
    for room, cap in zip(rooms, capacities):
        for _ in range(cap):
            if student_index >= len(students):
                break
            student = students[student_index]
            student_index += 1
            assignments.append({
                "Interval": interval + 1,
                "First name": student["First name"],
                "Last name": student["Last name"],
                "Email address": student["Email address"],
                "Group": student["Grupa"],
                "Room": room
            })

writer = pd.ExcelWriter(output_excel, engine="openpyxl")
for interval in range(1, num_intervals + 1):
    df_interval = pd.DataFrame([
        a for a in assignments if a["Interval"] == interval
    ])
    df_interval = df_interval.drop(columns=["Interval"])
    df_interval.to_excel(writer, sheet_name=f"Interval {interval}", index=False)
writer.close()

print(f"Excel file created: {output_excel}")

# === UPLOAD TO GOOGLE DRIVE ===
creds = authenticate_google()
drive_service = build('drive', 'v3', credentials=creds)

file_metadata = {
    'name': 'Student Room Assignments',
    'mimeType': 'application/vnd.google-apps.spreadsheet'
}
media = MediaFileUpload(output_excel, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

uploaded_file = drive_service.files().create(
    body=file_metadata,
    media_body=media,
    fields='id'
).execute()

file_id = uploaded_file.get('id')

# Make it shareable
drive_service.permissions().create(
    fileId=file_id,
    body={'role': 'reader', 'type': 'anyone'},
).execute()

shareable_link = f"https://docs.google.com/spreadsheets/d/{file_id}"
print(f"Link: {shareable_link}")
