from __future__ import print_function
import os
import pickle
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ['https://www.googleapis.com/auth/drive']

# ---- CONFIG ----
TEMPLATE_FILE_ID = '1jzoB4MqcB2GIJZLNaPrmv7V6T7ZcHQFJGN3nnb9m5Zk'   # Template sheet ID
NAMES_FILE = 'names.txt'

def authenticate():
    """Authenticate and return a Drive API service object."""
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('cred-os-labs.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return build('drive', 'v3', credentials=creds)

def read_names():
    """Read names from a text file."""
    with open(NAMES_FILE, 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f if line.strip()]
    return lines

def make_copies(service, names):
    """Make copies of the template for each name."""
    # First, get the parent folder of the template file
    template_metadata = service.files().get(fileId=TEMPLATE_FILE_ID, fields='parents').execute()
    parents = template_metadata.get('parents', [])
    parent_folder_id = parents[0] if parents else None

    for name in names:
        file_metadata = {
            'name': name,
            'parents': [parent_folder_id] if parent_folder_id else None
        }
        copied_file = service.files().copy(
            fileId=TEMPLATE_FILE_ID,
            body=file_metadata
        ).execute()
        print(f"Created copy: {copied_file.get('name')} (ID: {copied_file.get('id')})")

def main():
    service = authenticate()
    names = read_names()
    make_copies(service, names)

if __name__ == '__main__':
    main()
