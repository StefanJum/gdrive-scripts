import os
import time
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

def authenticate_drive(credentials_path):
    """
    Authenticate and connect to Google Drive and Google Sheets API.
    Args:
        credentials_path (str): Path to the service account credentials JSON file.
    Returns:
        gspread.Client: Authenticated gspread client.
        GoogleDrive: Authenticated PyDrive2 GoogleDrive client.
    """
    # Google Sheets API scope
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]

    creds = ServiceAccountCredentials.from_json_keyfile_name('cred.json', scope)

    # Authenticate for gspread
    gspread_client = gspread.authorize(creds)

    # Authenticate for PyDrive2
    gauth = GoogleAuth()
    gauth.credentials = creds
    drive_client = GoogleDrive(gauth)

    return gspread_client, drive_client

def get_files_in_folder(drive_client, folder_name):
    """
    Fetch all files from a specific Google Drive folder.
    Args:
        drive_client: Authenticated PyDrive2 GoogleDrive client.
        folder_name (str): Name of the target folder.
    Returns:
        list: List of file objects in the folder.
    """
    # Search for the folder by its name
    folder_id = '1lslhMrnqoj8ayR1oH65w8S4AegP2bpap'
    folder_id = '1cwOe9P4UsgF0y3t4YBWFUJZoYbtgxvFa'
    print(f"Found folder with ID: {folder_id}")

    # List all files within the folder
    file_query = f"'{folder_id}' in parents and trashed = false"
    print(file_query)
    files_in_folder = drive_client.ListFile({'q': file_query}).GetList()

    print(f"Found {len(files_in_folder)} file(s) in folder '{folder_name}'.")
    print(files_in_folder)
    return files_in_folder

def process_sheets_in_folder(gspread_client, drive_files, output_file="output.xlsx"):
    """
    Extract columns 1, 3, and 9 from sheets named Lab1 to Lab9 in the given Google Sheets files.
    Args:
        gspread_client: Authenticated gspread client.
        drive_files (list): List of files in the target folder.
        output_file (str): Local path to save the extracted results.
    """
    extracted_data = []

    # drive_files = [
    # '1Mey9GgnJXIGRqh08u61x9QexG9TuL0LmeTPYx6bYp4Y',
    # '1a8Ds1M4Y8B84VgLlnLRbDDYq9_HNHNz8-5ybE05iYV0',
    # '1qGQPoErimOu-B0-i2C4y4KKooiEpP42LjTK99nrT_NY',
    # '1JqYWd83X_gGstelzo-Emkv9ZjT58txSZZnL_uETwm88',
    # '1XU_5bfQ4rYYFq5-Ep1lQniEMrXn64n1G_yj9oOFQsHM',
    # '1Lvcga577OZStZ2EQHRunxaFBhalwoMwWfmiKzjKjDUU',
    # '19oq7nSdcg2IuMpu9DhGhlKat63_Vj-huygnRAxtj2AY',
    # '1EzgvvNdzUzsJ6dUSrNI7IO-FBcNni9d5PDVmpfSKX9s',
    # '1s8AQ6W0CGEQM2Ux8auFra0ob-l4rvHslEFxqXPDr0XI',
    # '1OE76xs4d3mTiDs5MWVjEQsN9si3qDYsyF34-YAMX2ik',
    # '1eQAxqanMCHSRbxQT8q43iOLZpSQL6JtWgYikYWTA1LU',
    # '16HH2VeNrLrkmoU_OT799UgazhDc__V7kUDuyqUA3g_Q',
    # '1oZKNokJyRvApNY7B6_Ys5TTMeIa3cRvqxpgbfc0fbAM',
    # '17ImfZmH8T05l_naxZo4GkiYqKQYl-LlmnlD8rqeWXJA',
    # '1FhD0LdrFa9W4HLpupWm2G6ICt-eapodP9VQ-48waLYo',
    # '1S6AzOF9VxdNisZ2PISjNLdQbvhQla3Eh9PaUnc86O24',
    # '16aDlPHjS_UpRxkEEc-pBA4KlZlYOVUCRD-XZ1RkERGA',
    # '1jjrnIOEFNYpiS4FlZG_N1mv1QKZaRR2gwBipY9ZmUC0',
    # '15AMpTohtA89W5rWcJ7P-Kuv_6wGpLj8om5ZMPEIHcmo',
    # '1SNxtcxskAubgM-l4IIRlByTxRkNXf9y4Lqqmjq9Uh3I',
    # '1GOFWxayaplN0SCMQ-lxPZL5dxZVEqjov-MTxLMncXSs',
    # '1ynXzCnZYn3UwNoD5rKNET0uJJv5It6IsEqCgE820iYc',
    # '187R-B5UmTUM6ywNTL0bE7NCFRRO_2e4tTIBTixwqKUU',
    # '1osTEHAsH7JpOWEtJtFBjmZULPgD2rH4dEtwjI6z6sNs',
    # '1GR-e3jqe12WXwdYAvi3vyX-VBxNHRpnimQykyIgXKpo',
    # '1An2NLeygp5IgVZef2HMpJPwknfUfsOuQRuNsuD-t7vo',
    # '187dnGoHRw8iwE_sTVpWqBdgzViL_EU7HgAhcD5DOEWA',
    # '15SAuALvnYxDQ_5Z_VsliPIM6GRSh_Kc-64uA13rUnSE',
    # '1s7EcxD2XCi1WbDeS48BpOt8ftERcuK0c7ZUk7zqRrmM',
    # '10rTkfDks7Ys0a9gGs4xbEftUcd3XeAjPVZ2864XB-wI',
    # '1kFopkWImHqWMxsndetNxFdGmyd6mVSD4H0_d11eD5s0',
    # '19S4MsCrj0FQ_oGw2-OcwrCk97RkjlMpb12LZVuxup9Q',
    # '1HclYMobmUzKJN4pEpOyBAUJmCCattY9fePwVRdpqPZs',
    # '1v28R-TADdPhyVprEioZOGp_k6xQztyVIVa1w-GcMApg'
    # ]

    drive_files = [
    '16HH2VeNrLrkmoU_OT799UgazhDc__V7kUDuyqUA3g_Q',
    '17ImfZmH8T05l_naxZo4GkiYqKQYl-LlmnlD8rqeWXJA'
    ]

    for file in drive_files:
        #file_name = file['title']
        #print(f"Processing file: {file_name}")

        spreadsheet = gspread_client.open_by_key(file)

        # Check for sheets named Lab1 to Lab9
        for worksheet in spreadsheet.worksheets():
            #print(worksheet.title)
            worksh_title = worksheet.title.replace(" ", "")
            if worksh_title.startswith("Lab") and len(worksh_title) > 4 and worksh_title[3].isdigit() and worksh_title[4].isdigit():
                sheet_number = int(worksh_title[3:5])
                if 10 <= sheet_number <= 12:
                    print(sheet_number)
                    print (file)
                    #print("TEST")
                    #continue
                    print(f"  Extracting from sheet: {worksh_title}")

                    # Convert to DataFrame and extract desired columns
                    df = get_as_dataframe(worksheet, evaluate_formulas=True, header=None)
                    columns_to_extract = [0, 2, 8]  # 0-indexed
                    df_extracted = df.iloc[:, [col for col in columns_to_extract if col < df.shape[1]]]

                    # Add source file and sheet info
                    df_extracted["Source_File"] = file
                    df_extracted["Source_Sheet"] = worksh_title

                    extracted_data.append(df_extracted)
        time.sleep(8)

    # Save all data to a local Excel file
    if extracted_data:
        print("Saving extracted data to output file...")
        combined_df = pd.concat(extracted_data, ignore_index=True)
        combined_df.to_csv('output-last-labs2.csv', index=False)
        print(f"Data saved to {output_file}")
    else:
        print("No matching data found.")

if __name__ == "__main__":
    # Path to your Google service account credentials
    credentials_path = "./credentials.json"
    folder_name = "Your_Target_Folder_Name"  # Replace with your folder's name
    output_file = "extracted_lab_data.xlsx"

    # Authenticate clients
    gspread_client, drive_client = authenticate_drive(credentials_path)

    # Get files in the target folder
    drive_files = get_files_in_folder(drive_client, folder_name)

    # Process the sheets and save the output
    process_sheets_in_folder(gspread_client, drive_files, output_file)

