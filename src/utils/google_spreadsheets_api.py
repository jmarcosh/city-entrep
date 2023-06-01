from __future__ import print_function

import os.path
import pandas as pd

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# If modifying these scopes, delete the file token.json.


def get_access_to_spreadhseets_api(scopes=['https://www.googleapis.com/auth/spreadsheets']):
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    creds = None
    if os.path.exists(os.path.join(os.path.dirname(__file__), '../../../config_files/token.json')):
        # TODO create new token if scopes are different from existing token
        creds = Credentials.from_authorized_user_file(
            os.path.join(os.path.dirname(__file__), '../../../config_files/token.json'), scopes)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                os.path.join(os.path.dirname(__file__), '../../../config_files/google_spreadsheets_api_credentials.json'), scopes)
            creds = flow.run_local_server(port=0)
        # Save the google_spreadsheets_api_credentials for the next run
        with open(os.path.join(os.path.dirname(__file__), '../../../config_files/token.json'), 'w') as token:
            token.write(creds.to_json())
    return creds


# The ID and range of a sample spreadsheet.
def read_spreadsheet(spreadsheet_id, sheet='Sheet1', creds=get_access_to_spreadhseets_api()):
    """Reads values using Sheets API.
        Returns dataframe with values.
        """
    SAMPLE_SPREADSHEET_ID = spreadsheet_id
    SAMPLE_RANGE_NAME = sheet

    try:
        service = build('sheets', 'v4', credentials=creds)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                    range=SAMPLE_RANGE_NAME).execute()
        values = result.get('values', [])

        if not values:
            print('No data found.')
            return

        headers = values.pop(0)
        if len(headers) > max(len(sublist) for sublist in values) and len(values) > 0:
            values[0] = values[0] + [''] * (len(headers) - len(values[0]))
        df = pd.DataFrame(values, columns=headers)
    except HttpError as err:
        print(err)

    return df


def create_spreadsheet(title, creds=get_access_to_spreadhseets_api()):
    """
    Creates the Sheet the user has access to.
        """
    # creds, _ = google.auth.default()
    # pylint: disable=maybe-no-member
    try:
        service = build('sheets', 'v4', credentials=creds)
        spreadsheet = {
            'properties': {
                'title': title
            }
        }
        spreadsheet = service.spreadsheets().create(body=spreadsheet,
                                                    fields='spreadsheetId') \
            .execute()
        print(f"Spreadsheet ID: {(spreadsheet.get('spreadsheetId'))}")
        return spreadsheet.get('spreadsheetId')
    except HttpError as error:
        print(f"An error occurred: {error}")
        return error


def update_values(spreadsheet_id, df, creds=get_access_to_spreadhseets_api(), range_name='Sheet1',
                  erase_values=False, value_input_option='USER_ENTERED'):
    """
    Creates the batch_update the user has access to.
        """
    # pylint: disable=maybe-no-member
    try:
        service = build('sheets', 'v4', credentials=creds)
        if erase_values:
            body = {
                'values': [[''] * erase_values[1]] * erase_values[0]
            }
            result = service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id, range=range_name, valueInputOption=value_input_option,
                body=body).execute()
            print(f"{result.get('updatedCells')} cells erased.")
        df.fillna('', inplace=True)
        values = [list(df.columns)] + df.values.tolist()
        body = {
            'values': values
        }
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=range_name, valueInputOption=value_input_option, body=body).execute()
        print(f"{result.get('updatedCells')} cells updated.")
        return result
    except HttpError as error:
        print(f"An error occurred: {error}")
        return error


# TODO add method for sharing permissions
# https://stackoverflow.com/questions/53368658/using-google-api-to-create-and-share-a-spreadsheet