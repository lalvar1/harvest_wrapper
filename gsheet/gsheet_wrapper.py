import httplib2
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta, date


class GoogleRunner:
    """
    A class to manage Google Sheets
    """

    def __init__(self, spreadsheet_id, credentials_file, entries_sheet, logs_sheet, roles_sheet,
                 weekly_tasks_sheet, projects_sheet):
        self.spreadsheet_id = spreadsheet_id
        self.credentials_file = credentials_file
        self.entries_sheet = entries_sheet
        self.logs_sheet = logs_sheet
        self.roles_sheet = roles_sheet
        self.weekly_tasks_sheet = weekly_tasks_sheet
        self.projects_sheet = projects_sheet

    def google_auth(self):
        """
        oauth2 authentication agains Google Gsheet API
        :return:
        """
        scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        try:
            credentials = ServiceAccountCredentials.from_json_keyfile_name(self.credentials_file, scope)
            service = build('sheets', 'v4', http=credentials.authorize(httplib2.Http()), cache_discovery=False)
            return service
        except Exception as e:
            print(f"Error connecting: {e}. Retrying connection...")
            credentials = ServiceAccountCredentials.from_json_keyfile_name(self.credentials_file, scope)
            service = build('sheets', 'v4', http=credentials.authorize(httplib2.Http()), cache_discovery=False)
            return service

    def gsheet_append(self, gsheet_range, values):
        """
        Append rows to Google Sheet
        :param ghseet_range:
        :param values:
        :return:
        """
        try:
            if values:
                service = self.google_auth()
                print(f'Updating {gsheet_range} Google Sheet')
                body = {
                    'values': values
                }
                result = service.spreadsheets().values().append(spreadsheetId=self.spreadsheet_id, range=gsheet_range,
                                                                insertDataOption="INSERT_ROWS",
                                                                valueInputOption="RAW", body=body).execute()
                updated_data = result.get('updates').get('updatedCells')
                print('{0} cells appended.'.format(updated_data))
                return updated_data
            else:
                print('Current entries are up to date')
                return None
        except Exception as e:
            print(f'Error while updating Gsheet {self.spreadsheet_id}. Error was: {e}')

    def gsheet_update(self, gsheet_range, values):
        """
        Update row range on Google Sheet
        :param values:
        :param gsheet_range:
        :return:
        """
        try:
            if values:
                service = self.google_auth()
                print(f'Updating {gsheet_range} Google Sheet')
                body = {
                    'values': values
                }
                result = service.spreadsheets().values().update(spreadsheetId=self.spreadsheet_id, range=gsheet_range,
                                                                valueInputOption="RAW", body=body).execute()
                updated_data = result.get('updatedCells')
                print(f'{updated_data} cells on Row {gsheet_range} were updated.')
                return updated_data
            else:
                print('There is nothing to be updated')
                return None
        except Exception as e:
            print(f'Error while updating Gsheet Row {gsheet_range}. Error was: {e}')

    def read_gsheet_data(self, sheet_range):
        """
        Append rows to Google Sheet
        :param sheet_range:
        :return: all rows if exists
        """
        try:
            print(f'Getting data from {sheet_range}')
            service = self.google_auth()
            sheet = service.spreadsheets()
            result = sheet.values().get(spreadsheetId=self.spreadsheet_id,
                                        range=sheet_range).execute()
            values = result.get('values', [])
            if not values:
                print('No data found.')
                return None
            else:
                return values
        except Exception as e:
            print(f'Error while reading Gsheet data from {self.spreadsheet_id}. Error was: {e}')

    def get_missing_rows(self, input_entries, past_entries_lookup):
        """
        Get missing rows in Gsheet, based on a list of rows input
        :param input_entries: potential new rows from previous fortnight
        :param past_entries_lookup: days to compare in the past
        :return: missing rows
        """
        print('Getting Missing Rows from Google Sheet')
        current_rows = self.read_gsheet_data(self.entries_sheet)
        last_period_initial_date = (datetime.today() - timedelta(days=past_entries_lookup)).strftime('%Y-%m-%d')
        last_period_rows_uid = [int(row[0]) for row in current_rows[1:] if row[1] >= last_period_initial_date]
        new_rows = [row for row in input_entries if row[0] not in last_period_rows_uid]
        return new_rows

    def get_weekly_entries(self):
        """
        Get weekly automated Harvest tasks
        :return:
        """
        print('Getting Automated time-entries from Google Sheet')
        weekly_entries = self.read_gsheet_data(self.weekly_tasks_sheet)
        entries = [{"user": row[0], "project": row[1], "code": row[2], "task": row[3], "date": row[4],
                    "hours": row[5]} for row in weekly_entries[1:]]
        return entries

    def get_eligible_roles(self):
        """
        Get roles and their target utilization
        :return: dict: {'role_a': 0.15}
        """
        print(f'Getting AirTable Roles from {self.roles_sheet} Google Sheet')
        eligible_roles_rows = self.read_gsheet_data(self.roles_sheet)
        eligible_roles = {}
        for row in eligible_roles_rows[1:]:
            if '%' in row[1]:
                eligible_roles.update({row[0]: float('0.' + row[1].strip('%'))})
            else:
                eligible_roles.update({row[0]: float(row[1])})
        return eligible_roles

    def log_update(self, payload, sheet_id, type="entries"):
        """
        Log updated rows on Gsheet
        :param payload:
        :param sheet_id:
        :param type: gsheet column length identifier
        :return: None
        """
        sheet_type = {
            "projects": 12,
            "entries": 15
        }
        length = sheet_type[type]
        log_date = date.today()
        rows = int(payload) // length
        update_msg = f'Logging info for {log_date}: {rows} rows were appended on {sheet_id}'
        self.gsheet_append(self.logs_sheet, [[update_msg]])

    def get_new_rows(self, input_entries, past_entries_lookup):
        """
        Get missing rows in Gsheet, based on a list of rows input
        :return: missing rows
        """
        print('Getting Missing Rows from Google Sheet')
        new_rows = []
        current_rows = self.read_gsheet_data(self.entries_sheet)
        last_period_initial_date = (datetime.today() - timedelta(days=past_entries_lookup)).strftime('%Y-%m-%d')
        last_period_rows = {}
        for index, row in enumerate(current_rows[1:]):
            if row[1] >= last_period_initial_date:
                last_period_rows.update({row[0]: {'values': row[1:11] + [float(row[11]), int(row[12])]
                                                  + list(map(float, row[13:])), 'index': index + 2}})
        last_period_rows_uid = [int(row) for row in last_period_rows.keys()]
        for row in input_entries:
            if row[0] not in last_period_rows_uid:
                new_rows.append(row)
            elif row[1:] != last_period_rows[str(row[0])]['values']:
                print(f'{last_period_rows[str(row[0])]["index"]}: {row}')
                # row_index = last_period_rows[str(row[0])]['index']
                # gsheet_range = f'{ENTRIES_SHEET}!A{row_index}:P{row_index}'
                # gsheet_update(CREDENTIALS_FILE, SPREADSHEET_ID, ENTRIES_SHEET, gsheet_range, [row])
            else:
                pass
        return new_rows
