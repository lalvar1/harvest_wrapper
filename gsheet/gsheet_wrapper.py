import httplib2
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials


def google_auth(credential_file):
    """
    oauth2 authentication agains Google Gsheet API
    :return:
    """
    scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    try:
        credentials = ServiceAccountCredentials.from_json_keyfile_name(credential_file, scope)
        service = build('sheets', 'v4', http=credentials.authorize(httplib2.Http()), cache_discovery=False)
        return service
    except Exception as e:
        print(f"Error connecting: {e}. Retrying connection...")
        credentials = ServiceAccountCredentials.from_json_keyfile_name(credential_file, scope)
        service = build('sheets', 'v4', http=credentials.authorize(httplib2.Http()), cache_discovery=False)
        return service


def gsheet_append(credentials_file, spreadsheet_id, gsheet_range, values):
    """
    Append rows to Google Sheet
    :param spreadsheet_id:
    :param ghseet_range:
    :param credentials_file:
    :param values:
    :return:
    """
    try:
        if values:
            service = google_auth(credentials_file)
            print(f'Updating {gsheet_range} Google Sheet')
            body = {
                'values': values
            }
            result = service.spreadsheets().values().append(spreadsheetId=spreadsheet_id, range=gsheet_range,
                                                            insertDataOption="INSERT_ROWS",
                                                            valueInputOption="RAW", body=body).execute()
            updated_data = result.get('updates').get('updatedCells')
            print('{0} cells appended.'.format(updated_data))
            return updated_data
        else:
            print('Current entries are up to date')
            return None
    except Exception as e:
        print(f'Error while updating Gsheet {spreadsheet_id}. Error was: {e}')


def gsheet_update(credentials_file, spreadsheet_id, gsheet_range, values):
    """
    Update row range on Google Sheet
    :param spreadsheet_id:
    :param values:
    :param credentials_file:
    :param gsheet_range:
    :return:
    """
    try:
        if values:
            service = google_auth(credentials_file)
            print(f'Updating {gsheet_range} Google Sheet')
            body = {
                'values': values
            }
            result = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=gsheet_range,
                                                            valueInputOption="RAW", body=body).execute()
            updated_data = result.get('updatedCells')
            print(f'{updated_data} cells on Row {gsheet_range} were updated.')
            return updated_data
        else:
            print('There is nothing to be updated')
            return None
    except Exception as e:
        print(f'Error while updating Gsheet Row {gsheet_range}. Error was: {e}')


def read_gsheet_data(credentials_file, spreadsheet_id, sheet_range):
    """
    Append rows to Google Sheet
    :param spreadsheet_id:
    :param sheet_range:
    :param credentials_file:
    :return: all rows if exists
    """
    try:
        service = google_auth(credentials_file)
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=spreadsheet_id,
                                    range=sheet_range).execute()
        values = result.get('values', [])
        if not values:
            print('No data found.')
            return None
        else:
            return values
    except Exception as e:
        print(f'Error while reading Gsheet data from {spreadsheet_id}. Error was: {e}')


def get_missing_rows(input_entries, past_entries_lookup):
    """
    Get missing rows in Gsheet, based on a list of rows input
    :param input_entries: potential new rows from previous fortnight
    :param past_entries_lookup: days to compare in the past
    :return: missing rows
    """
    print('Getting Missing Rows from Google Sheet')
    current_rows = read_gsheet_data(CREDENTIALS_FILE, SPREADSHEET_ID, ENTRIES_SHEET)
    last_period_initial_date = (datetime.today() - timedelta(days=past_entries_lookup)).strftime('%Y-%m-%d')
    last_period_rows_uid = [int(row[0]) for row in current_rows[1:] if row[1] >= last_period_initial_date]
    new_rows = [row for row in input_entries if row[0] not in last_period_rows_uid]
    return new_rows


def get_new_rows(input_entries, past_entries_lookup):
    """
    Get missing rows in Gsheet, based on a list of rows input
    :return: missing rows
    """
    print('Getting Missing Rows from Google Sheet')
    new_rows = []
    current_rows = read_gsheet_data(CREDENTIALS_FILE, SPREADSHEET_ID, ENTRIES_SHEET)
    last_period_initial_date = (datetime.today() - timedelta(days=past_entries_lookup)).strftime('%Y-%m-%d')
    last_period_rows = {}
    for index, row in enumerate(current_rows[1:]):
        if row[1] >= last_period_initial_date:
            last_period_rows.update({row[0]: {'values': row[1:11] + [float(row[11]), int(row[12])] +
                                              list(map(float, row[13:])), 'index': index + 2}})
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


def get_weekly_entries():
    """
    Get weekly automated Harvest tasks
    :return:
    """
    print(f'Getting Automated time-entries from {WEEKLY_TASKS_SHEET} Google Sheet')
    weekly_entries = read_gsheet_data(CREDENTIALS_FILE, SPREADSHEET_ID, WEEKLY_TASKS_SHEET)
    entries = [{"user": row[0], "project": row[1], "code": row[2], "task": row[3], "date": row[4],
                "hours": row[5]} for row in weekly_entries[1:]]
    return entries


def get_eligible_roles():
    """
    Get roles and their target utilization
    :return: dict: {'role_a': 0.15}
    """
    print(f'Getting AirTable Roles from {ROLES_SHEET} Google Sheet')
    eligible_roles_rows = read_gsheet_data(CREDENTIALS_FILE, SPREADSHEET_ID, ROLES_SHEET)
    eligible_roles = {}
    for row in eligible_roles_rows[1:]:
        if '%' in row[1]:
            eligible_roles.update({row[0]: float('0.' + row[1].strip('%'))})
        else:
            eligible_roles.update({row[0]: float(row[1])})
    return eligible_roles


def log_update(payload, sheet_id, type="entries"):
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
    gsheet_append(CREDENTIALS_FILE, SPREADSHEET_ID, LOGS_SHEET, [[update_msg]])
