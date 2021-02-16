import httplib2
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import logging
import requests
import urllib3
from datetime import datetime, timedelta, date
import os


logging.info('Loading ENV vars')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
CREDENTIALS_FILE = os.environ["CREDENTIALS_FILE"]
ENTRIES_SHEET = os.environ["ENTRIES_SHEET"]
LOGS_SHEET = os.environ["LOGS_SHEET"]
ROLES_SHEET = os.environ["ROLES_SHEET"]
WEEKLY_TASKS_SHEET = os.environ["WEEKLY_TASKS_SHEET"]
PROJECTS_SHEET = os.environ["PROJECTS_SHEET"]
HARVEST_TOKEN = os.environ["HARVEST_TOKEN"]
HARVEST_ACCOUNT_ID = os.environ["HARVEST_ACCOUNT_ID"]
PAST_ENTRIES_LOOKUP = int(os.environ["PAST_ENTRIES_LOOKUP"])


class HarvestAnalytics:
    """
    A class to process and structure Harvest data
    """
    def __init__(self, entries_lookup, harvest_account, harvest_token):
        self.past_entries_lookup = entries_lookup
        self.harvest_api = 'https://api.harvestapp.com/v2/'
        self.harvest_account = harvest_account
        self.harvest_token = harvest_token
        self.weekly_entries = get_weekly_entries()
        self.harvest_eligible_roles = get_eligible_roles()
        self.harvest_tasks = self.get_tasks()
        self.harvest_projects = self.get_projects()
        self.harvest_users = self.get_users_data()

    def get_historical_data(self):
        """
        Get Harvest formatted time-entries
        :return:
        """
        print('Getting Historical time entries from Harvest')
        url_time_entries = self.harvest_api + 'time_entries'
        headers = {
            "User-Agent": "Python Harvest API Sample",
            "Authorization": "Bearer {}".format(self.harvest_token),
            "Harvest-Account-ID": self.harvest_account
        }
        try:
            total_pages = requests.get(url_time_entries, verify=False, headers=headers).json()['total_pages']
            users_full_data = self.get_row_list(url_time_entries, headers, total_pages)
            print('Harvest Data was retrieved successfully')
            return users_full_data
        except Exception as e:
            logging.fatal(f'Failed while getting data from Harvest. Error was: {e}')
            print(f'Error while getting data from Harvest. Error was: {e}')

    def get_users_data(self):
        """
        Get Harvest user roles
        :return: users data dict of dict
        """
        url_users = self.harvest_api + 'users'
        headers = {
            "User-Agent": "Python Harvest API Sample",
            "Authorization": "Bearer {}".format(self.harvest_token),
            "Harvest-Account-ID": self.harvest_account
        }
        try:
            users_data = {}
            print('Getting Harvest Users data')
            harvest_users_data = requests.get(url_users, verify=False, headers=headers).json()
            for user in harvest_users_data['users']:
                full_name = user['first_name'] + " " + user['last_name']
                role = user['roles'][0] if user['roles'] else None
                timezone = user['timezone']
                if 'US' in timezone.upper():
                    timezone = 'USA'
                elif 'MADRID' in timezone.upper():
                    timezone = 'SPAIN'
                else:
                    timezone = None
                user_id = user['id']
                user_info = {'role': role, 'geography': timezone, 'id': user_id}
                users_data.update({full_name: user_info})
            return users_data
        except Exception as e:
            print(f'Error while getting users roles. Error was {e}')

    def get_row_list(self, url, headers, total_pages):
        """
        Get rows list of lists, each list representing a new row
        :param url:
        :param headers:
        :param total_pages:
        :return:
        """
        available_roles = self.harvest_eligible_roles.keys()
        users_entries = []
        start_date = (datetime.today() - timedelta(days=self.past_entries_lookup)).strftime('%Y-%m-%d')
        execution_date = datetime.today().strftime('%Y-%m-%d')
        try:
            for page in range(1, total_pages + 1):
                print(f'Getting Harvest entries from page #{page}')
                page_entries = requests.get(url, verify=False, params={'page': page}, headers=headers).json()
                for entry in page_entries['time_entries']:
                    entry_date = entry['spent_date']
                    full_name = entry['user']['name']
                    role = self.harvest_users[full_name]['role']
                    if role in available_roles and start_date <= entry_date <= execution_date:
                        entry_id = entry['id']
                        date = entry_date
                        staff_member = full_name
                        geography = self.harvest_users[full_name]['geography']
                        client = entry['client']['name']
                        project = entry['project']['name']
                        project_code = entry['project']['code']
                        task = entry['task']['name']
                        billable = str(entry['billable']).upper()
                        locked = str(entry['is_locked']).upper()
                        hours = entry['hours']
                        target_utilization = self.harvest_eligible_roles[role]
                        cost_rate = entry['cost_rate']
                        hourly_rate = entry['user_assignment']['hourly_rate']
                        row = [entry_id, date, staff_member, role, geography, client, project, project_code, task,
                               billable, locked, hours, target_utilization, cost_rate, hourly_rate]
                        users_entries.append(row)
                    elif entry_date >= start_date:
                        pass
                    else:
                        return users_entries
            return users_entries
        except Exception as e:
            print(f'Error while getting page Data. Error was {e}')

    def get_projects(self):
        """
        Get Harvest projects
        :return: projects list of dicts
        """
        url_projects = self.harvest_api + 'projects'
        headers = {
            "User-Agent": "Python Harvest API Sample",
            "Authorization": "Bearer {}".format(self.harvest_token),
            "Harvest-Account-ID": self.harvest_account
        }
        try:
            print('Getting Harvest Projects')
            budgets = self.get_budgets()
            projects_hashmap = {}
            none_budget = {
                "budget": 0,
                "spent": 0,
                "remaining": 0
            }
            total_pages = requests.get(url_projects, verify=False, headers=headers, ).json()['total_pages']
            for page in range(1, total_pages + 1):
                projects = requests.get(url_projects, verify=False, headers=headers, params={'page': page}).json()
                for project in projects["projects"]:
                    project_id = project["id"]
                    project_data = {
                        "name": project["name"].upper(),
                        "code": project["code"],
                        "is_active": project["is_active"],
                        "client": project["client"]["name"],
                        "notes": project["notes"],
                        "start_date": project["starts_on"],
                        "end_date": project["ends_on"],
                        "creation_date": project["created_at"][:10],
                        "update_date": project["updated_at"][:10]
                    }
                    projects_hashmap[project_id] = project_data
                    if project_id in budgets:
                        projects_hashmap[project_id].update(budgets[project_id])
                    else:
                        projects_hashmap[project_id].update(none_budget)
            return projects_hashmap
        except Exception as e:
            print(f'Error while getting projects. Error was {e}')

    def get_project_rows(self):
        rows = [[project_id] + list(project_values.values()) for project_id, project_values
                in self.harvest_projects.items()]
        return rows

    def get_budgets(self):
        """
        Get Harvest Project Budgets
        :return: tasks list of dicts
        """
        url_budget = self.harvest_api + 'reports/project_budget'
        headers = {
            "User-Agent": "Python Harvest API Sample",
            "Authorization": "Bearer {}".format(self.harvest_token),
            "Harvest-Account-ID": self.harvest_account
        }
        try:
            print('Getting Harvest Budgets')
            budget_hashmap = {}
            total_pages = requests.get(url_budget, verify=False, headers=headers).json()['total_pages']
            for page in range(1, total_pages + 1):
                projects = requests.get(url_budget, verify=False, headers=headers, params={'page': page}).json()
                for project in projects["results"]:
                    name = project["project_name"].upper()
                    project_id = project["project_id"]
                    budget = project["budget"]
                    spent = project["budget_spent"]
                    remaining = project["budget_remaining"]
                    budget_hashmap.update({project_id: {"name": name, "budget": budget, "spent": spent,
                                                        "remaining": remaining}})
            return budget_hashmap
        except Exception as e:
            print(f'Error while getting tasks. Error was {e}')

    def get_tasks(self):
        """
        Get Harvest tasks
        :return: tasks list of dicts
        """
        url_tasks = self.harvest_api + 'tasks'
        headers = {
            "User-Agent": "Python Harvest API Sample",
            "Authorization": "Bearer {}".format(self.harvest_token),
            "Harvest-Account-ID": self.harvest_account
        }
        try:
            print('Getting Harvest Tasks')
            tasks_hashmap = {}
            total_pages = requests.get(url_tasks, verify=False, headers=headers).json()['total_pages']
            for page in range(1, total_pages + 1):
                tasks = requests.get(url_tasks, verify=False, headers=headers, params={'page': page}).json()
                for task in tasks["tasks"]:
                    name = task["name"].upper()
                    task_id = task["id"]
                    tasks_hashmap.update({name: task_id})
            return tasks_hashmap
        except Exception as e:
            print(f'Error while getting tasks. Error was {e}')

    def create_time_entry(self, user_id, project_id, task_id, spent_date, hours):
        """
        Create a time entry on Harvest
        :param user_id:
        :param project_id:
        :param task_id:
        :param spent_date:
        :param hours:
        :return: entry_id
        """
        url_time_entries = self.harvest_api + 'time_entries'
        headers = {
            "User-Agent": "Python Harvest API Sample",
            "Authorization": "Bearer {}".format(self.harvest_token),
            "Harvest-Account-ID": self.harvest_account
        }
        body = {
            "user_id": user_id,
            "project_id": project_id,
            "task_id": task_id,
            "spent_date": spent_date,
            "hours": hours
        }
        try:
            response = requests.post(url_time_entries, verify=False, data=body, headers=headers).json()
            print(f'Harvest time-entry created, response was: {response}')
            return response
        except Exception as e:
            print(f'Error while creating time-entry. Error was {e}')

    def get_project_id(self, name, code):
        """
        Get project id for a given project name and code
        """
        for project_id, project_data in self.harvest_projects.items():
            if project_data["name"].upper() == name.upper() and project_data["code"].upper() == code.upper():
                return project_id
        return

    def create_weekly_entries(self):
        print('Creating weekly Harvest time-entries')
        for entry in self.weekly_entries:
            user_id = self.harvest_users[entry["user"]]["id"]
            project_id = self.get_project_id(entry["project"], entry["code"])
            task_id = self.harvest_tasks[entry["task"].upper()]
            hours = entry["hours"]
            spent_date = self.to_spent_date(entry["date"].upper())
            self.create_time_entry(user_id, project_id, task_id, spent_date, hours)

    @staticmethod
    def to_spent_date(week_day):
        iso_week_days = {
            "MONDAY": 1,
            "TUESDAY": 2,
            "WEDNESDAY": 3,
            "THURSDAY": 4,
            "FRIDAY": 5,
            "SATURDAY": 6,
            "SUNDAY": 7
        }
        offset = datetime.today().isoweekday() - iso_week_days[week_day]
        spent_date = (datetime.today() - timedelta(days=offset) + timedelta(days=7)).strftime('%Y-%m-%d')
        return spent_date


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
    :param gsheet_range:
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


def runner(event, context):
    logging.info(f'Starting Cloud function Runner. {event}: {context}')
    harvest_runner = HarvestAnalytics(PAST_ENTRIES_LOOKUP, HARVEST_ACCOUNT_ID, HARVEST_TOKEN)
    harvest_runner.create_weekly_entries()
    harvest_entries = harvest_runner.get_historical_data()
    new_rows = get_missing_rows(harvest_entries, PAST_ENTRIES_LOOKUP)
    updated_cells = gsheet_append(CREDENTIALS_FILE, SPREADSHEET_ID, ENTRIES_SHEET, new_rows)
    log_update(updated_cells, ENTRIES_SHEET, "entries")
    projects_status = harvest_runner.get_project_rows()
    projects_range = f'{PROJECTS_SHEET}!A2:M'
    updated_cells = gsheet_update(CREDENTIALS_FILE, SPREADSHEET_ID, projects_range, projects_status)
    log_update(updated_cells, PROJECTS_SHEET, "projects")


def wrapper(event, context):
    try:
        runner(event, context)
    except Exception as e:
        print(e)
        logging.fatal(e)
