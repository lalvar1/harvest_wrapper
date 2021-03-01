import httplib2
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import logging
import requests
import urllib3
from datetime import datetime, timedelta, date
import os
import unidecode


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
FLOAT_TOKEN = os.environ["FLOAT_TOKEN"]


class HarvestAnalytics:
    """
    A class to process and structure Harvest data
    """
    def __init__(self, entries_lookup, harvest_account, harvest_token, weekly_entries, eligible_roles):
        self.past_entries_lookup = entries_lookup
        self.harvest_api = 'https://api.harvestapp.com/v2/'
        self.harvest_account = harvest_account
        self.harvest_token = harvest_token
        self.weekly_entries = weekly_entries
        self.harvest_eligible_roles = eligible_roles
        self.harvest_tasks = self.get_tasks()
        self.harvest_projects = self.get_projects()
        self.harvest_users = self.get_users_data()

    def get_historical_data(self):
        """
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
                hourly_rate = user['default_hourly_rate']
                user_info = {'role': role, 'geography': timezone, 'id': user_id, 'default_hourly_rate': hourly_rate}
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
        # start_date = '2019-01-01'
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
                        "name": project["name"],
                        "code": project["code"],
                        "is_active": project["is_active"],
                        "is_billable": project["is_billable"],
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
            # entry_id = response["id"]
            print(f'Entry created, response was: {response}')
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


class FloatAnalytics:
    """
    A class to process and structure Float data
    """
    def __init__(self, float_token, users=None, projects=None, clients=None, tasks=None):
        self.float_token = float_token
        self.float_api = 'https://api.float.com/v3'
        self.float_clients = self.get_clients()
        self.float_projects = self.get_projects()
        self.float_users = self.get_people()
        self.harvest_users = {unidecode.unidecode(user): data for user, data in users.items()}
        self.harvest_projects = projects
        self.harvest_clients = clients
        self.harvest_tasks = tasks

    def get_clients(self):
        """
        Get Float clients
        :return: clients list of dicts
        """
        clients_url = f"{self.float_api}/clients"
        headers = {
            "User-Agent": "Python Float App",
            "Authorization": f"Bearer {self.float_token}"
        }
        try:
            print('Getting Float Clients')
            clients_hashmap = {}
            total_pages = requests.get(clients_url, verify=False, headers=headers).headers['X-Pagination-Page-Count']
            for page in range(1, int(total_pages) + 1):
                clients = requests.get(clients_url, verify=False, headers=headers, params={'page': page}).json()
                for client in clients:
                    name = client["name"]
                    client_id = client["client_id"]
                    clients_hashmap.update({name: {"id": client_id}})
            return clients_hashmap
        except Exception as e:
            print(f'Error while getting clients. Error was {e}')

    def get_projects(self):
        """
        Get Float projects
        :return: projects list of dicts, by id given there aren't unique names on some cases
        """
        projects_url = f"{self.float_api}/projects"
        headers = {
            "User-Agent": "Python Float App",
            "Authorization": f"Bearer {self.float_token}"
        }
        try:
            print('Getting Float Projects')
            projects_hashmap = {}
            total_pages = requests.get(projects_url, verify=False, headers=headers).headers['X-Pagination-Page-Count']
            for page in range(1, int(total_pages) + 1):
                projects = requests.get(projects_url, verify=False, headers=headers, params={'page': page}).json()
                for project in projects:
                    name = project["name"].upper()
                    project_id = project["project_id"]
                    budget = project["budget_total"]
                    client = project["client_id"]
                    code = project["tags"][0] if project["tags"] else ""
                    is_active = project["active"]
                    is_billable = project["non_billable"]
                    projects_hashmap.update({project_id: {"name": name, "budget": budget, "client": client,
                                            "code": code, "is_active": is_active, "is_billable": is_billable}})
            return projects_hashmap
        except Exception as e:
            print(f'Error while getting projects. Error was {e}')

    def get_people(self):
        """
        Get Float Users
        :return: users list of dicts
        """
        projects_url = f"{self.float_api}/people"
        headers = {
            "User-Agent": "Python Float App",
            "Authorization": f"Bearer {self.float_token}"
        }
        try:
            print('Getting Float Users')
            user_hashmap = {}
            total_pages = requests.get(projects_url, verify=False, headers=headers).headers['X-Pagination-Page-Count']
            for page in range(1, int(total_pages) + 1):
                users = requests.get(projects_url, verify=False, headers=headers, params={'page': page}).json()
                for user in users:
                    name = user["name"]
                    people_id = user["people_id"]
                    role = user["job_title"]
                    hourly_rate = user["default_hourly_rate"]
                    user_hashmap.update({name: {"id": people_id, "role": role, "default_hourly_rate": hourly_rate}})
            return user_hashmap
        except Exception as e:
            print(f'Error while getting users. Error was {e}')

    def create_tasks_from_ghseet(self, gsheet_data, scheduled=False):
        """
        Create Float task
        :return: none
        """
        task_type = 'tasks' if scheduled else 'logged-time'
        tasks_url = f"{self.float_api}/{task_type}"
        headers = {
            "User-Agent": "Python Float App",
            "Authorization": f"Bearer {self.float_token}"
        }
        try:
            print(f'Creating Float {task_type} Tasks')
            count = 0
            for row in gsheet_data:
                # if row[1][:4] == '2021':
                body = {
                    "project_id": self.get_project_id(row[6].upper(), row[7]),
                    "people_id": self.float_users[unidecode.unidecode(row[2])]['id'],
                    "hours": round(float(row[11]) * 4) / 4 if float(row[11]) >= 0.25 else 0.25,
                    "date": row[1],
                    "billable": 1 if row[9].strip().upper() == 'TRUE' else 0,
                    "task_name": row[8]
                }
                response = requests.post(tasks_url, verify=False, headers=headers, data=body)
                print(row[9], body["billable"], response.json()[0]["billable"])
                if 'X-RateLimit-Remaining-Minute' in response.headers:
                    rate_limit_remaining = response.headers['X-RateLimit-Remaining-Minute']
                    if int(rate_limit_remaining) <= 15:
                        print("Cooling down 1:30 minutes")
                        sleep(90)
                    else:
                        sleep(0.8)  # 800ms pause to avoid API Throttle
                count += 1
                if response.status_code == 200:
                    print(count, response.status_code, response.json())
                else:
                    print(count, response.status_code, row)

            print(f" {count} Tasks were successfully created")
        except Exception as e:
            print(f'Error while creating tasks. Error was {e}')

    def get_project_id(self, name, code):
        """
        Get Float project id, based on project name and code
        """
        for project_id, project_data in self.float_projects.items():
            if code and project_data["code"]:
                if project_data["name"].upper() == name.upper() and project_data["code"].upper() == code.upper():
                    return project_id
            elif project_data["name"].upper() == name.upper():
                return project_id

    def get_harvest_project_data(self, name, code):
        """
        Get project data for a given project name and code
        """
        for project_id, project_data in self.harvest_projects.items():
            data = {"client": project_data["client"],
                    "is_billable": project_data["is_billable"],
                    "is_active": project_data["is_active"],
                    "id": project_id,
                    "name": project_data["name"]}
            if project_data["name"].upper() == name.upper() and project_data["code"].upper() == code.upper():
                return data
        return

    def sync_projects(self):
        """
        Sync Float projects from Harvest's projects
        :return: none
        """
        try:
            print('Syncing Float Projects')
            for id, project_data in self.float_projects.items():
                harvest_data = self.get_harvest_project_data(project_data["name"], project_data["code"])
                if harvest_data:
                    is_billable = 0 if harvest_data["is_billable"] else 1
                    is_active = 1 if harvest_data["is_active"] else 0
                    client = harvest_data["client"]
                    body = {}
                    if project_data["client"] != self.float_clients[client]["id"]:
                        body["client_id"] = self.float_clients[client]["id"]
                    if project_data["is_billable"] != is_billable:
                        body["non_billable"] = is_billable
                    if project_data["is_active"] != is_active:
                        body["active"] = is_active
                    if body:
                        sleep(0.4)
                        self.update_data('projects', id, body)
                else:
                    print('Float project data not found in Harvest', id, project_data)
            print('Projects finished syncing')
        except Exception as e:
            print(f'Error while syncing projects. Error was {e}')

    def sync_people(self):
        """
        Sync Float Users from Harvest
        :return: none
        """
        try:
            print('Syncing Float Users')
            for user, user_data in self.float_users.items():
                if user not in self.harvest_users.keys():
                    continue
                float_rate = float(user_data["default_hourly_rate"]) if user_data["default_hourly_rate"] else float(0)
                float_role = user_data["role"] if user_data["role"] else ""
                if self.harvest_users[user]["default_hourly_rate"]:
                    harvest_rate = self.harvest_users[user]["default_hourly_rate"]
                else:
                    harvest_rate = float(0)
                harvest_role = self.harvest_users[user]["role"]
                body = {}
                if float_rate != harvest_rate:
                    body["default_hourly_rate"] = harvest_rate
                if float_role != harvest_role:
                    body["job_title"] = harvest_role
                if body:
                    self.update_data('people', user_data["id"], body)
        except Exception as e:
            print(f'Error while syncing Users. Error was {e}')

    def update_data(self, endpoint, id, body):
        """
        Update via PATCH method a Float field on specified endpoint
        """
        url = f"{self.float_api}/{endpoint}/{id}"
        headers = {
            "User-Agent": "Python Float App",
            "Authorization": f"Bearer {self.float_token}"
        }
        try:
            response = requests.patch(url, verify=False, headers=headers, data=body).json()
            print(f'Updated {endpoint}/{id}. Input: {body}. Response: {response}')
        except Exception as e:
            print(f'Error while updating {endpoint}/{id}: {e}')


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


def runner(event, context):
    logging.info(f'Starting Cloud function Runner. {event}: {context}')
    google_runner = GoogleRunner(SPREADSHEET_ID, CREDENTIALS_FILE, ENTRIES_SHEET, LOGS_SHEET,
                                 ROLES_SHEET, WEEKLY_TASKS_SHEET, PROJECTS_SHEET)
    weekly_entries = google_runner.get_weekly_entries()
    eligible_roles = google_runner.get_eligible_roles()
    harvest_runner = HarvestAnalytics(PAST_ENTRIES_LOOKUP, HARVEST_ACCOUNT_ID, HARVEST_TOKEN,
                                      weekly_entries, eligible_roles)
    harvest_entries = harvest_runner.get_historical_data()
    new_rows = google_runner.get_missing_rows(harvest_entries, PAST_ENTRIES_LOOKUP)
    updated_cells = google_runner.gsheet_append(ENTRIES_SHEET, new_rows)
    google_runner.log_update(updated_cells, ENTRIES_SHEET)
    projects_status = harvest_runner.get_project_rows()
    projects_range = f'{PROJECTS_SHEET}!A2:M'
    updated_cells = google_runner.gsheet_update(projects_range, projects_status)
    google_runner.log_update(updated_cells, PROJECTS_SHEET, "projects")
    harvest_users = harvest_runner.harvest_users
    harvest_projects = harvest_runner.harvest_projects
    float_runner = FloatAnalytics(FLOAT_TOKEN, users=harvest_users, projects=harvest_projects)
    float_runner.sync_people()
    float_runner.sync_projects()
    # float_runner.create_tasks_from_ghseet(new_rows)


def wrapper(event, context):
    try:
        runner(event, context)
    except Exception as e:
        print(e)
        logging.fatal(e)
