import httplib2
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import logging
import requests
import urllib3
from datetime import datetime, timedelta, date
import os
from dotenv import load_dotenv
import unidecode
from time import sleep

load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
CREDENTIALS_FILE = os.environ["CREDENTIALS_FILE"]
ENTRIES_SHEET = os.environ["ENTRIES_SHEET"]
TEST_SHEET = os.environ["TEST_SHEET"]
LOGS_SHEET = os.environ["LOGS_SHEET"]
ROLES_SHEET = os.environ["ROLES_SHEET"]
WEEKLY_TASKS_SHEET = os.environ["WEEKLY_TASKS_SHEET"]
PROJECTS_SHEET = os.environ["PROJECTS_SHEET"]
HARVEST_TOKEN = os.environ["HARVEST_TOKEN"]
HARVEST_ACCOUNT_ID = os.environ["HARVEST_ACCOUNT_ID"]
PAST_ENTRIES_LOOKUP = int(os.environ["PAST_ENTRIES_LOOKUP"])
FORECAST_SHEET = os.environ["FORECAST_SHEET"]
FORECAST_TOKEN = os.environ["FORECAST_TOKEN"]
FORECAST_ACCOUNT_ID = os.environ["FORECAST_ACCOUNT_ID"]
FLOAT_TOKEN = os.environ["FLOAT_TOKEN"]


class HarvestAnalytics:
    """
    A class to process and structure Harvest data
    """

    def __init__(self, entries_lookup, harvest_account, harvest_token):
        self.past_entries_lookup = entries_lookup
        self.harvest_api = 'https://api.harvestapp.com/v2/'
        self.harvest_account = harvest_account
        self.harvest_token = harvest_token
        # self.harvest_tasks = self.get_tasks()
        self.harvest_projects = self.get_projects()
        self.harvest_clients = self.get_clients()
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
                full_name = unidecode.unidecode(user['first_name'] + " " + user['last_name'])
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

    def get_clients(self):
        """
        Get Harvest clients
        :return: tasks list of dicts
        """
        url_tasks = self.harvest_api + 'clients'
        headers = {
            "User-Agent": "Python Harvest API Sample",
            "Authorization": "Bearer {}".format(self.harvest_token),
            "Harvest-Account-ID": self.harvest_account
        }
        try:
            print('Getting Harvest Clients')
            clients_hashmap = {}
            total_pages = requests.get(url_tasks, verify=False, headers=headers).json()['total_pages']
            for page in range(1, total_pages + 1):
                clients = requests.get(url_tasks, verify=False, headers=headers, params={'page': page}).json()
                for client in clients["clients"]:
                    name = client["name"]
                    client_id = client["id"]
                    is_active = client["is_active"]
                    clients_hashmap.update({name: {"id": client_id, "is_active": is_active}})
            return clients_hashmap
        except Exception as e:
            print(f'Error while getting clients. Error was {e}')


class FloatAnalytics:
    """
    A class to process and structure Float data
    """

    def __init__(self, float_token, users=None, projects=None, clients=None, tasks=None):
        self.float_token = float_token
        self.float_api = 'https://api.float.com/v3'
        self.float_clients = self.get_clients()
        self.float_projects = self.get_projects()
        # self.float_users = self.get_people()
        # self.float_tasks = self.get_tasks()
        self.harvest_users = users
        self.harvest_projects = projects
        self.harvest_clients = clients
        self.harvest_tasks = tasks

    def get_tasks(self):
        """
        Get Float logged tasks
        :return: tasks list of dicts
        """
        clients_url = f"{self.float_api}/logged-time"
        headers = {
            "User-Agent": "Python Float App",
            "Authorization": f"Bearer {self.float_token}"
        }
        try:
            print('Getting Float Tasks')
            tasks_hashmap = {}
            total_pages = requests.get(clients_url, verify=False, headers=headers).headers['X-Pagination-Page-Count']
            for page in range(1, int(total_pages) + 1):
            # for page in range(1, 30):
                print(f'Getting Float entries from page #{page}')
                tasks = requests.get(clients_url, verify=False, headers=headers, params={'page': page}).json()
                for task in tasks:
                    name = task["task_name"]
                    project_id = task["project_id"]
                    people_id = task["people_id"]
                    hours = task["hours"]
                    date = task["date"]
                    billable = task["billable"]
                    logged_time_id = task["logged_time_id"]
                    tasks_hashmap.update({logged_time_id: {"name": name, "user": people_id, "project": project_id,
                                                           "hours": hours, "date": date, "billable": billable,
                                                           "project_name": self.float_projects[project_id]["name"]}})
            # ord = sorted(tasks_hashmap.items(), key=lambda x: x[1]['project_name'])
            # print(ord)
            return tasks_hashmap
        except Exception as e:
            print(f'Error while getting tasks. Error was {e}')

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
            # ord = sorted(projects_hashmap.items(), key=lambda x: x[1]['name'])
            # print(ord)
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

    def create_clients(self):
        """
        Create Float clients
        :return: none
        """
        clients_url = f"{self.float_api}/clients"
        headers = {
            "User-Agent": "Python Float App",
            "Authorization": f"Bearer {self.float_token}"
        }
        try:
            print('Creating Float Clients')
            for name, data in self.harvest_clients.items():
                body = {
                    "name": name
                }
                response = requests.post(clients_url, verify=False, headers=headers, data=body).json()
            print(f"{len(self.harvest_clients)} were created")
        except Exception as e:
            print(f'Error while creating clients. Error was {e}')

    def create_projects(self):
        """
        Create Float projects
        :return: none
        """
        clients_url = f"{self.float_api}/projects"
        headers = {
            "User-Agent": "Python Float App",
            "Authorization": f"Bearer {self.float_token}"
        }
        try:
            print('Creating Float Projects')
            for project_id, data in self.harvest_projects.items():
                is_active = 1 if data["is_active"] else 0
                is_billable = 0 if data["is_billable"] else 1
                body = {
                    "name": data["name"],
                    "client_id": self.float_clients[data["client"]]["id"],
                    "budget_type": 2,  # 2 Total Fee
                    "budget_total": data["budget"],
                    "non_billable": is_billable,  # 0 billable, 1 non-billable
                    "active": is_active  # 1 active, 0 inactive
                }
                response = requests.post(clients_url, verify=False, headers=headers, data=body)
                print(response.status_code, response.json())
            print(f"{len(self.harvest_projects)} were created")
        except Exception as e:
            print(f'Error while creating projects. Error was {e}')

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
            for row in gsheet_data[1:]:
                if row[1][:4] == '2021':
                    body = {
                        "project_id": self.get_project_id(row[6].upper(), row[7]),
                        "people_id": self.float_users[unidecode.unidecode(row[2])]['id'],
                        "hours": round(float(row[11]) * 4) / 4 if float(row[11]) >= 0.25 else 0.25,
                    }
                    if scheduled:
                        body.update({
                            "start_date": row[1],
                            "end_date": row[1],
                            "repeat_state": 0,  # no repeat
                            "name": row[8]
                        })
                    else:
                        body.update({
                            "date": row[1],
                            "billable": 1 if row[9].upper() == 'TRUE' else 0,
                            "task_name": row[8]
                        })
                    response = requests.post(tasks_url, verify=False, headers=headers, data=body)
                    if 'X-RateLimit-Remaining-Minute' in response.headers:
                        rate_limit_remaining = response.headers['X-RateLimit-Remaining-Minute']
                        if int(rate_limit_remaining) <= 15:
                            print("Cooling down 1:30 minute")
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

    def create_people(self):
        name = ""
        email = ""
        job_title = ""
        tags = "location"
        pass

    def create_tasks(self):
        pass

    def create_reports(self):
        pass

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

    #  SYNC FUNCTIONS
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
                    # harvest_id = harvest_data["id"]
                    if project_data["client"] != self.float_clients[client]["id"]:
                        body["client_id"] = self.float_clients[client]["id"]
                        # print('client_id', id, harvest_id, project_data["name"], project_data["client"],
                        #      self.float_clients[client]["id"])
                    if project_data["is_billable"] != is_billable:
                        body["non_billable"] = is_billable
                        # print('billable', id, harvest_id, project_data["name"], project_data["is_billable"],
                        # is_billable)
                    if project_data["is_active"] != is_active:
                        body["active"] = is_active
                        # print('active', id, harvest_id, project_data["name"], project_data["is_active"], is_active)
                    if body:
                        self.update_data('projects', id, body)
                    else:
                        continue
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
                rate = float(user_data["default_hourly_rate"].strip('0'))
                if rate != self.harvest_users[user]["default_hourly_rate"]:
                    body = {
                      "default_hourly_rate": rate
                    }
                    # print(user, rate, self.harvest_users[user]["default_hourly_rate"])
                    self.update_data('people', user_data["id"], body)
        except Exception as e:
            print(f'Error while syncing Users. Error was {e}')

    def update_data(self, endpoint, id, body):
        f"""
        Update via PUT method a Float field on specified endpoint
        """
        url = f"{self.float_api}/{endpoint}/{id}"
        headers = {
            "User-Agent": "Python Float App",
            "Authorization": f"Bearer {self.float_token}"
        }
        try:
            response = requests.put(url, verify=False, headers=headers, data=body).json()
            print(f'Updated {endpoint}/{id}. Input: {body}. Response: {response}')
        except Exception as e:
            print(f'Error while updating {endpoint}/{id}: {e}')


def read_gsheet_data(credentials_file, spreadsheet_id, sheet_range):
    """
    Append rows to Google Sheet
    :param spreadsheet_id:
    :param sheet_range:
    :param credentials_file:
    :return: all rows if exists
    """
    try:
        print('Reading Data from Gsheets')
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


def main(event, context):
    logging.info(f'Payload input data is {event} and context {context}')
    # harvest_runner = HarvestAnalytics(PAST_ENTRIES_LOOKUP, HARVEST_ACCOUNT_ID, HARVEST_TOKEN)
    # harvest_projects = harvest_runner.harvest_projects
    # harvest_users = harvest_runner.harvest_users
    # harvest_clients = harvest_runner.harvest_clients
    # time_entries = read_gsheet_data(CREDENTIALS_FILE, SPREADSHEET_ID, ENTRIES_SHEET)
    # float_runner = FloatAnalytics(FLOAT_TOKEN, projects=harvest_projects,
    # users=harvest_users, clients=harvest_clients)
    float_runner = FloatAnalytics(FLOAT_TOKEN)
    # float_runner.sync_projects()  # sync projects, billable, wrong or missing client
    # float_runner.sync_people()  # sync users, cost_rate
    float_runner.create_tasks_from_ghseet(time_entries)  # create logged-timed entries


if __name__ == "__main__":
    main('data', 'context')
