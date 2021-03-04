import requests
from datetime import datetime, timedelta
import logging


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
                    billable = task["billable_by_default"]
                    tasks_hashmap.update({name: {"id": task_id, "billable": billable}})
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

    def delete_time_entry(self, entry_id):
        """
        Delete a time entry on Harvest
        :param entry_id:
        :return:
        """
        url_time_entry = self.harvest_api + 'time_entries/' + str(entry_id)
        headers = {
            "User-Agent": "Python Harvest API Sample",
            "Authorization": "Bearer {}".format(self.harvest_token),
            "Harvest-Account-ID": self.harvest_account
        }
        try:
            response = requests.delete(url_time_entry, verify=False, headers=headers).json()
            print(f'Entry deleted, response was: {response}')
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
            # print(user_id, project_id, task_id, spent_date, hours)
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
        # spent_date = (datetime.today() - timedelta(days=offset)).strftime('%Y-%m-%d')
        return spent_date

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
