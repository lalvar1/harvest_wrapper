from time import sleep
import requests
import unidecode


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
        # self.float_tasks = self.get_tasks()
        self.harvest_users = {unidecode.unidecode(user): data for user, data in users.items()}
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
                    active = True if user["active"] == 1 else False
                    user_hashmap.update({name: {"id": people_id, "role": role, "default_hourly_rate": hourly_rate,
                                                "active": active}})
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
                    sleep(0.65)  # 650ms pause to avoid API Throttle
                count += 1
                if response.status_code == 200:
                    print(count, response.status_code, response.json())
                else:
                    print(count, response.status_code, row)

            print(f" {count} Tasks were successfully created")
        except Exception as e:
            print(f'Error while creating tasks. Error was {e}')

    def create_people(self):
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
                    if project_data["client"] != self.float_clients[client]["id"]:
                        body["client_id"] = self.float_clients[client]["id"]
                    if project_data["is_billable"] != is_billable:
                        body["non_billable"] = is_billable
                    if project_data["is_active"] != is_active:
                        body["active"] = is_active
                    if body:
                        # print('projects', id, body)
                        sleep(0.4)
                        self.update_data('projects', id, body)
                else:
                    print('Float project data not found in Harvest', id, project_data)
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
                float_status = user_data["active"]
                if self.harvest_users[user]["default_hourly_rate"]:
                    harvest_rate = self.harvest_users[user]["default_hourly_rate"]
                else:
                    harvest_rate = float(0)
                harvest_role = self.harvest_users[user]["role"]
                harvest_status = self.harvest_users[user]["active"]
                body = {}
                if float_rate != harvest_rate:
                    body["default_hourly_rate"] = harvest_rate
                if float_role != harvest_role:
                    body["job_title"] = harvest_role
                if float_status != harvest_status:
                    body["active"] = harvest_status
                if body:
                    # print(user, rate, self.harvest_users[user])
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
