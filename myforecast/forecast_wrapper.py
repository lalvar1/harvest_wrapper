import forecast


class ForecastAnalytics:
    """
    A class to process and structure Harvest Forecast data
    """

    def __init__(self, forecast_account, forecast_token):
        self.forecast_account = forecast_account
        self.forecast_token = forecast_token

    def get_forecast_assignments(self):
        """
        Get Forecast user assignments
        :return: assignments as rows to be inserted in gsheets
        """
        user_assignments = []
        projects = self.get_forecast_projects()
        users = self.get_forecast_users()
        try:
            print('Getting Forecast Assignments')
            api = forecast.Api(account_id=self.forecast_account, auth_token=self.forecast_token)
            for assignment in api.get_assignments():
                assignment_id = assignment.id
                date = assignment.start_date
                hours = assignment.allocation / 3600
                project_data = projects[assignment.project_id]
                project = project_data["name"]
                project_code = project_data["code"]
                client = project_data["client"]
                staff_member = users[assignment.person_id]["name"]
                role = users[assignment.person_id]["role"]
                row = [assignment_id, date, staff_member, role, client, project, project_code, hours]
                user_assignments.append(row)
            return user_assignments
        except Exception as e:
            print(f'Error while getting Forecast data. Error was {e}')

    def get_forecast_projects(self):
        """
        Get Forecast projects
        :return: projects dicts with its info: name, client, code
        """
        projects = {}
        try:
            clients = self.get_forecast_clients()
            print('Getting Forecast Projects')
            api = forecast.Api(account_id=self.forecast_account, auth_token=self.forecast_token)
            for project in api.get_projects():
                project_id = project.id
                name = project.name
                code = project.code
                client = clients[project.client_id] if project.client_id else None
                projects.update({project_id: {"name": name, "code": code, "client": client}})
            return projects
        except Exception as e:
            print(f'Error while getting Forecast projects. Error was {e}')

    def get_forecast_clients(self):
        """
        Get Forecast clients
        :return: clients dict with its name
        """
        clients = {}
        try:
            print('Getting Forecast Clients')
            api = forecast.Api(account_id=self.forecast_account, auth_token=self.forecast_token)
            for client in api.get_clients():
                client_id = client.id
                name = client.name
                clients.update({client_id: name})
            return clients
        except Exception as e:
            print(f'Error while getting Forecast clients. Error was {e}')

    def get_forecast_users(self):
        """
        Get Forecast users
        :return: users dict with its info: name, role
        """
        users = {}
        try:
            print('Getting Forecast Users')
            api = forecast.Api(account_id=self.forecast_account, auth_token=self.forecast_token)
            for person in api.get_people():
                person_id = person.id
                full_name = person.first_name + ' ' + person.last_name
                role = person.roles[0] if person.roles else None
                users.update({person_id: {"name": full_name, "role": role}})
            return users
        except Exception as e:
            print(f'Error while getting Forecast users. Error was {e}')
