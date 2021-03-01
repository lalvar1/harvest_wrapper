import logging
import urllib3
import os
from dotenv import load_dotenv
from harvest.harvest_wrapper import HarvestAnalytics
from float.float_wrapper import FloatAnalytics
from gsheet.gsheet_wrapper import GoogleRunner
# from .myforecast.forecast_wrapper import ForecastAnalytics


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
FLOAT_TOKEN = os.environ["FLOAT_TOKEN"]
# FORECAST_SHEET = os.environ["FORECAST_SHEET"]
# FORECAST_TOKEN = os.environ["FORECAST_TOKEN"]
# FORECAST_ACCOUNT_ID = os.environ["FORECAST_ACCOUNT_ID"]


def main(event, context):
    logging.info(f'Payload input data is {event} and context {context}')
    google_runner = GoogleRunner(SPREADSHEET_ID, CREDENTIALS_FILE, ENTRIES_SHEET, LOGS_SHEET,
                                 ROLES_SHEET, WEEKLY_TASKS_SHEET, PROJECTS_SHEET)
    weekly_entries = google_runner.get_weekly_entries()
    eligible_roles = google_runner.get_eligible_roles()
    harvest_runner = HarvestAnalytics(PAST_ENTRIES_LOOKUP, HARVEST_ACCOUNT_ID, HARVEST_TOKEN,
                                      weekly_entries, eligible_roles)
    # harvest_runner.create_weekly_entries()

    # harvest_entries = harvest_runner.get_historical_data()
    # new_rows = google_runner.get_missing_rows(harvest_entries, PAST_ENTRIES_LOOKUP)

    # updated_cells = google_runner.gsheet_append(ENTRIES_SHEET, new_rows)
    # google_runner.log_update(updated_cells, ENTRIES_SHEET, "entries")
    # projects_status = harvest_runner.get_project_rows()
    # projects_range = f'{PROJECTS_SHEET}!A2:M'
    # updated_cells = google_runner.gsheet_update(projects_range, projects_status)
    # google_runner.log_update(updated_cells, PROJECTS_SHEET, "projects")
    # new_rows = google_runner.read_gsheet_data(ENTRIES_SHEET)
    harvest_users = harvest_runner.harvest_users
    harvest_projects = harvest_runner.harvest_projects
    float_runner = FloatAnalytics(FLOAT_TOKEN, users=harvest_users, projects=harvest_projects)
    float_runner.sync_people()
    float_runner.sync_projects()
    float_runner.create_tasks_from_ghseet(new_rows)
    # forecast_runner = ForecastAnalytics(FORECAST_ACCOUNT_ID, FORECAST_TOKEN)
    # forecast_assignments = forecast_runner.get_forecast_assignments()
    # updated_cells = google_runner.gsheet_append(FORECAST_SHEET, forecast_assignments)
    # google_runner.log_update(updated_cells, FORECAST_SHEET, "entries")


if __name__ == "__main__":
    main('data', 'context')
