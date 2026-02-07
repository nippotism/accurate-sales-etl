import requests
from airflow.models import Variable
from airflow.hooks.base import BaseHook
import base64


def refresh_token():
    """Refreshes an API token and updates the Airflow Variable."""
    conn = BaseHook.get_connection("accurate-api")
    extra = conn.extra_dejson
    
    refresh_token = Variable.get("accurate_refresh_token")

    credentials = base64.b64encode(f"{extra['client_id']}:{extra['client_secret']}".encode()).decode()

    resp = requests.post(
        url=f"{conn.host}/oauth/token",
        headers={
            "Authorization": f"Basic {credentials}",
        },
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
    )

    if resp.status_code == 200:
        data = resp.json()
        Variable.set("accurate_access_token", data["access_token"])
        Variable.set("accurate_refresh_token", data["refresh_token"])

        print("Token refreshed successfully.")

        # Refresh DB Session if needed
        resp_db = requests.get(
            url=f"{conn.host}/api/db-refresh-session.do",
            headers={
                "Authorization": f"Bearer {data['access_token']}"
            },
            params={
                "id": extra["id"],
                "session": Variable.get("accurate_db_session"),
            }
        )

        if resp_db.status_code == 200:
            db_data = resp_db.json()
            Variable.set("accurate_db_session", db_data["d"]["session"])
            print("DB session refreshed successfully.")

            
    else:
        raise Exception(f"Failed to refresh token: {resp.text}")



    