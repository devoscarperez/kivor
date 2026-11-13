import json
import os

import gspread
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

_client = None


def get_sheets_client():
    global _client

    if _client is None:
        raw_credentials = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
        if not raw_credentials:
            raise Exception("GOOGLE_SERVICE_ACCOUNT_JSON no está configurada")

        credentials_info = json.loads(raw_credentials)
        credentials = Credentials.from_service_account_info(credentials_info, scopes=SCOPES)
        _client = gspread.authorize(credentials)

    return _client


def get_sheet_worksheet():
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    sheet_tab = os.getenv("GOOGLE_SHEET_TAB")

    if not sheet_id or not sheet_tab:
        raise Exception("GOOGLE_SHEET_ID o GOOGLE_SHEET_TAB no están configuradas")

    client = get_sheets_client()
    return client.open_by_key(sheet_id).worksheet(sheet_tab)
