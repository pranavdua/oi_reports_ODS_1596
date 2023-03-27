import datetime
import logging
import logging.config
from ast import Dict, Str
import pandas as pd

from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


# get sheet IDs from a spreadsheet
def get_sheet_id(service_sheets, spreadsheet_id: str):
    """
    Get details of all the sheets present in spreadsheet
    1. service_sheets - service object for drive
    2. spreadsheetid : Spreadsheet ID for which sheet details are required
    """
    try:
        request = service_sheets.spreadsheets().get(spreadsheetId=spreadsheet_id)

        response = request.execute()

        return response

    except HttpError as error:
        raise error


# Duplicate spreadsheet
def duplicate_spreadsheet(
    service_drive,
    source_spreadsheetid: str,
    destination_spreadsheetid_name: str,
    destination_folderid: str,
):
    """
    Create a copy of existing spradsheet in the destination folder
    1. service_drive - service object for drive
    2. source_spreadsheetid -
    3. destination_spreadsheetid_name -
    4. destination_folderid -
    """
    try:
        newfile = {
            "title": destination_spreadsheetid_name,
            "parents": [destination_folderid],
        }
        request = service_drive.files().copy(fileId=source_spreadsheetid, body=newfile)
        response = request.execute()

        logger.info(
            "Duplicate spreadsheet created with following id : {}".format(
                response["id"]
            )
        )

        return response

    except HttpError as error:
        raise error


# Duplicate Sheet
def duplicate_sheet(
    service_sheets,
    spreadsheet_id: str,
    source_dict: Dict,
    destination_dict: Dict,
    paste_type: str,
):
    """
    This function creates a duplicated sheet within the spreadsheet for the respective sheet id passed
    User can also edit the paste type of data which could be VALUES, FORMAT, ALL ETC
    1. service_sheets - service object for sheets
    2. spreadsheet_id - Respective Spreadsheet ID
    3. source_dict - Source dict contains the source sheet parameters
    4. destination_dict - Destination dict contains the destination sheet parameters
    5. paste_type - Paste type of the spreadsheet
    """
    try:
        request_body = {
            "requests": [
                {
                    "copyPaste": {
                        "source": source_dict,
                        "destination": destination_dict,
                        "pasteType": paste_type,
                    }
                }
            ]
        }

        response = (
            service_sheets.spreadsheets()
            .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
            .execute()
        )

        return response

    except HttpError as error:
        raise error


# Duplicate Sheet
def cutpaste_sheet(
    service_sheets,
    spreadsheet_id: str,
    source_dict: Dict,
    destination_dict: Dict,
    paste_type: str,
):
    """
    This function creates a duplicated sheet within the spreadsheet for the respective sheet id passed
    User can also edit the paste type of data which could be VALUES, FORMAT, ALL ETC
    1. service_sheets - service object for sheets
    2. spreadsheet_id - Respective Spreadsheet ID
    3. source_dict - Source dict contains the source sheet parameters
    4. destination_dict - Destination dict contains the destination sheet parameters
    5. paste_type - Paste type of the spreadsheet
    """
    try:
        request_body = {
            "requests": [
                {
                    "cutPaste": {
                        "source": source_dict,
                        "destination": destination_dict,
                        "pasteType": paste_type,
                    }
                }
            ]
        }

        response = (
            service_sheets.spreadsheets()
            .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
            .execute()
        )

        return response

    except HttpError as error:
        raise error


# Delete sheet from a spreadsheet
def delete_sheet(service_sheets, spreadsheetid: str, sheetid: str):
    """
    Delete a spreadsheet or a sheet based on the ID passed
    1. service_sheets - build object for spreadsheets
    2. spreadsheetid - spreadsheet which contain the sheet which is to be deleted
    3. sheetid - sheet amongst the spreadsheet which is to be deleted
    """
    try:
        request_body = {"requests": [{"deleteSheet": {"sheetId": sheetid}}]}

        request = service_sheets.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheetid, body=request_body
        )
        response = request.execute()

        return response

    except HttpError as error:
        raise error


# clear the contents of sheet cells
def clear_sheet(service_sheets, spreadsheet_id: Str, sheet_dict: Str):
    """
    This clears the contents of the sheet based on the range of cells give
    1. service_sheets - build object for sheets
    2. spreadsheet_id - Spreadsheet ID which contains the sheet
    3. sheet_id - respective sheet ID which is supposed to be cleared
    """
    try:
        body = {"requests": [{"updateCells": {"range": sheet_dict, "fields": "*"}}]}
        resultClear = (
            service_sheets.spreadsheets()
            .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
            .execute()
        )

        return resultClear

    except HttpError as error:
        raise error


# copy sheet from one spreadsheet to another spreadsheet
def copy_sheets(
    service_sheets, source_spreadsheet_id: str, target_spreadsheet_id: str, sheetid: str
):
    """
    1. service_sheets - build object for service sheet
    2. source_spreadsheet_id - Source spreadsheet ID
    3. target_spreadsheet_id - Destination spreadsheet ID
    4. sheetid - Sheet to be copied
    """
    try:
        copy_sheet_to_another_spreadsheet_request_body = {
            # The ID of the spreadsheet to copy the sheet to.
            "destination_spreadsheet_id": target_spreadsheet_id,
        }

        request = (
            service_sheets.spreadsheets()
            .sheets()
            .copyTo(
                spreadsheetId=source_spreadsheet_id,
                sheetId=sheetid,
                body=copy_sheet_to_another_spreadsheet_request_body,
            )
        )
        response = request.execute()

        return response
    except HttpError as error:
        raise error


# create empty sheets at the destination folder in drive
def create_empty_sheets(service_sheets, spreadsheet_id: str, title: str):
    """
    Create an empty sheets within a spreadsheet
    1. Spreadsheet_id - spreadsheet id which conatins the sheet
    2. Title - Title of the sheets(Name)
    """
    try:
        request_body = {"requests": [{"addSheet": {"properties": {"title": title}}}]}
        response = (
            service_sheets.spreadsheets()
            .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
            .execute()
        )

        temp_sheet_id = response["replies"][0]["addSheet"]["properties"]["sheetId"]

        return temp_sheet_id

    except HttpError as error:
        raise error


# Create a new empty spreadsheet
def create_new_spreadsheet(service_drive, folder_id: str):
    """
    Functions creates a new spreadsheet and returns the spreadsheet ID
    1. service_drive - build object for drive
    2. Folder ID - Parent folder ID where you wish to create a spreadsheet
    """
    try:
        report_name = "Staging sheet %s" % (
            datetime.datetime.utcnow().strftime("%d %B %Y")
        )
        file_metadata = {
            "name": report_name,
            "parents": [folder_id],
            "mimeType": "application/vnd.google-apps.spreadsheet",
        }
        response = service_drive.files().create(body=file_metadata).execute()

        return response["id"]

    except HttpError as error:
        raise error


# rename a sheet within a worksheet
def rename_worksheet(
    service_sheet, destination: str, sheetid: str, new_sheet_name: str
):

    """
    Renames a sheet with the specified ID

    """
    try:
        body = {
            "requests": {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheetid,
                        "title": new_sheet_name,
                    },
                    "fields": "title",
                }
            }
        }
        response = (
            service_sheet.spreadsheets()
            .batchUpdate(spreadsheetId=destination, body=body)
            .execute()
        )
        return response

    except HttpError as error:
        raise error


# change font color within a sheet with the specified row and column range
def change_font_color(service_sheet, key, color, ranges):
    """
    Changes the colors of the specified cells

    """
    try:
        data = {
            "requests": [
                {
                    "repeatCell": {
                        "range": r,
                        "cell": {
                            "userEnteredFormat": {
                                "textFormat": {
                                    "foregroundColor": {  # color of text
                                        "red": color[0],
                                        "green": color[1],
                                        "blue": color[2],
                                    },
                                }
                            }
                        },
                        "fields": "userEnteredFormat.textFormat.foregroundColor",
                    }
                }
                for r in ranges
            ]
        }
        service_sheet.spreadsheets().batchUpdate(spreadsheetId=key, body=data).execute()

    except HttpError as error:
        raise error


# rename a file kept at a destination on drive
def renameFile(service_drive: str, fileId: str, newTitle: str):
    """
    This renames a file kept on drive

    """

    try:
        body = {"name": newTitle}
        response = service_drive.files().update(fileId=fileId, body=body).execute()

        return response

    except HttpError as error:
        raise error


# delete row or column with the specified range
def delete_dimension(service_sheet: str, spreadsheetId: str, sheet_dimension_dict: str):
    """
    Deletes the dimension passed in the dictionary like row or dimension

    """
    try:
        spreadsheet_data = {
            "requests": [
                {
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheet_dimension_dict["sheetId"],
                            "dimension": sheet_dimension_dict["dimension"],
                            "startIndex": sheet_dimension_dict["startIndex"],
                            "endIndex": sheet_dimension_dict["endIndex"],
                        }
                    }
                }
            ]
        }

        service_sheet.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheetId, body=spreadsheet_data
        ).execute()

    except HttpError as error:
        raise error


# change column types to the specified format
def change_column_type(service_sheet, spreadsheetId, sheet_data_dict):
    """
    Change columns type

    """
    try:
        spreadsheet_data = {
            "requests": [
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_data_dict["sheetId"],
                            "startRowIndex": sheet_data_dict["startRowIndex"],
                            "endRowIndex": sheet_data_dict["endRowIndex"],
                            "startColumnIndex": sheet_data_dict["startColumnIndex"],
                            "endColumnIndex": sheet_data_dict["endColumnIndex"],
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "numberFormat": {
                                    "type": "DATE",
                                    "pattern": "mm/dd/yyyy hh:mm:ss",
                                }
                            }
                        },
                        "fields": "userEnteredFormat.numberFormat",
                    }
                }
            ]
        }

        response = (
            service_sheet.spreadsheets()
            .batchUpdate(spreadsheetId=spreadsheetId, body=spreadsheet_data)
            .execute()
        )
        return response

    except HttpError as error:
        raise error


def load_table_gbq_append(client, dataframe, request_dict: dict, schema: dict):
    """
    function to load table to google big query
    """
    try:
        tablename: str = "{}".format(request_dict["tablename"])
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job_config.schema = format_schema(schema)
        jobtodf = client.load_table_from_dataframe(
            dataframe, tablename, job_config=job_config
        )
        logger.info(
            "table {} created from the dataframe.".format(request_dict["tablename"])
        )
        return jobtodf.result()
    except BadRequest as e:
        logger.error("Unable to run query: {}".format(e["message"]))


def format_schema(schema: dict) -> list:
    """
    Converts schema dictionary to BigQuery's expected format for job_config.schema
    """
    formatted_schema = []
    for row in schema:
        formatted_schema.append(
            bigquery.SchemaField(row["name"], row["type"], row["mode"])
        )
    return formatted_schema


def convert_sql_to_dataframe(
    query: str, google_client: bigquery.Client
) -> pd.DataFrame:
    """
    Function to convert SQL query and extract data from given table and put it into a dataframe
    """

    try:
        job = google_client.query(query)

        dataframe = job.to_dataframe()
        logger.info("Successfully executed query : {}".format(query))
        return dataframe

    except BadRequest as e:
        if job.errors:
            for e in job.errors:
                logger.error("Unable to run query: {}".format(e["message"]))
        else:
            logger.error("Unable to run query: {}".format(e.message))
        return None


def readFile(filename: str) -> str:
    """
    Read and return the contents of a file!
    """

    file = open(filename, "r")
    content = file.read()
    file.close()
    return content



def find_files_gdrive(service_drive, folder_id):
    sheetMymeType = "application/vnd.google-apps.spreadsheet"
    parent = folder_id
    q = "mimeType = '{}' and parents in '{}'".format(sheetMymeType, parent)
    g_files = service_drive.files().list(q=q, fields='files(name,id)').execute()
    return [i['name'] for i in g_files['files']]

