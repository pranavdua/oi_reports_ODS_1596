import datetime
import logging
import time

import pandas as pd
from google.auth.exceptions import GoogleAuthError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from libs import utils as util

from datadog import initialize, statsd

# Gets or creates a logger
logger = logging.getLogger(__name__)


# initialize datadog options
options = {
    'statsd_host':'127.0.0.1',
    'statsd_port':8126
}

initialize(**options)

class MoveSheets:

    def __init__(self, credentials, config, google_client):
        try:
            self.google_client = google_client
            self.service_drive = build("drive", "v3", credentials=credentials)
            self.service_sheet = build("sheets", "v4", credentials=credentials)
            logger.info("google sheet/drive authentication done")
            self.config = config
        except GoogleAuthError as e:
            logger.error("Google authentication failed" + str(e))
            raise e

    @staticmethod
    def create_copy_of_spreadsheet(service_drive
                                , source_spreadsheet_id
                                , spreadsheet_name
                                , destination_folder_id
                                , sheet_url):
        """
        Creates copy of spreadsheets to destination folder
        """
        try:
            response = util.duplicate_spreadsheet(
                service_drive,
                source_spreadsheet_id,
                spreadsheet_name,
                destination_folder_id,
            )
            logger.info(
                "Successfully copied spreadsheet with id: {} to destination folder id: {} ".format(
                    source_spreadsheet_id,
                    destination_folder_id,
                )
            )

            sheet_link = sheet_url + response["id"]

            return response, sheet_link

        except GoogleAuthError as error:
            logger.error(
                "Failed to copy sheet with id: {} to destination folder:{}" + str(error)
            )
            raise error


    @staticmethod
    def move_data_within_sheet(service_sheet
                            , spreadsheetid
                            , sheet_info_dict):
        """
        cut and paste the data within a same sheet

        """
        try:
            sheets_data = sheet_info_dict
            responses = list()
            for sheet_data in sheets_data:
                response = util.cutpaste_sheet(
                    service_sheet,
                    spreadsheetid,
                    {
                        "sheetId": sheet_data["sheetId"],
                        "startRowIndex": sheet_data["source_row_start"],
                        "endRowIndex": sheet_data["source_row_end"],
                        "startColumnIndex": sheet_data["source_column_start"],
                        "endColumnIndex": sheet_data["source_column_end"],
                    },
                    {
                        "sheetId": sheet_data["sheetId"],
                        "rowIndex": sheet_data["dest_row_start"],
                        "columnIndex": sheet_data["dest_column_start"],
                    },
                    paste_type="PASTE_VALUES",
                )
                logger.info(
                    "Successfully moved and overwritten data with sheet for spreadsheet with id: {}".format(
                        sheet_data["sheetId"]
                    )
                )
                responses.append(response)
            return responses

        except HttpError as error:
            raise error


    @staticmethod
    def clear_contents_of_specified_cells(service_sheet
                                    , clear_sheet_data_content_list
                                    , spreadsheetid):
        try:
            clear_sheet_cell_info = clear_sheet_data_content_list
            response = util.clear_sheet(
                service_sheet,
                spreadsheetid,
                {
                    "sheetId": clear_sheet_cell_info["sheetId"],
                    "startRowIndex": clear_sheet_cell_info["startRowIndex"],
                    "endRowIndex": clear_sheet_cell_info["endRowIndex"],
                    "startColumnIndex": clear_sheet_cell_info["startColumnIndex"],
                    "endColumnIndex": clear_sheet_cell_info["endColumnIndex"],
                },
            )

            logger.info(
                "Clear contents of specified cells from sheetID : {} ".format(
                    clear_sheet_cell_info["sheetId"]
                )
            )

            return response

        except HttpError as error:
            raise (error)


    @staticmethod
    def renaming_sheets(service_sheet
                    , spreadsheetid
                    , new_sheet_name_dict):
        try:
            renaming_sheet_list = new_sheet_name_dict
            responses = list()
            for sheet in renaming_sheet_list:
                response = util.rename_worksheet(
                    service_sheet,
                    destination=spreadsheetid,
                    sheetid=sheet,
                    new_sheet_name=renaming_sheet_list[sheet],
                )

                logger.info(
                    "Renamed sheet id {} to {}".format(
                        sheet, renaming_sheet_list[sheet]
                    )
                )

                responses.append(response)

        except HttpError as error:
            raise (error)


    @staticmethod
    def delete_scrap_sheets_from_spreadsheet(service_sheet
                                        , sheet_info_dict
                                        , spreadsheetid):
        try:
            # sheets in scope
            sheets_in_scope = [i["sheetId"] for i in sheet_info_dict]

            # # get the data for all the sheets
            all_sheets_data = util.get_sheet_id(service_sheet, spreadsheetid)
            all_sheets = [i["properties"] for i in all_sheets_data["sheets"]]
            all_sheets_id = [str(i["sheetId"]) for i in all_sheets]

            # sheets to be deleted
            sheets_to_delete = [i for i in all_sheets_id if i not in sheets_in_scope]

            # deleting sheets
            responses = []
            for sheet in sheets_to_delete:
                response = util.delete_sheet(
                    service_sheet, spreadsheetid=spreadsheetid, sheetid=sheet
                )
                responses.append(response)
                logger.info("sheet with ID : {} deleted".format(sheet))
            return responses

        except HttpError as error:
            logger.error("Delete sheet Failed")
            raise error



    @staticmethod
    def rename_file(service_drive
                , spreadsheetid
                , new_file_name):
        """
        Renaming a file on drive
        """
        try:            

            response = util.renameFile(service_drive, spreadsheetid, new_file_name)
            logger.info("Renamed spreadsheet to {}".format(new_file_name))
            return response

        except HttpError as error:
            raise error


    
    @staticmethod
    def delete_rows_and_columns(service_sheet
                            , update_sheet_dimension_list
                            , spreadsheetid):
        """
        Deleting specified rows and columns in config file

        """
        try:
            updating_dimensions = update_sheet_dimension_list

            responses = []

            for dimension in updating_dimensions:
                response = util.delete_dimension(
                    service_sheet, spreadsheetid, dimension
                )

                logger.info(
                    "Deleted specified {} from sheetID : {}".format(
                        dimension["dimension"], dimension["sheetId"]
                    )
                )
                responses.append(response)

            return responses

        except HttpError as error:
            raise error


    @staticmethod
    def change_date_type_format(service_sheet
                            , spreadsheetid
                            , column_change_parameters_list):
        """
        change data type of the columns which are supposed to be converted to date format

        """

        try:

            column_change_data = column_change_parameters_list
            response = util.change_column_type(
                service_sheet, spreadsheetid, column_change_data
            )

            logger.info(
                "Data type of column changed to specified date format for Sheet ID : {}".format(
                    column_change_data["sheetId"]
                )
            )

            return response

        except HttpError as error:
            raise error



    @staticmethod
    def load_psp_report_link(google_client
                            , psp_result_df
                            , reporting_table_name_dict):
        try:
            reporting_table_name = '.'.join(list(reporting_table_name_dict))

            logger.info("loading to bigquery starting...")
            logger.info("creating psp report table")

            util.load_table_gbq_append(
                google_client,
                psp_result_df,
                {"tablename": reporting_table_name},
                {}
            )

            logger.info("loading to bigquery ended.")
        except HttpError as e:
            raise e



    @staticmethod
    def read_table_gbq(google_client
        , data_refresh_table_name_dict):
        
        try:
            project_name, dataset_name, table_name = list(data_refresh_table_name_dict)
            query_path = 'config/compare_latest_publish_date.sql'
            query_string = util.readFile(query_path)\
                                .replace('__project__', project_name)\
                                .replace('__dataset__', dataset_name)\
                                .replace('__table__', table_name)


            result_df = util.convert_sql_to_dataframe(query_string, google_client)
            
            if len(result_df) == 0:
                return None

            else:
                # values
                report_star_date = result_df['report_start_date'].item()
                report_end_date = result_df['report_end_date'].item()
                report_publish_date = result_df['report_publish_date'].item()

                return [report_star_date, report_end_date, report_publish_date]

        except HttpError as error:
            raise error



    def create_target_spreadsheet_runner(self):


        # initialize counter
        counter = 3

        # export df format
        df_data = {
                "sheet_link" : [None],
                "run_date" : [None],
                "report_publish_date" : [None],
                "report_start_date" : [None],
                "report_end_date" : [None],
                "comments" : [None],
                "etl_flag" : [None]
            }


        while counter != 0:

            # read PSP refresh data from BQ (start, end and refresh date)
            table_values = self.read_table_gbq(google_client=self.google_client
                ,data_refresh_table_name_dict=self.config['PSP_DATA_REFRESH_TABLE_NAME'].values())

            if table_values == None:
                counter -= 1
                expected_publish_date = datetime.datetime.utcnow().strftime('%Y-%m-%d')
                df_data["run_date"] = [datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')]
                df_data["comments"] = ['PSP spreadsheet publish date {} not present in oi_psp_report_tf.psp_report_refresh_date, process will re-run after an hour'.format(expected_publish_date)]
                df_data["etl_flag"] = ['RETRY']
                df_report_data = pd.DataFrame(df_data, columns=["sheet_link", "run_date", "report_publish_date", "report_start_date", "report_end_date", "comments", "etl_flag"])
                
                self.load_psp_report_link(google_client=self.google_client
                                    ,psp_result_df=df_report_data
                                    ,reporting_table_name_dict=self.config['REPORTING_TABLE_NAME'].values())
                time.sleep(60*60)
                continue

            else:
                start_date, end_date, publish_date = table_values
                break


        if counter != 0:

            # creating a duplicate spreadsheet
            create_copy_spreadsheet_response, sheet_link = self.create_copy_of_spreadsheet(
                service_drive=self.service_drive
                ,source_spreadsheet_id=self.config['SOURCE_SPREADSHEET_ID']
                ,spreadsheet_name=''
                ,destination_folder_id=self.config['DESTINATION_FOLDER_ID']
                ,sheet_url=self.config['SHEET_URL']
                )
            # destination spreadsheet name
            dest_spreadsheet_name = create_copy_spreadsheet_response['id']


            # overwrite file contents
            self.move_data_within_sheet(
                service_sheet=self.service_sheet
                ,spreadsheetid=dest_spreadsheet_name
                ,sheet_info_dict=self.config["SHEETS_INFO"])


            # Delete contents from the cell
            self.clear_contents_of_specified_cells(
                service_sheet=self.service_sheet
                ,clear_sheet_data_content_list=self.config["CLEAR_SHEET_DATA_CONTENT"][0]
                ,spreadsheetid=dest_spreadsheet_name)


            # delete rows and columns from a sheet
            self.delete_rows_and_columns(service_sheet=self.service_sheet
                ,update_sheet_dimension_list=self.config["UPDATE_SHEET_DIMENSION"]
                ,spreadsheetid=dest_spreadsheet_name)


            # change column type to specified date format
            self.change_date_type_format(service_sheet=self.service_sheet
                ,spreadsheetid=dest_spreadsheet_name
                ,column_change_parameters_list=self.config["COLUMN_CHANGE_PARAMETERS"][0])


            # Renaming sheets
            self.renaming_sheets(
                service_sheet=self.service_sheet
                ,spreadsheetid=dest_spreadsheet_name
                ,new_sheet_name_dict=self.config["NEW_SHEETS_NAME"])


            # Delete sheet
            self.delete_scrap_sheets_from_spreadsheet(service_sheet=self.service_sheet
                ,sheet_info_dict=self.config["SHEETS_INFO"]
                ,spreadsheetid=dest_spreadsheet_name)


            date_format = datetime.datetime.utcnow().strftime("%Y.%m.%d")
            file_name = self.config["LATEST_FILE_NAME"].format(date_format)
            # Rename spreadsheet
            self.rename_file(service_drive=self.service_drive
                ,spreadsheetid=dest_spreadsheet_name
                ,new_file_name=file_name)

            
            # load psp report link
            df_data["run_date"] = [datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')]
            df_data["sheet_link"] = sheet_link
            df_data["report_publish_date"] = [publish_date]
            df_data["report_start_date"] = [start_date]
            df_data["report_end_date"] = [end_date]
            df_data["comments"] = ["ETL finished successfully"]
            df_data["etl_flag"] = ["SUCCESS"]

            df_report_data = pd.DataFrame(df_data, columns=["sheet_link", "run_date", "report_publish_date", "report_start_date", "report_end_date", "comments", "etl_flag"])
            
            self.load_psp_report_link(google_client=self.google_client
                                    ,psp_result_df=df_report_data
                                    ,reporting_table_name_dict=self.config['REPORTING_TABLE_NAME'].values())

            # datadog success metric
            statsd.increment('PSP.SUCCESS_METRIC', tags=["PSP:STAGE"])
            statsd.decrement('PSP.FAILURE_METRIC', -0, tags=["PSP:STAGE"])
            


        else:
            df_data["run_date"] = [datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')]
            df_data["comments"] = ['PSP spreadsheet publish date {} not found in oi_psp_report_tf.psp_report_refresh_date'.format(expected_publish_date)]
            df_data["etl_flag"] = ['FAILED']

            df_report_data = pd.DataFrame(df_data, columns=["sheet_link", "run_date", "report_publish_date", "report_start_date", "report_end_date", "comments", "etl_flag"])
            self.load_psp_report_link(google_client=self.google_client
                                    ,psp_result_df=df_report_data
                                    ,reporting_table_name_dict=self.config['REPORTING_TABLE_NAME'].values())

            # datadog failure metric
            statsd.increment('PSP.FAILURE_METRIC', tags=["PSP:STAGE"])
            statsd.decrement('PSP.SUCCESS_METRIC', -0, tags=["PSP:STAGE"])

        # total events datadog metric
        statsd.increment('PSP.TOTAL_EVENTS_METRIC', tags=["PSP:STAGE"])

