# # import python libs
# import unittest
# import json
# import os

# # import google libs
# from google.cloud import bigquery
# from google.oauth2 import service_account
# from googleapiclient.discovery import build

# # import utils file functions
# from src.move_sheets import MoveSheets
# from libs import utils as util

# class MyTestCase(unittest.TestCase):
#   def setUp(self):
#      self.setUpMyStuff()


# class TestMoveSheets(MyTestCase):

# 	def setUpMyStuff(self):
# 		self.client_secrets_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

# 		with open("config/config_file.json") as config_file:
# 		    self.config = json.load(config_file)

# 		with open("config/test_config.json") as config_file:
# 		    self.test_config = json.load(config_file)		

# 		self.credentials = service_account.Credentials.from_service_account_file(
# 		    self.client_secrets_path, scopes=self.config["SCOPES"])

# 		self.google_client = bigquery.Client()
# 		self.service_drive = build("drive", "v3", credentials=self.credentials)
# 		self.service_sheet = build("sheets", "v4", credentials=self.credentials)


# 		self.mv_test = MoveSheets(self.credentials, self.config, self.google_client)

 
# 	def test_create_copy_of_spreadsheet(self):

# 		# constants *******************-------------------------------*****************************
# 		constants_dict = self.test_config["test_create_copy_of_spreadsheet"]
# 		destination_folder_id = constants_dict['destination_folder_id']
# 		source_spreadsheet_id = constants_dict['source_spreadsheet_id']
# 		copied_source_spreadsheet_name = constants_dict['copied_source_spreadsheet_name']
		
# 		# create copy of spreadsheet
# 		response = self.mv_test.create_copy_of_spreadsheet(self.service_drive
#                                , source_spreadsheet_id
#                                , ''
#                                , destination_folder_id
        
#                                , '')

# 		# get newly created spreadsheet name
# 		file_name_list = util.find_files_gdrive(self.service_drive, destination_folder_id)
		
# 		self.assertIn(copied_source_spreadsheet_name, file_name_list)
# 		self.service_drive.files().delete(fileId=response[0]['id']).execute()



# 	def test_rename_file(self):

# 		# constants *******************-------------------------------*****************************
# 		constants_dict = self.test_config["test_rename_file"]
# 		destination_folder_id = constants_dict['destination_folder_id']
# 		source_spreadsheet_id = constants_dict['source_spreadsheet_id']
# 		renamed_file_name = constants_dict['renamed_file_name']

# 		# create copy of spreadsheet
# 		response = self.mv_test.create_copy_of_spreadsheet(self.service_drive
#                                , source_spreadsheet_id
#                                , ''
#                                , destination_folder_id
#                                , '')

# 		self.mv_test.rename_file(self.service_drive
# 								,response[0]['id']
# 								,renamed_file_name)

# 		# get newly created spreadsheet name
# 		file_name_list = util.find_files_gdrive(self.service_drive, destination_folder_id)

# 		self.assertIn(renamed_file_name, file_name_list)
# 		self.service_drive.files().delete(fileId=response[0]['id']).execute()



# 	def test_delete_scrap_sheets_from_spreadsheet(self):

# 		# constants *******************-------------------------------*****************************
# 		constants_dict = self.test_config["test_delete_scrap_sheets_from_spreadsheet"]
# 		source_spreadsheet_id = constants_dict['source_spreadsheet_id']
# 		destination_folder_id = constants_dict['destination_folder_id']
# 		sheet_info_dict = constants_dict['sheet_info_dict']

# 		sheets_in_scope_list = [i['sheetId'] for i in sheet_info_dict]

# 		# create copy of spreadsheet
# 		response = self.mv_test.create_copy_of_spreadsheet(self.service_drive
#                                , source_spreadsheet_id
#                                , ''
#                                , destination_folder_id
#                                , '')

# 		self.mv_test.delete_scrap_sheets_from_spreadsheet(self.service_sheet
# 			,sheet_info_dict
# 			,response[0]['id'])

# 		all_sheets_data = util.get_sheet_id(self.service_sheet, response[0]['id'])
# 		all_sheets = [i["properties"] for i in all_sheets_data["sheets"]]
# 		all_sheets_id = [str(i["sheetId"]) for i in all_sheets]

# 		self.assertEqual(sheets_in_scope_list, all_sheets_id)
# 		self.service_drive.files().delete(fileId=response[0]['id']).execute()


# 	def test_renaming_sheets(self):


# 		# # constants *******************-------------------------------*****************************
# 		constants_dict = self.test_config["test_renaming_sheets"]
# 		source_spreadsheet_id = constants_dict['source_spreadsheet_id']
# 		destination_folder_id = constants_dict['destination_folder_id']
# 		rename_sheet_dict = constants_dict['rename_sheet_dict']

# 		sheets_in_scope_names = list(rename_sheet_dict.values())

# 		# create copy of spreadsheet
# 		response = self.mv_test.create_copy_of_spreadsheet(self.service_drive
#                                , source_spreadsheet_id
#                                , ''
#                                , destination_folder_id
#                                , '')

# 		self.mv_test.renaming_sheets(service_sheet=self.service_sheet
#         	,spreadsheetid=response[0]['id']
#         	,new_sheet_name_dict=rename_sheet_dict)


# 		all_sheets_data = util.get_sheet_id(self.service_sheet, response[0]['id'])
# 		all_sheets = [i["properties"] for i in all_sheets_data["sheets"]]
# 		all_sheets_name = [str(i["title"]) for i in all_sheets]

# 		self.assertEqual(sheets_in_scope_names, all_sheets_name)
# 		self.service_drive.files().delete(fileId=response[0]['id']).execute()


# if __name__ == '__main__':
#     unittest.main()