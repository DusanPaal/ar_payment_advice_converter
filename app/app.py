# pylint: disable = C0103, W0703, W1203

"""
The “AR Payment Advice Converter” application extracts accounting data
from customer payment advice documents received as PDF files. The user
sends the document to a specified email address. The automation downloads
the attached PDF files, converts them to plain text, and then extracts all
relevant accounting information from the text strings. Finally, the extracted
data is printed to a separate sheet of an Excel workbook, formatted, and 
emailed back to the user. In the current version, the conversion of payment
advice notes issued by OBI Germany and Markant Germany is implemented.

Version history:
----------------
1.0.20220722 - Initial version.
1.0.20220907 - Minor code style improvements.
				- Added docstrings to biaServices.py and biaProcessor.py modules.
1.0.20221018 - Minor code style improvements.
				- Updated docstrings.
1.0.20230117 - Minor refactoring with code style improvements.
				- Updated docstrings.
"""

import argparse
import logging
from os.path import join
import sys
from datetime import datetime as dt
from engine import controller

log = logging.getLogger("master")

def main(args: dict) -> int:
	"""Program entry point.

	Controls the overall execution of the program.

	Parameters (args):
	------------------
	- "email_id":
		The string ID of the user message
		that triggers the application.

	Returns:
	--------
	Program completion state:
	- 0: Program successfully completes.
	- 1: Program fails during the initialization phase.
	- 2: Program fails during the user input fetch phase.
	- 3: Program fails during the processing phase.
	- 4: Program fails during the reporting phase.
	"""

	app_dir = sys.path[0]
	log_dir = join(app_dir, "logs")
	temp_dir = join(app_dir, "temp")
	rules_path = join(app_dir, "rules.yaml")
	app_cfg_path = join(app_dir, "app_config.yaml")
	log_cfg_path = join(app_dir, "log_config.yaml")
	template_dir = join(app_dir, "notification")
	accounts_map_dir = join(app_dir, "maps")
	extractor_path = join(app_dir, "engine", "processor", "pdftotext.exe")
	curr_date = dt.now().strftime("%d-%b-%Y")

	try:
		controller.configure_logger(
			log_dir, log_cfg_path,
			"Application name: AR Payment Advice Converter",
			"Application version: 1.0.20230117",
			f"Log date: {curr_date}")
	except Exception as exc:
		print(exc)
		print("CRITICAL: Unhandled exception while configuring the logging system!")
		return 1

	try:
		log.info("=== Initialization START ===")
		cfg = controller.load_app_config(app_cfg_path)
		rules = controller.load_processing_rules(accounts_map_dir, rules_path)
		log.info("=== Initialization END ===\n")
	except Exception as exc:
		log.exception(exc)
		log.critical("=== Initialization FAILURE ===")
		return 2
	
	try:
		log.info("=== Fetching user input START ===")
		user_input = controller.fetch_user_input(
			cfg["messages"], args["email_id"], temp_dir)
		log.info("=== Fetching user input END ===\n")
	except Exception as exc:
		log.exception(exc)
		log.critical("=== Fetching user input FAILURE ===")
		log.info("=== Cleanup START ===")
		controller.delete_temp_files(temp_dir)
		log.info("=== Cleanup END ===\n")
		return 2
	 
	if user_input["error_message"] != "":
		log.info("=== Reporting START ===")
		controller.send_notification(
			cfg["messages"], user_input["email"], template_dir,
			error_msg = user_input["error_message"])
		log.info("=== Reporting END ===\n")
		return 2
	  
	log.info("=== Processing START ===")
	  
	try:
		output = controller.convert_documents(
			rules, user_input["attachment_paths"],
			temp_dir, extractor_path, cfg["excel"])
	except Exception as exc:
		log.exception(exc)
		log.critical("=== Processing FAILURE ===\n")
		log.info("=== Cleanup START ===")
		controller.delete_temp_files(temp_dir)
		log.info("=== Cleanup END ===\n")
		return 3
	
	if output["error_message"] != "":
		log.info("=== Reporting START ===")
		controller.send_notification(
			cfg["messages"], user_input["email"],
			template_dir, user_input["attachment_paths"],
			error_msg = output["error_message"])
		log.info("=== Reporting END ===\n")
		log.info("=== Cleanup START ===")
		controller.delete_temp_files(temp_dir)
		log.info("=== Cleanup END ===\n")
		return 3

	log.info("=== Processing END ===\n") 

	try:
		log.info("=== Reporting START ===")
		attachmens = user_input["attachment_paths"] + output["excel_paths"]
		controller.send_notification(
			cfg["messages"], user_input["email"],
			template_dir, attachmens)
		log.info("=== Reporting END ===\n")
	except Exception as exc:
		log.exception(exc)
		return 4
	finally:
		log.info("=== Cleanup START ===")
		controller.delete_temp_files(temp_dir)
		log.info("=== Cleanup END ===\n")

	return 0

if __name__ == "__main__":

	parser = argparse.ArgumentParser()
	parser.add_argument("-e", "--email_id", required = True, help = "Sender email id.")
	arguments = vars(parser.parse_args())

	exit_code = main(arguments)
	log.info(f"=== System shutdown with return code: {exit_code} ===")
	logging.shutdown()
	sys.exit(exit_code)
