# pylint: disable = R0914, W0703, W1203

"""
The controller.py represents the middle layer of the application design \n
and mediates communication between the top layer (app.py) and the \n
highly specialized modules situated on the bottom layer of the design \n
(processor.py, mails.py, report.py).
"""

import logging
import os
from datetime import datetime as dt
from datetime import timedelta
from glob import glob
from logging import Logger, config
from os.path import basename, isfile, join
from typing import Union

import json
import yaml

from engine import excel, mails, processor
from engine.processor import obi, markant

log = logging.getLogger("master")


# ====================================
# initialization of the logging system
# ====================================

def _compile_log_path(log_dir: str) -> str:
	"""Compiles the path to the log file
	by generating a log file name and then
	concatenating it to the specified log
	directory path."""

	date_tag = dt.now().strftime("%Y-%m-%d")
	nth = 0

	while True:
		nth += 1
		nth_file = str(nth).zfill(3)
		log_name = f"{date_tag}_{nth_file}.log"
		log_path = join(log_dir, log_name)

		if not isfile(log_path):
			break

	return log_path

def _read_log_config(cfg_path: str) -> dict:
	"""Reads logging configuration parameters from a yaml file."""

	# Load the logging configuration from an external file
	# and configure the logging using the loaded parameters.

	if not isfile(cfg_path):
		raise FileNotFoundError(f"The logging configuration file not found: '{cfg_path}'")

	with open(cfg_path, encoding = "utf-8") as stream:
		content = stream.read()

	return yaml.safe_load(content)

def _update_log_filehandler(log_path: str, logger: Logger) -> None:
	"""Changes the log path of a logger file handler."""

	prev_file_handler = logger.handlers.pop(1)
	new_file_handler = logging.FileHandler(log_path)
	new_file_handler.setFormatter(prev_file_handler.formatter)
	logger.addHandler(new_file_handler)

def _print_log_header(logger: Logger, header: list, terminate: str = "\n") -> None:
	"""Prints header to a log file."""

	for nth, line in enumerate(header, start = 1):
		if nth == len(header):
			line = f"{line}{terminate}"
		logger.info(line)

def _remove_old_logs(logger: Logger, log_dir: str, n_days: int) -> None:
	"""Removes old logs older than the specified number of days."""

	old_logs = glob(join(log_dir, "*.log"))
	n_days = max(1, n_days)
	curr_date = dt.now().date()

	for log_file in old_logs:
		log_name = basename(log_file)
		date_token = log_name.split("_")[0]
		log_date = dt.strptime(date_token, "%Y-%m-%d").date()
		thresh_date = curr_date - timedelta(days = n_days)

		if log_date < thresh_date:
			try:
				logger.info(f"Removing obsolete log file: '{log_file}' ...")
				os.remove(log_file)
			except PermissionError as exc:
				logger.error(str(exc))

def configure_logger(log_dir: str, cfg_path: str, *header: str) -> None:
	"""Configures application logging system.

	Parameters:
	-----------
	log_dir:
		Path to the directory to store the log file.

	cfg_path:
		Path to a yaml/yml file that contains
		application configuration parameters.

	header:
		A sequence of lines to print into the log header.
	"""

	log_path = _compile_log_path(log_dir)
	log_cfg = _read_log_config(cfg_path)
	config.dictConfig(log_cfg)
	logger = logging.getLogger("master")
	_update_log_filehandler(log_path, logger)
	if header is not None:
		_print_log_header(logger, list(header))
	_remove_old_logs(logger, log_dir, log_cfg.get("retain_logs_days", 1))


# ===============================================
#  application configuration and processig rules
# ===============================================

def load_app_config(cfg_path: str) -> dict:
	"""Reads application configuration
	parameters from a file.

	Parameters:
	-----------
	cfg_path:
		Path to a yaml/yml file that contains
		application configuration parameters.

	Returns:
	--------
	Application configuration parameters.
	"""

	log.info("Loading application configuration ...")

	if not cfg_path.lower().endswith((".yaml", ".yml")):
		raise ValueError("The configuration file not a YAML/YML type!")

	with open(cfg_path, encoding = "utf-8") as stream:
		content = stream.read()

	cfg = yaml.safe_load(content)
	log.info("Configuration loaded.")

	return cfg

def load_processing_rules(account_maps_dir: str, file_path: str) -> dict:
	"""Loads customer-specified data processing parameters.

	Parameters:
	-----------
	accounts_map_dir:
		Path to the directory where account maps are stored.

	file_path:
		Path to the file containing the processing rules.

	Returns:
	--------
	Data processing parameters.
	"""

	log.info("Loading customer processing rules ...")

	if not file_path.lower().endswith((".yaml", ".yml")):
		raise ValueError("The configuration file not a YAML/YML type!")

	with open(file_path, encoding = "utf-8") as stream:
		rules = yaml.safe_load(stream)

	for customer, params in rules.copy().items():
		map_name = params.pop("accounting_map")

		if not map_name.endswith(".json"):
			map_name = f"{map_name}.json"

		map_path = join(account_maps_dir, map_name)
		with open(map_path, encoding = "UTF-8-SIG") as stream:
			map_data = json.load(stream)

		rules[customer].update({"accounting_map": map_data})

	log.info("Processing rules loaded.")

	return rules


# ====================================
# 		Fetching of user input
# ====================================

def fetch_user_input(msg_cfg: dict, email_id: str, temp_dir: str) -> dict:
	"""Fetches the processing parameters and data provided by the user.

	Parameters:
	-----------
	msg_cfg:
		Application 'messages' configuration parameters.

	email_id:
		The string ID of the message.

	temp_dir:
		Path to the directory where temporary files are stored.

	Returns:
	--------
	Names of the processing parameters and their values:
	- "error_message": `str` A detailed error message if an exception occurs.
	- "email": `str` Email address of the sender.
	- "attachment_paths": `list[str]` List of paths to downloaded attachments.
	"""

	log.info("Retrieving user message ...")

	params = {
		"error_message": "",
		"email": "",
		"attachment_paths": []
	}

	acc = mails.get_account(
		msg_cfg["requests"]["mailbox"],
		msg_cfg["requests"]["account"],
		msg_cfg["requests"]["server"]
	)

	messages = mails.get_messages(acc, email_id)

	if len(messages) == 0:
		raise RuntimeError(
			f"Could not find message with the specified ID: '{email_id}'"
		)

	msg = messages[0]
	params.update({"email": msg.sender.email_address})

	log.info("User message retrieved.")

	log.info("Saving user attachments ...")
	pdf_paths = mails.save_attachments(
		msg, temp_dir, ext = ".pdf"
	)

	log.info(f"Saved {len(pdf_paths)} pdf files.")

	if len(pdf_paths) == 0:
		params.update({
			"error_message": "The message contains no PDF attachments!"
		})

	params.update({"attachment_paths": pdf_paths})

	if params["error_message"] != "":
		log.error(params["error_message"])

	return params

# ====================================
# 			Document processing
# ====================================

def _extract_document_data(
		pdf_path: str,
		temp_dir: str,
		rules: dict,
		extractor_path: str,
		datas_sht_name: str,
		notes_sht_name: str
	) -> str:
	"""Manages extracting of relevant 
	accounting data from a PDF document.

	Parameters:
	-----------
	pdf_path:
		Path to the pdf file representing 
		a remittance advice document.

	cfg_data:
		Application 'data' configuration parameters.

	rules:
		Customer-specific data processing parameters.

	Returns:
	--------
	Path to the generated excel file.
	"""

	log.info("Identifying customer name form the document  ...")
	customer = processor.identify_customer(
		pdf_path, temp_dir, extractor_path)
	log.info(f"Customer identified: '{customer}'")

	log.info("Extracting text from the PDF file ...")
	options = rules[customer]["conversion_options"]
	text = processor.extract_text(
		pdf_path, temp_dir, extractor_path, options)
	log.info("Extraction completed successfully.")

	log.info("Parsing the extracted text ...")
	acc_map = rules[customer]["accounting_map"]
	layout = rules[customer]["layout"]
	date_format = rules[customer]["date_format"]
	thresh = rules[customer]["threshold"]

	if customer == processor.CUSTOMER_OBI_DE:
		output = obi.parse(text, acc_map, thresh, layout, date_format)
	elif customer == processor.CUSTOMER_MARKANT_DE:
		output = markant.parse(text, acc_map, thresh, layout, date_format)

	log.info("Text parsed successfully.")

	xl_name = rules[customer]["excel_name"]
	xl_name = xl_name.replace("$docnum$", output["remittance_number"])
	xl_name = xl_name.replace("$docdate$", output["remittance_date"])
	xl_name = xl_name.replace("$suppnum$", output["supplier_id"])
	xl_name = xl_name.replace("$doctype$", output["remittance_name"])

	log.info("Writing data to excel ...")
	excel_path = join(temp_dir, f"{xl_name}.xlsx")

	excel.generate_excel_file(
		output["items"], excel_path,
		datas_sht_name, notes_sht_name, customer
	)

	log.info("Data written successfully.")

	return excel_path

def convert_documents(
		rules: dict, pdf_paths: str, temp_dir: str,
		extractor_path: str, excel_cfg: dict
	) -> dict:
	"""Convers payment advice PDFs to Excel files.

	First, the customer who issued the document
	is identified from the document contents.
	Then, data is extracted from the document
	based on the customer-specific rules.

	Parameters:
	-----------
	rules:
		Customer-specific data processing parameters.

	pdf_paths:
		Local paths to the downloaded PDF attachments.

	temp_dir:
		Path to the directory where temporary files are stored.

	extractor_path:
		Path to the executable that performs text extraction from a PDF.

	excel_cfg:
		Application 'excel' configuration parameters.

	Returns:
	--------
	The processing result, with the following keys and values:
		"error_message": `str, None`
			An error message if an exception occurs, otherwise None.
		"excel_paths": `list[str]`
			Paths to the generated Excel file(s) if a PDF
			was successfully converted, otherwise [].
	"""

	assert isfile(extractor_path), f"Data extractor not found: '{extractor_path}'"

	result = {"error_message": "", "excel_paths": []}

	for nth, pdf_path in enumerate(pdf_paths, start = 1):

		file_name = basename(pdf_path)
		log.info(f"Processing file ({nth} of {len(pdf_paths)}): '{file_name}' ...")

		try:
			excel_path = _extract_document_data(
				pdf_path, temp_dir, rules,
				extractor_path,
				excel_cfg["data_sheet_name"],
				excel_cfg["notes_sheet_name"])
		except Exception as exc:
			log.error(exc)
			result.update({"error_message": str(exc)})
			return result

		result["excel_paths"].append(excel_path)

	if len(result["excel_paths"]) == 0:
		raise RuntimeError("Processing failed for all files!")

	return result

# ====================================
# 	Reporting of processing output
# ====================================

def send_notification(
		msg_cfg: dict,
		user_mail: str,
		template_dir: str,
		attachment: Union[dict, str] = None,
		error_msg: str = ""
	) -> None:
	"""Sends a notification with processing result to the user.

	Parameters:
	-----------
	msg_cfg:
		Application 'messages' configuration parameters.

	template_dir:
		Path to the application directory
		that contains notification templates.

	user_mail:
		Email address of the user who requested processing.

	attachment:
		Attachment name and data or a file path.

	error_msg:
		Error message that will be included in the user notification.
		By default, no erro message is included.
	"""

	log.info("Sending notification to user ...")

	notif_cfg = msg_cfg["notifications"]

	if not notif_cfg["send"]:
		log.warning(
			"Sending of notifications to users "
			"is disabled in 'appconfig.yaml'.")
		return

	if error_msg != "":
		templ_name = "template_error.html"
	else:
		templ_name = "template_completed.html"

	templ_path = join(template_dir, templ_name)

	with open(templ_path, encoding = "utf-8") as stream:
		html_body = stream.read()

	if error_msg != "":
		html_body = html_body.replace("$error_msg$", error_msg)

	if attachment is None:
		msg = mails.create_smtp_message(
			notif_cfg["sender"], user_mail,
			notif_cfg["subject"], html_body
		)
	elif isinstance(attachment, dict):
		msg = mails.create_smtp_message(
			notif_cfg["sender"], user_mail,
			notif_cfg["subject"], html_body,
			{attachment["name"]: attachment["content"]}
		)
	elif isinstance(attachment, (str, list)):
		msg = mails.create_smtp_message(
			notif_cfg["sender"], user_mail,
			notif_cfg["subject"], html_body,
			attachment
		)
	else:
		raise ValueError(f"Unsupported data type: '{type(attachment)}'!")

	try:
		mails.send_smtp_message(msg, notif_cfg["host"], notif_cfg["port"])
	except Exception as exc:
		log.error(exc)
		return

	log.info("Notification sent.")


# ====================================
# 			Data cleanup
# ====================================

def delete_temp_files(temp_dir: str) -> None:
	"""Removes all temporary files.

	Parameters:
	-----------
	temp_dir:
		Path to the directory where temporary files are stored.
	"""

	file_paths = glob(join(temp_dir, "*.*"))

	if len(file_paths) == 0:
		return

	log.info("Removing temporary files ...")

	for file_path in file_paths:
		try:
			os.remove(file_path)
		except Exception as exc:
			log.exception(exc)

	log.info("Files successfully removed.")
