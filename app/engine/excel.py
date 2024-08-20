# pylint: disable = C0301, E0110, E1101

"""Creates excel file from data extracted from payment advices."""

import logging
from datetime import date
from os.path import dirname, exists
from typing import Union

import pandas as pd
from pandas import DataFrame, ExcelWriter, Index, Series
from xlsxwriter.workbook import Workbook
from xlsxwriter.worksheet import Worksheet

FilePath = str

log = logging.getLogger("master")

class FolderNotFoundError(Exception):
	"""Raised when a directory is
	requested but doesn't exist.
	"""

def _to_excel_serial(val: date) -> int:
	"""Converts a date object into excel-compatible
	date integer (serial) format.
	"""
	return (val - date(1899, 12, 30)).days

def _get_col_width(vals: Series, col_name: str) -> float:
	"""Returns excel column width calculated as
	the maximum count of characters contained
	in the column name and column data strings.
	"""

	stringified = vals.astype("string").dropna().str.len()
	val_lst = list(stringified)

	val_lst.append(len(str(col_name.replace('_', ' '))))
	max_width = max(val_lst) + 2

	return max_width

def _get_idx_width(vals: Index) -> float:
	"""Returns excel column width calculated 
	as the maximum char length found among data
	index labels.
	"""

	stringified = vals.astype("string").dropna().str.len()
	val_lst = list(stringified)
	max_width = max(val_lst) + 2

	return max_width

def _col_to_rng(
		data: DataFrame, first_col: Union[str,int],
		last_col: Union[str,int] = None, row: int = -1):
	"""Generates excel data range notation (e.g. 'A1:D1', 'B2:G2'). \n
	If 'last_col' is None, then only single-column range will be generated (e.g. 'A:A', 'B1:B1'). \n
	If 'row' is '-1', then the generated range will span all the column(s) rows (e.g. 'A:A', 'E:E'). \n

	Parameters:
	-----------
	data:
		Source data containing the column names used for range generation.

	first_col:
		First column name representing the start of the range.

	last_col:
		Last column name representing the end of the range.

	row:
		Row index representing the range last row. \n
		the default value represents all rows available

	Returns:
	--------
	If 'first_col' is provided only, then a range coresponding to a single column is returned. \n
	If 'first_col' and 'row' is provided, then a range coresponding to a single column and rows count \n
	equal to the 'row' value is returned. If 'first_col' and 'last_col' are provided, then a range spanning \n
	from the first to the last column is returned. If 'first_col', 'last_col', and 'row' are provided, then a range \n
	spanning from the first to the last column, with rows count equal to the 'row' value is returned.
	"""

	if isinstance(first_col, str):
		first_col_idx = data.columns.get_loc(first_col)
	elif isinstance(first_col, int):
		first_col_idx = first_col
	else:
		assert False, "Argument 'first_col' has invalid type!"

	first_col_idx += 1
	prim_lett_idx = first_col_idx // 26
	sec_lett_idx = first_col_idx % 26

	lett_a = chr(ord('@') + prim_lett_idx) if prim_lett_idx != 0 else ""
	lett_b = chr(ord('@') + sec_lett_idx) if sec_lett_idx != 0 else ""
	lett = "".join([lett_a, lett_b])

	if last_col is None:
		last_lett = lett
	else:

		if isinstance(last_col, str):
			last_col_idx = data.columns.get_loc(last_col)
		elif isinstance(last_col, int):
			last_col_idx = last_col
		else:
			assert False, "Argument 'last_col' has invalid type!"

		last_col_idx += 1
		prim_lett_idx = last_col_idx // 26
		sec_lett_idx = last_col_idx % 26

		lett_a = chr(ord('@') + prim_lett_idx) if prim_lett_idx != 0 else ""
		lett_b = chr(ord('@') + sec_lett_idx) if sec_lett_idx != 0 else ""
		last_lett = "".join([lett_a, lett_b])

	if row == -1:
		rng = ":".join([lett, last_lett])
	else:
		rng = ":".join([f"{lett}{row}", f"{last_lett}{row}"])

	return rng

def _generate_column_formats(wbk: Workbook) -> dict:
	"""Generates column data formats associated with
	an excel workbook representing user report.
	"""

	formats = {}

	formats["general"] = wbk.add_format({"align": "center"})
	formats["date"] = wbk.add_format({"num_format": "dd.mm.yyyy", "align": "center"})
	formats["money"] = wbk.add_format({"num_format": "#,##0.00", "align": "center"})
	formats["integer"] = wbk.add_format({'num_format': "0", "align": "center"})
	formats["category"] = wbk.add_format({'num_format': "000", "align": "center"})

	formats["header"] = wbk.add_format({
		"align": "center",
		"bg_color": "#000000",
		"font_color": "white",
		"bold": True
	})

	return formats

def _format_data_sheet(cust: str, data: DataFrame, sht: Worksheet, formats: dict, header_idx: int) -> None:
	"""Applies data formatting associated with 'Data' worksheet columns and header.
	If the provided customer name is not identified, then no formatting will be applied
	to neither the columns nor the header.
	"""

	# format 'Data' sheet fields
	if cust == "OBI_DE":
		col_formats = {
			"Branch_Number": formats["category"],
			"Gross_Amount": formats["money"],
			"Gross_Amount_(ABS)": formats["money"],
			"Net_Amount": formats["money"],
			"Deduction": formats["money"],
			"Discount": formats["money"],
			"Provision_Discount": formats["money"],
			"Category": formats["category"],
		}

	elif cust == "MARKANT_DE":
		col_formats = {
			"Document_Date": formats["date"],
			"Gross_Amount": formats["money"],
			"Gross_Amount_(ABS)": formats["money"],
			"Markant_SB_Condition": formats["money"],
			"Customer_SB_Condition": formats["money"],
			"Discount": formats["money"],
			"DL_Condition": formats["money"],
			"Net_Amount": formats["money"],
			"Deduction": formats["money"],
			"Provision_Discount": formats["money"],
			"Category": formats["category"],
			"Tax_Rate": formats["money"],
			"ILN": formats["integer"],
		}

	else:
		log.warning(
			"Data sheet will not be formatted! "
			"No sheet formatting has been defined for the customer."
		)
		return

	# column data
	for col_name in data.columns:
		col_width = _get_col_width(data[col_name], col_name)
		col_rng = _col_to_rng(data, col_name)
		fmt = col_formats.get(col_name, formats["general"])
		sht.set_column(col_rng, col_width, fmt)

	# header
	first_col_name = data.columns[0]
	last_col_name = data.columns[-1]

	sht.conditional_format(
		_col_to_rng(data, first_col_name, last_col_name, row = header_idx),
		{"type": "no_errors", "format": formats["header"]}
	)

def _format_notes_sheet(data: DataFrame, sht: Worksheet):
	"""Applies formatting to the 'Notes' worksheet column.
	As of current version, only column width is set.
	"""

	for idx in range(len(data.columns)):
		sht.set_column(idx, idx, _get_idx_width(data.index))

def _generate_notes_data(cust: str) -> DataFrame:
	"""Generates a table to which document \n
	numbers will be written by an accountant \n
	upon posting of the payment in F-30.
	"""

	# leave data generating in a separate function since notes
	# data sheet may further get extended by another parameters
	# in the future

	cust_notes_data = {
		"MARKANT_DE": {"Value": [pd.NA]},
		"OBI_DE": {"Value": [pd.NA]},
	}

	notes = DataFrame(cust_notes_data[cust], index = ["Booking_Number:"])
	assert not notes.empty, "Notes cannot be an empty dataset!"

	return notes

def generate_excel_file(
		data: DataFrame,
		file: FilePath,
		data_sht_name: str,
		notes_sht_name: str,
		customer: str
	) -> None:
	"""Creates an excel file from the converted payment advice data.

	Parameters:
	-----------
	data:
		Accounting items extracted from the payment advice.

	file:
		Path to the .xlsx file to create.

	data_sht_name:
		Accounting data sheet name.

	notes_sht_name:
		Notes data sheet name.

	customer:
		Name of the customer issuing the remittance advice.
	"""

	dst_dir = dirname(file)

	if not file.lower().endswith(".xlsx"):
		raise ValueError(f"Unsupported report file format: '{file}'")

	if not exists(dst_dir):
		raise FolderNotFoundError(f"Destination folder not found: '{dst_dir}'")

	if "" in (data_sht_name, notes_sht_name):
		raise ValueError("Sheet name cannot be an empty string!")

	if customer == "MARKANT_DE":
		data["Document_Date"] = data["Document_Date"].apply(
			lambda x: _to_excel_serial(x) if not pd.isna(x) else x
		)

	notes = _generate_notes_data(customer)

	with ExcelWriter(file, engine = "xlsxwriter") as wrtr:

		# replace underscores in column names with spaces before outputting data to excel
		data.columns = data.columns.str.replace("_", " ")
		notes.index = notes.index.str.replace("_", " ")
		data.to_excel(wrtr, sheet_name = data_sht_name, index = False)
		notes.to_excel(wrtr, sheet_name = notes_sht_name, index = True, header = False)

		# replace spaces in column names back with underscores to avoid
		# any bugs coming from spaces between words in field names
		data.columns = data.columns.str.replace(" ", "_")
		notes.index = notes.index.str.replace(" ", "_")

		data_sht = wrtr.sheets[data_sht_name]
		notes_sht = wrtr.sheets[notes_sht_name]

		formats = _generate_column_formats(wrtr.book)

		_format_data_sheet(customer, data, data_sht, formats, header_idx = 1)
		_format_notes_sheet(notes, notes_sht)
