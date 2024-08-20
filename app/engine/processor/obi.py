"""
The module parses payment (remittance) advices issued by the OBI company.
Current version supports parsing of German version of the documents only.
"""

import re
from typing import Union
import pandas as pd
from pandas import DataFrame, Series
from . import ParsingError, parse_amount, parse_amounts

VAT_CODE_FRANCE_INTRACOMMUNITY = "C3"
VAT_CODE_GERMANY_NO_TAX_PROCEDURE = "A0"
OBI_GERMANY_HEAD_OFFICE_BUSINESS_UNIT = "850"
OBI_GERMANY_TRANSPORT_COSTS_BUSINESS_UNIT = "875"
OBI_GERMANY_BONUS_BUSINESS_UNIT = "950"


def _get_item_type(gross_amnt: float,doc_num: str, thresh: float = 0.0) -> str:
	"""Returns the type of an accounting item
	contained in a remittance advice."""

	if thresh <= 0:
		raise ValueError("Threshold cannot be a negative!")

	penalty_prefixes = ("DE", "PE")

	if gross_amnt <= -1 * thresh :
		doc_type = "Debit"
	elif  -1 * thresh < gross_amnt < 0.00:
		if doc_num.startswith(penalty_prefixes):
			doc_type = "WriteOff Penalty"
		else:
			doc_type = "WriteOff Others"
	elif re.search(r"^41\d{7}", doc_num) is None:
		doc_type = "Credit"
	else:
		doc_type = "Credit/Invoice"

	return doc_type

def _get_gl_account(doc_types: Series) -> Union[int,None]:
	"""Returns a GL account based on to the data provided.
	If the procedure fails to assign an account to the
	data, then None is returned.
	"""

	gl_acc = None

	if doc_types == "WriteOff Penalty":
		gl_acc = 66010030 # penalties
	elif doc_types == "WriteOff Others":
		gl_acc = 66791580 # delivery, price difference, return, bonus

	return gl_acc

def _parse_obi_de(text: str) -> dict:
	"""Parses converted document text for OBI DE.
	Returns a dict of parsed data as well as other
	relevant accouning information about the document.
	"""

	if text == "":
		raise ValueError("Cannot parse an empty string!")

	match = re.search(r"Überweisung Nr. (?P<PaymAdvNum>\d{8}) ", text, re.M)
	assert match, "Payment advice number not found!"
	doc_num = match.group("PaymAdvNum")

	match = re.search(r"Datum (?P<Date>\d{2}\.\d{2}\.\d{4}) ", text, re.M)
	assert match, "Payment advice date not found!"
	doc_date = pd.to_datetime(match.group("Date"), dayfirst = True).date()

	match = re.search(r"Ihre Kto-Nr bei uns (?P<Supplier>\d{4})", text, re.M)
	assert match, "Supplier number not found!"
	supplier_id = match.group("Supplier")

	matches = re.search(r"Gesamt-Summe:\s+(?P<TotDed>\S+)\s+(?P<TotNetAmnt>\S+)", text, re.M)
	assert match, "Total amounts not found!"

	tot_net_amount = parse_amount(matches.group("TotNetAmnt"))
	tot_doc_ded_amnt = parse_amount(matches.group("TotDed"))

	lines = re.findall(r"""
		(\d*).*EUR\s+			# branch
		(\d\S+)\s+				# gross amount
		(\S+)\s+				# deduction
		(\S+)\n\n				# net amount
		(\S+).*\n+				# document number
		(\S.*)?					# note
		""",
	text, re.M|re.X)

	# store extracted item data into data frame
	items = DataFrame(
		lines, columns = [
			"Branch_Number",
			"Gross_Amount",
			"Deduction",
			"Net_Amount",
			"Document_Number",
			"Note"
		]
	)

	# parse amounts by swapping trailing minus where applicable
	# and subsequent converting to appropriate data type
	items["Gross_Amount"] = parse_amounts(items["Gross_Amount"])
	items["Deduction"] = parse_amounts(items["Deduction"])
	items["Net_Amount"] = parse_amounts(items["Net_Amount"])
	items["Document_Number"] = items["Document_Number"].astype("string")
	items["Note"] = items["Note"].astype("string")

	output = {
		"items": items,
		"remittance_number": doc_num,
		"remittance_date": doc_date,
		"remittance_type": None,
		"supplier_id": supplier_id,
		"total_net_amount": tot_net_amount,
		"total_deductions": tot_doc_ded_amnt
	}

	return output

def parse(text: str, accounting_map: dict, threshold: float, fields: list, date_format: str) -> dict:
	"""Parses the text extracded from a PDF payment advice.

	If parsing of the text fails, then a `ParsingError` exception is raised.

	Parameters:
	-----------
	text:
		Plain text extracted from remittance advice.

	accounting_map:
		Supplier ID numbers and branch numbers to customer accounts.

	threshold:
		The amount limit below which items are written off.

	fields:
		Field names that define the ordering of columns in the processed data.

	date_format:
		An explicit format string that controls the resulting format of the document date.

	Returns:
	--------
	Accounting parameters and their values:
		- "items": `pandas.DataFrame`
			Accounting items.
		- "supplier_id": `str`
			Ledvance listing ID in the customer's accounting.
		- "remittance_number": `str`
			Document number of the remittance advice.
		- "remittance_date": `str`
			Document date.
		- "remittance_name": `str`, ""
			Not applicable for OBI.
		- "remittance_type": `str`, ""
			Not applicable for OBI.
	"""

	if len(accounting_map) == 0:
		raise ValueError("Branch map defined in 'branch_map' cannot be empty!")

	try:
		parsed = _parse_obi_de(text)
	except AssertionError as exc:
		raise ParsingError("Could not extract data from the document!") from exc

	items = parsed["items"]

	# if branch number is not stated, then the branch number is most likely 850
	missing_branch = items["Branch_Number"] == ""
	items.loc[missing_branch, "Branch_Number"] = OBI_GERMANY_HEAD_OFFICE_BUSINESS_UNIT

	items = items.assign(
		Case_ID = pd.NA,
		On_Account_Text = pd.NA,
		Tax_Code = pd.NA,
		Discount = items["Deduction"] / 5 * 3,
		Provision_Discount = items["Deduction"] / 5 * 2,
		Debitor = items["Branch_Number"].apply(
			lambda x: accounting_map[parsed["supplier_id"]][x]
		).astype("UInt64")
	)

	# determine tax symbols
	items.loc[items["Deduction"] == 0, "Tax_Code"] = VAT_CODE_GERMANY_NO_TAX_PROCEDURE
	items.loc[items["Deduction"] != 0, "Tax_Code"] = VAT_CODE_FRANCE_INTRACOMMUNITY
	tax_mask = items["Branch_Number"].isin([
		OBI_GERMANY_HEAD_OFFICE_BUSINESS_UNIT,
		OBI_GERMANY_BONUS_BUSINESS_UNIT])
	items.loc[tax_mask, "Tax_Code"] = "check"

	# determine document types
	document_type = items.apply(
		lambda x: _get_item_type(
			x["Gross_Amount"],
			x["Document_Number"],
			threshold
		), axis = 1
	)

	items = items.assign(Document_Type = document_type)

	# transport costs
	mask = items["Branch_Number"] == OBI_GERMANY_TRANSPORT_COSTS_BUSINESS_UNIT
	items.loc[mask, "Case_ID"] = "NA"
	items.loc[mask, "Document_Type"] = "Debit"
	items.loc[mask, "On_Account_Text"] = items.loc[mask, "Document_Number"] + " Fracht"

	# convert branch to numeric data type
	items["Branch_Number"] = pd.to_numeric(items["Branch_Number"]).astype("UInt16")

	# the branch ID field can now be converted to an integer dtype
	items["GL_Account"] = items["Document_Type"].apply(_get_gl_account)
	items["Gross_Amount_(ABS)"] = items["Gross_Amount"].abs()

	calc_net_amount = items["Net_Amount"].sum().round(2)
	calc_deductions_amount = items["Deduction"].sum().round(2)
	parsed_net_amount = parsed["total_net_amount"]
	parsed_deductions = parsed["total_deductions"]

	# check if calculated total amounts are in line
	# with the totals stated in the remittance advice
	if calc_net_amount != parsed_net_amount or calc_deductions_amount != parsed_deductions:
		raise ParsingError("Could not extract data from the document!")

	# sort the data values on desired accountig data fields,
	# then reorder the data on on the defined layout
	items.sort_values(["Document_Type", "Tax_Code", "Gross_Amount_(ABS)"], inplace = True)
	reordered = items.reindex(fields, axis = 1)
	parsed["remittance_date"] = parsed["remittance_date"].strftime(date_format)

	parsed.update({
		"items": reordered,
		"remittance_type": "",
		"remittance_name": "",
	})

	return parsed
