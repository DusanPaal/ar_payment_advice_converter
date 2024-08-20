# pylint: disable = C0301, W1203

"""
The module parses payment (remittance) advices
issued by Markant company. Current version
supports parsing of german version of the
document only.
"""

import logging
import re
from enum import Enum
from re import Match

import pandas as pd
from pandas import DataFrame
from . import ParsingError, parse_amount, parse_amounts

log = logging.getLogger("master")

VAT_CODE_FRANCE_INTRACOMMUNITY = "C3"
VAT_CODE_FRANCE_OUTPUT_20_PERCENT = "C6"
VAT_CODE_GERMANY_DOMESTIC_19_PERCENT = "AB"
VAT_CODE_GERMANY_DOMESTIC_16_PERCENT = "AA"

class Journals(Enum):
	"""Markant payment advice types."""

	JOURNAL_10 = "invoicing"            # Rechnungen/Gutschrifte
	JOURNAL_20 = "other"                # Belastungen/Ruckbelastungen
	JOURNAL_22 = "services"       # sonstige leistungen
	JOURNAL_30 = "corrections"          # Korrekturen
	UNDEFINED = ""


def _parse_markant_de(text: str) -> DataFrame:
	"""Parses converted document text for Markant DE.
	Returns a dict of parsed data as well as other relevant
	accouning information about the document.
	"""

	remit_type = _get_remittance_type(text)

	match = re.search(r"Nummer :\s+(?P<RemitNum>\d{8}) ", text, re.M)
	assert match, "Remittance advice number not found!"
	remit_num = match.group("RemitNum")

	match = re.search(r"Datum\s+:\s+(?P<RemitDate>\S.*)", text, re.M|re.X)
	assert match, "Remittance advice date not found!"
	str_remit_date = match.group("RemitDate").replace(" ", "")
	remit_date = pd.to_datetime(str_remit_date, dayfirst = True).date()

	match = re.search(r"KTO: (?P<Supplier>\d{8})", text, re.M)
	assert match, "Supplier number not found!"
	supplier_id = match.group("Supplier")

	summ_line = re.search(r"Gesamtsumme\s+auf\s+Journal.*", text).group() # get summary amounts line
	patt = re.compile(r"(\S+,\S+)")

	gross_amount = patt.search(summ_line, 53, 67)
	markant_gross = patt.search(summ_line, 82, 94)
	customer_gross = patt.search(summ_line, 109, 120)
	discount = patt.search(summ_line, 136, 147)
	dl_condition = patt.search(summ_line, 160, 171)
	net_amount = patt.search(summ_line, 182, 195)

	tot_gross_amount = 0.0 if gross_amount is None else parse_amount(gross_amount.group())
	tot_markant_gross = 0.0 if markant_gross is None else parse_amount(markant_gross.group())
	tot_customer_gross = 0.0 if customer_gross is None else parse_amount(customer_gross.group())
	tot_discount = 0.0 if discount is None else parse_amount(discount.group())
	tot_dl_condition = 0.0 if dl_condition is None else parse_amount(dl_condition.group())
	tot_net_amount = 0.0 if net_amount is None else parse_amount(net_amount.group())

	# must be performed after extracting header data !!!
	cleaned = _clean_markant_text(text, remit_type)

	# odtiallto specifikovat processing pre kazdy typ zvlast
	if remit_type == Journals.JOURNAL_10:

		segmentation_rx = r"GLN-RA.*?EUR"

		bas_seg_rx = r"""GLN-LA:\s+
			(?P<iln>\d+).*\n+\s+
			(?P<doc>\S+)\s+
			(?P<dat>\d+\S+)\s+
			(?P<typ>\S+)\s+
			(?P<arch>\d+).*\n+
			(?P<amt>(\s+[0-9,-.]*)*)
		"""

	elif remit_type == Journals.JOURNAL_20:

		segmentation_rx = r"^(\s+GLN-RA.*?)(?=\n\s+GLN-RA|\Z)"

		bas_seg_rx = r"""GLN-LA:\s+
			(?P<iln>\d+).*?
			(?P<org>\S+)\n\s+
			(?P<doc>\S*?)\s+
			(?P<dat>\d+\S+)\s+
			(?P<typ>\S+)\s+
			(?P<arch>\d+).*\n
			(?P<amt>(\s+[0-9,-.]*)*)
			"""

		alt_seg_rx = r"""GLN-LA:\s+
			(?P<iln>\d+).*?
			(?P<org>\S+)\n\s+
			(?P<doc>\S*?)\s+
			(?P<dat>\d+\S+)\s+
			(?P<typ>\S+)\s+
			(?P<arch>\d+).*\n
			(?P<amt>(\s+[0-9,-.]*)*)EUR\n
			(?P<amt2>(\s+[0-9,-.]*)*)
			"""

	elif remit_type == Journals.JOURNAL_30:
		segmentation_rx = r"GLN-RA.*?EUR"

	data_segments = re.findall(segmentation_rx, cleaned, re.M|re.S)
	assert data_segments is not None, "Failed to identify items in the document!"

	# sort segments to one and more than one item segments
	bas_segments = []
	alt_segments = []

	for seg in data_segments:

		assert len(re.findall("EUR", seg)) != 0, "Parsing failed!"

		if len(re.findall("EUR", seg)) == 1:
			bas_segments.append(seg)
		else:
			alt_segments.append(seg)

	# data container
	items = DataFrame(
		columns = [
			"ILN",
			"Document_Number",
			"Document_Date",
			"Document_Type",
			"Archive_Number",
			"Gross_Amount",
			"Markant_SB_Condition",
			"Customer_SB_Condition",
			"DL_Condition",
			"Tax_Rate",
			"Net_Amount"
	])

	for seg in bas_segments:

		matches = re.search(bas_seg_rx, seg, re.M|re.X)
		assert matches is not None, "Parsing failed!"
		amnts = _extract_amounts(matches.group("amt"))

		if amnts is None:
			log.error("Failed to extract amount(s) for a document item!")

		record = DataFrame({
			"ILN": matches.group("iln"),
			"Document_Number": matches.group("doc"),
			"Original_Document": _get_original_document(matches, remit_type),
			"Document_Date": matches.group("dat"),
			"Document_Type": matches.group("typ"),
			"Archive_Number": matches.group("arch"),
			"Gross_Amount": amnts["gross_amount"],
			"Tax_Rate": amnts["tax_rate"],
			"Markant_SB_Condition": amnts["markant_amount"],
			"Customer_SB_Condition": amnts["customer_amount"],
			"Discount": amnts["discount"],
			"DL_Condition": amnts["dl_condition"],
			"Net_Amount": amnts["net_amount"]
		}, index = [0])

		items = pd.concat([items, record], ignore_index = True)

	if remit_type == Journals.JOURNAL_10 and len(alt_segments) != 0:
		assert False, "There should be no multiline items in this type of remittance advice (journal)!"

	for seg in alt_segments:

		matches = re.search(alt_seg_rx, seg, re.M|re.X)
		assert matches is not None, "Parsing failed!"

		line_1_amnts = _extract_amounts(matches.group("amt"))
		line_2_amnts = _extract_amounts(matches.group("amt2"))

		if line_1_amnts is None or line_2_amnts is None:
			log.error("Failed to extract amount(s) for a document item!")

		items = pd.concat([items, DataFrame({
			"ILN": matches.group("iln"),
			"Document_Number": matches.group("doc"),
			"Original_Document": matches.group("org"),
			"Document_Date": matches.group("dat"),
			"Document_Type": matches.group("typ"),
			"Archive_Number": matches.group("arch"),
			"Gross_Amount": line_1_amnts["gross_amount"],
			"Tax_Rate": line_1_amnts["tax_rate"],
			"Markant_SB_Condition": line_1_amnts["markant_amount"],
			"Customer_SB_Condition": line_1_amnts["customer_amount"],
			"Discount": line_1_amnts["discount"],
			"DL_Condition": line_1_amnts["dl_condition"],
			"Net_Amount": line_1_amnts["net_amount"]
		}, index = [0])], ignore_index = True)

		items = pd.concat([items, DataFrame({
			"ILN": matches.group("iln"),
			"Document_Number": matches.group("doc"),
			"Original_Document": matches.group("org"),
			"Document_Date": matches.group("dat"),
			"Document_Type": matches.group("typ"),
			"Archive_Number": matches.group("arch"),
			"Gross_Amount": line_2_amnts["gross_amount"],
			"Tax_Rate": line_2_amnts["tax_rate"],
			"Markant_SB_Condition": line_2_amnts["markant_amount"],
			"Customer_SB_Condition": line_2_amnts["customer_amount"],
			"Discount": line_2_amnts["discount"],
			"DL_Condition": line_2_amnts["dl_condition"],
			"Net_Amount": line_2_amnts["net_amount"]
		}, index = [0])], ignore_index = True)

	items["ILN"] = pd.to_numeric(items["ILN"]).astype("UInt64")
	items["Document_Number"] = items["Document_Number"].astype("string")
	items["Original_Document"] = items["Original_Document"].astype("string")
	items["Document_Date"] = pd.to_datetime(items["Document_Date"], dayfirst = True).dt.date
	items["Document_Type"] = items["Document_Type"].astype("category")
	items["Archive_Number"] = pd.to_numeric(items["Archive_Number"]).astype("UInt64")
	items["Gross_Amount"] = parse_amounts(items["Gross_Amount"])
	items["Tax_Rate"] = parse_amounts(items["Tax_Rate"])
	items["Markant_SB_Condition"] = parse_amounts(items["Markant_SB_Condition"])
	items["Customer_SB_Condition"] = parse_amounts(items["Customer_SB_Condition"])
	items["Discount"] = parse_amounts(items["Discount"])
	items["DL_Condition"] = parse_amounts(items["DL_Condition"])
	items["Net_Amount"] = parse_amounts(items["Net_Amount"])

	if remit_type == Journals.JOURNAL_10:
		items["Document_Number"] = pd.to_numeric(items["Document_Number"]).astype("UInt64")

	# validate data
	assert tot_gross_amount == items["Gross_Amount"].sum().round(2)
	assert tot_markant_gross == items["Markant_SB_Condition"].sum().round(2)
	assert tot_customer_gross == items["Customer_SB_Condition"].sum().round(2)
	assert tot_discount == items["Discount"].sum().round(2)
	assert tot_dl_condition == items["DL_Condition"].sum().round(2)
	assert tot_net_amount == items["Net_Amount"].sum().round(2)

	if remit_type == Journals.JOURNAL_10:
		name = "Rechnungen"
	elif remit_type == Journals.JOURNAL_20:
		name = "Belastungen"
	elif remit_type == Journals.UNDEFINED:
		name = ""
	else:
		name = ""

	output = {
		"items": items,
		"remittance_type": remit_type,
		"remittance_name": name,
		"remittance_num": remit_num,
		"remittance_date": remit_date,
		"supplier_id": supplier_id,
		"total_gross_amount": tot_gross_amount,
		"total_stock_markant_amount": tot_markant_gross,
		"total_stock_ledvance_amount": tot_customer_gross,
		"total_discount_amount": tot_discount,
		"total_dl_amount": tot_dl_condition,
		"total_net_amount": tot_net_amount
	}

	return output

def _clean_markant_text(text: str, journal_type: str) -> str:
	"""Removes irrelevant strings from remittance text."""

	if text == "":
		raise ValueError("Cannot parse an empty string!")

	if journal_type == Journals.JOURNAL_10:
		subst_a, _ = re.subn(r"^\s+Summenwerte.*\n", "", text, flags = re.M)
		subst_b, _ = re.subn(r"^\s+Seite.*?brutto$", "", subst_a, flags = re.M|re.S)
		subst_c, _ = re.subn(r"^\s+Seite.*?ReLi.*?\n", "", subst_b, flags = re.M|re.S)
		subst_d, _ = re.subn(r"^\s+Gesamtsumme.*", "", subst_c, flags = re.M|re.S)
		subst_e, _ = re.subn(r"^\s+ReLi.*", "", subst_d, flags = re.M)
		subst_f, _ = re.subn(r"^\s+GuLi.*", "", subst_e, flags = re.M)
		result, _ = re.subn(r"\n{3,}", "\n", subst_f, flags = re.M)
	elif journal_type == Journals.JOURNAL_20:
		subst_a, _ = re.subn(r"^\s+Summenwerte.*", "", text, flags = re.M|re.S)
		subst_b, _ = re.subn(r"^\s+Seite.*?brutto$", "", subst_a, flags = re.M|re.S)
		result, _ = re.subn(r"\n{2,}", "\n", subst_b, flags = re.M|re.S)

	return result

def _extract_amounts(line: str) -> dict:
	"""Extracts amounts from a given item line \n
	based on their pre-defined positions \n
	and returns the amounts as a dict of strings.
	"""

	gross_amnt = None
	tax_rate = None
	markant_amnt = "0.00"
	customer_amnt = "0.00"
	discount_amnt = "0.00"
	dl_condition_amnt = "0.00"
	net_amnt = None

	amounts = re.finditer(r"\S+[,]\S+", line)

	for mtch in amounts:

		val = mtch.group(0)

		if mtch.end() <= 69:
			gross_amnt = val
		elif mtch.end() >= 77 and mtch.end() <= 78:
			tax_rate = val
		elif mtch.end() >= 94 and mtch.end() <= 96:
			markant_amnt = val
		elif mtch.end() >= 121 and mtch.end() <= 123:
			customer_amnt = val
		elif mtch.end() >= 148 and mtch.end() <= 150:
			discount_amnt = val
		elif mtch.end() >= 172 and mtch.end() <= 174:
			dl_condition_amnt = val
		elif mtch.end() >= 196 and mtch.end() <= 197:
			net_amnt = val
		else:
			return None

	if gross_amnt is None or tax_rate is None or net_amnt is None:
		return None

	vals = {
		"gross_amount": gross_amnt,
		"tax_rate": tax_rate,
		"markant_amount": markant_amnt,
		"customer_amount": customer_amnt,
		"discount": discount_amnt,
		"dl_condition": dl_condition_amnt,
		"net_amount": net_amnt

	}

	return vals

def _get_original_document(matches: Match, remit_type: Journals) -> str:
	"""Returns the number of the original document \n
	from 'Ursprungsbeleg' text matched by regex.
	"""

	if remit_type != Journals.JOURNAL_20:
		return ""

	txt = matches.group("org")
	num = txt[txt.find("/") + 1:]

	return num

def _get_remittance_type(text: str) -> Journals:
	"""Returns the type of a remittance advice."""

	if "Rechnungen/Gutschriften" in text:
		remit_type = Journals.JOURNAL_10
	elif "Belastungen/Rückbelast" in text:
		remit_type = Journals.JOURNAL_20
	elif "Korrekturen" in text:
		remit_type = Journals.JOURNAL_30
	elif "sonstige Leistungen" in text:
		remit_type = Journals.JOURNAL_22
	else:
		assert False, "Failed to detect journal type!"

	return remit_type

def _get_tax_code(tax_rate: float) -> str:
	"""Returns a tax code that correcponds
	to the tax rate provided as percent
	(e.g. 20.0). An empty string will be
	returned if the used tax rate cannot
	be assigned to any tax code available.
	"""

	# leave the mapping in a separate function since
	# remittance processing might get extended by
	# countries other than Germany in the future
	tax_code = ""

	if tax_rate == 20.0:
		tax_code = VAT_CODE_FRANCE_OUTPUT_20_PERCENT
	elif tax_rate == 19.0:
		tax_code = VAT_CODE_GERMANY_DOMESTIC_19_PERCENT
	elif tax_rate == 16.0:
		tax_code = VAT_CODE_GERMANY_DOMESTIC_16_PERCENT
	elif tax_rate == 0.0:
		tax_code = VAT_CODE_FRANCE_INTRACOMMUNITY
	else:
		log.error(f"Could not assign a tax code to the tax rate: {tax_rate}!")

	return tax_code

def parse(text: str, accounting_map: dict, threshold: float, fields: list, date_format: str) -> dict:
	"""Parses the text extracted  from a PDF payment advice.

	If parsing of the text fails, then a `ParsingError` exception is raised.

	Parameters:
	-----------
	text:
		Plain text extracted from a remittance advice pdf.

	accounting_map:
		A dict that maps ILN numbers to SAP customer accounts.

	threshold:
		The amount limit below which items are written off.

	fields:
		A list of field names that define order of columns and in the processed data.

	date_format:
		String that dicates the resulting date format
		for converrsion of the document date to text.

	Returns:
	--------
	Accounting parameters and their values:
	- "items": `pandas.DataFrame`
		Accounting items.
	- "supplier_id": `str`
		Not applicable for Markant.
	- "remittance_number": `str`
		Document number of the remittance advice.
	- "remittance_date": `str`
		Document date.
	- "remittance_name": `str`, ""
		Name of the remittance advice in local 
		language as stated on the document.
	- "remittance_type": `str`
		Type pf the remittance advice:
		- "invoicing": Journal 10 - Rechnungen/Gutschrifte
		- "other": Journal 20 - Belastungen/Ruckbelastungen
		- "services": Journal 22 - Sonstige Leistungen
		- "corrections": Journal 30 - Korrekturen
		- "": Unrecognized payment advice type.
	"""

	if len(accounting_map) == 0:
		raise ValueError("Branch map defined in 'branch_map' cannot be empty!")

	try:
		parsed = _parse_markant_de(text)
	except AssertionError as exc:
		raise ParsingError("Could not extract data from the document!") from exc

	items = parsed["items"]

	conditions_fields = (
		"Markant_SB_Condition",
		"Customer_SB_Condition",
		"Discount",
		"DL_Condition"
	)

	for cond_fld in conditions_fields:
		items[cond_fld] = -1 * items[cond_fld]

	iln_to_debitor = DataFrame(
		accounting_map.values(),
		columns = ["Debitor"],
		index = accounting_map.keys()
	)

	iln_to_debitor.index.rename("ILN", inplace = True)
	iln_to_debitor.index = pd.to_numeric(iln_to_debitor.index).astype("UInt64")

	items = items.join(iln_to_debitor, how = "left", on = "ILN")

	items = items.assign(
		Case_ID = pd.NA,
		Search_Key = "*" + items["Archive_Number"].astype("string") + "*",
		Tax_Code = items["Tax_Rate"].apply(_get_tax_code)
	)

	if parsed["remittance_type"] == Journals.JOURNAL_10:
		items = items.assign(
			Overpayment = items["Document_Number"].duplicated(keep = "first"),
			Search_Key = "*" + items["Document_Number"].astype("string") + "*"
		)
	elif parsed["remittance_type"] == Journals.JOURNAL_20:
		items = items.assign(
			On_Account_Text = pd.NA
		)

	doc_types = {
		"RG": "Invoice",
		"Bela": "Debit",
		"RbelD": "Credit",
		"WKZ-B": "Debit",
		"WKZ-G": "Credit",
		"RetBe": "Debit",
		"RetRb": "Credit"
	}

	items["Document_Type"] = items["Document_Type"].apply(lambda x: doc_types[x] if x in doc_types else x)

	if parsed["remittance_type"] == Journals.JOURNAL_20:
		write_off = items.query(f"Document_Type == 'Debit' and Gross_Amount > -{threshold} and Gross_Amount < 0")
		items.loc[write_off.index, "Document_Type"] = "WriteOff"

	layout = fields.copy()

	if parsed["remittance_type"] == Journals.JOURNAL_10:
		layout.remove("Gross_Amount_(ABS)")
		layout.remove("On_Account_Text")

	items["Gross_Amount_(ABS)"] = items["Gross_Amount"].abs()

	# data type conversion
	items["ILN"] = items["ILN"].astype("UInt64")
	items["Debitor"] = items["Debitor"].astype("UInt32")

	# sort the data values on significant accountig data fields, then reorder the data on on the defined layout
	items.sort_values(["Document_Type", "Tax_Code", "Gross_Amount_(ABS)"], inplace = True)
	reordered = items.reindex(layout, axis = 1)
	parsed["remittance_date"] = parsed["remittance_date"].strftime(date_format)

	output = {
		"items": reordered,
		"remittance_number": parsed["remittance_num"],
		"remittance_date": parsed["remittance_date"],
		"remittance_type": parsed["remittance_type"],
		"remittance_name": parsed["remittance_name"],
		"supplier_id": ""
	}

	return output
