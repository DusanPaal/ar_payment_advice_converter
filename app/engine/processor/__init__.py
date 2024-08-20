"""
The module parses payment (remittance) advices issued by the company.
Current version supports parsing of German version of the documents issued by . Payment
advices issued by Autrian branch should work as well, but have not been tested.
"""

import shlex
from os.path import basename, exists, isfile, join
from subprocess import PIPE, Popen, TimeoutExpired
import pandas as pd
from pandas import Series

FilePath = str
DirPath = str

CUSTOMER_MARKANT_DE = "MARKANT_DE"
CUSTOMER_OBI_DE = "OBI_DE"

class UnrecognizedCustomerError(Exception):
	"""Raised when the program fails to identify
	the customer who issued a payment advice"""

class FolderNotFoundError(Exception):
	"""Raised when a directory is requested	but doesn't exist."""

class PdfConversionError(Exception):
	"""Raised when file conversion fails
	for one of the following reasons:

		- file conversion terminates as a result of an error
		- file conversion passes but no output file is created
	"""

class ParsingError(Exception):
	"""Raised when the data parser encounters
	an invalid value during text parsing.
	"""


def parse_amount(val: str) -> float:
	"""Parses SAP amount string.

	Parameters:
	-----------
	val: Value to parse

	Returns:
	---------
	The parsed value in the `floa`t data type.
	"""

	parsed = val.replace(".", "").replace(",", ".")

	if parsed.endswith("-"):
		parsed = "".join(["-" , parsed.replace("-", "")])

	return float(parsed)

def parse_amounts(vals: Series) -> Series:
	"""Parses SAP amount strings.

	Parameters:
	-----------
	vals:
		String values to parse stored in a `pandas.Series` object.

	Returns:
	-----------
	Parsed values converted to `float64` data 
	type, stored in a `pandas.Series` object.
	"""

	if vals.empty:
		raise ValueError("The amount field contains no values!")

	repl = vals.str.replace(".", "", regex = False)
	repl = repl.str.replace(",", ".", regex = False)
	repl = repl.mask(repl.str.endswith("-"), "-" + repl.str.rstrip("-"))
	conv = pd.to_numeric(repl).astype("float64")

	return conv

def identify_customer(pdf: FilePath, dst: DirPath, extractor: FilePath) -> str:
	"""Identifies the issuer of a PDF remittance advice from the document text.

	If the procedure fails to identify the issuer,
	then an `UnrecognizedCustomerError` exception is raised

	If the text exraction from the PDF fails, 
	then an `PdfConversionError` exception is raised.

	Parameters:
	----------
	pdf:
		Path to a PDF document that represents the payment advice.

		If the PDF is not found at the specified path,
		then a `FileNotFoundError` exception is raised.

	dst:
		Path to the folder to store the output text file.

		If the folder is not found at the specified path,
		then a `FolderNotFoundError` exception is raised.

	extractor:
		Path to the executable (.exe) file that extracts text from a PDF.

		If the extractor is not found at the specified path,
		then a `FileNotFoundError` exception is raised.

		If an invalid extractor file format is used,
		then a `ValueError` exception is raised.

	Returns:
	--------
	Name and country code of the customer who issued the document:
		'OBI_DE': if the issuer is OBI Germany
		'MARKANT_DE': if the issuer is Markant Germany
	"""

	content = extract_text(pdf, dst, extractor,	options = "-raw -enc UTF-8")

	if "Markant " in content or "MARKANT " in content:
		cust_name = CUSTOMER_MARKANT_DE
	elif " OBI " in content:
		cust_name = CUSTOMER_OBI_DE
	else:
		raise UnrecognizedCustomerError(
			"Could not detect the customer name form "
			f"the document data: {basename(pdf)}! "
			"The document may not be a payment advice.")

	return cust_name

def extract_text(pdf: FilePath, dst: DirPath, extractor: FilePath, options: str) -> str:
	"""Extracts text from a PDF document.

	Refer to the official PdfToText project 
	documentation on how to use the PDF extractor.

	If the text exraction from the PDF fails,
	then an `PdfConversionError` exception is raised.

	Parmeters:
	----------
	pdf:
		Path to a PDF document that represents the payment advice.

		If the PDF is not found at the specified path,
		then a `FileNotFoundError` exception is raised.

	dst:
		Path to the folder to store the output text file.

		If the folder is not found at the specified path,
		then a `FolderNotFoundError` exception is raised.

	extractor:
		Path to the executable (.exe) file that extracts text from a PDF.

		If the extractor is not found at the specified path,
		then a `FileNotFoundError` exception is raised.

		If an invalid extractor file format is used,
		then a `ValueError` exception is raised.

	options:
		Conversion options passed to the PDF extractor.

	Returns:
	--------
	Text strings extracted fro the pdf document.
	If the document is a scanned pdf, then an empty string will be returned.
	"""

	if not isfile(pdf):
		raise FileNotFoundError(f"Pdf file not found: '{pdf}'")

	if not exists(dst):
		raise FolderNotFoundError(f"Conversion output folder not found: '{dst}'")

	if not isfile(extractor):
		raise FileNotFoundError(f"Extractor not found: '{extractor}'")

	if not extractor.endswith(".exe"):
		raise ValueError(f"Invalid extractor not an executable: '{extractor}'")

	txt_name = basename(pdf)
	txt_name = txt_name.replace(".pdf", ".txt").replace(".PDF", ".txt")
	txt_path = join(dst, txt_name)
	cmd_line = " ".join([extractor, options, pdf, txt_path])
	args = shlex.split(cmd_line, posix = False) # compile converter console args
	timeout_secs = 30

	try:
		with Popen(
			args, stdout = PIPE, stdin = PIPE,
			stderr = PIPE, text = True
		) as conv:
			conv.wait(timeout_secs)
			return_code = conv.returncode
	except TimeoutExpired as exc:
		if isfile(txt_path):
			with open(txt_path, encoding = "UTF-8-SIG") as stream:
				content = stream.read()
			return content
		raise TimeoutError from exc

	if return_code != 0:
		raise PdfConversionError(f"File conversion failed with return code: {return_code}")

	# better raise a separate exception with error message for clarity
	if not isfile(txt_path):
		raise PdfConversionError(
			"The conversion process returned with no error, "
			f"however the output file was not found: '{txt_path}'"
		)

	with open(txt_path, encoding = "UTF-8-SIG") as stream:
		content = stream.read()

	return content
