CONFIG_FILE_NAME="FridayConfig.yaml"
STATIC_FOLDER_NAME="static"
TEMPLATE_FOLDER_NAME="templates"
LOG_FILE_NAME="Travis.log"
HOME_PAGE_TITLE = "TRAVIS Homepage"
TRAVIS1_TITLE = "TRAVIS Data Migration Studio"
TRAVIS2_TITLE = "TRAVIS Data Quality Studio"
COMPARE_REPORT_FILE="Compare_Report.html"
TAGGING_REPORT_FILE="Tagging_Report.html"
IMAGE_PREFIX="data:image/png;base64,"
SPECIAL_CHARACTERS="[@!#$%^*()<>?/\|}{~:]"
FILE_TYPES=[("All Files", "*.*"),
            ("Text Files", "*.txt"),
            ("SQL Files", "*.sql"),
            ("CSV Files", "*.csv"),
            ("json Files", "*.json"),
            ("Key Files", "*.pem")]
BASE_METADATA_DATABASE = "base_metadata.db"
RELEASE_METADATA_DATABASE = "release_metadata.db"
BASE_METADATA_TABLE = "BASE_METADATA_TABLE"
RELEASE_METADATA_TABLE = "RELEASE_METADATA_TABLE"
BASE_DATABASE = "base_data.db"
RELEASE_DATABASE = "release_data.db"
BASE_TABLE = "BASE_TABLE"
RELEASE_TABLE = "RELEASE_TABLE"
BASE = "Base"
RELEASE = "Release"
JSON = "JSON"
CSV = "CSV"
MISMATCH = "Mismatch"
MATCH = "Match"
UNMATCH_FILE_NAME = "Unmatch_File.csv"
MATCH_FILE_NAME = "Match_File.csv"
OUT_OF_SEQ_FILE_NAME = "Out_Of_Sequence.csv"
EXCEPTION_FILE_NAME = "Exception_File.log"
MERGED_FILE_NAME = "Merged.csv"



MESSAGE_LOOKUP = {
    1: "%s cannot be spaces",
    2: "ERROR - %s cannot have special characters",
    3: "CRITICAL ERROR - Please check the %s Log File",
    4: "CRITICAL ERROR - Please provide %s file",
    5: "CRITICAL ERROR - Invalid %s. Please check with %s",
    6: "CRITICAL ERROR - Expired %s. Please check with %s",
    7: "WARNING - Looks like %s selected is not available at this moment. Please contact %s support team.",
    8: "Invalid Configuration Settings",
    9: "First option cannot be spaces",
    10: "Sub option cannot be spaces",
    11: "%s Files must be a list",
    12: "Corrupted Request for %s. Please correct the configurations",
    13: "Values of %s cannot be negative or zero",
    14: "%s is Complete. Output in %s",
    15: "Unbalanced Number of Files in base and releasee. Use %s Dynamic Compare",
    16: "Cannot Initialize %s for TRAVIS. Please check with %s",
}