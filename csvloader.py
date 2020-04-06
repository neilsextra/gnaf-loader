import csv
import logging
import os
import re
import tempfile
from psycopg2 import connect

class CsvLoader(object):
    """
    Automatically create tables and load data from CSV files to your database.
    This loader creates tables based on CSV headers. Then data is loaded using COPY command.
    It works only with PostgreSQL for now.
    """

    DEFAULT_DELIMITER = ','
    DEFAULT_QUOTE_CHAR = '"'
    DEFAULT_ESCAPE_CHAR = None
    DEFAULT_TABLE_PREFIX = "csv_"
    DEFAULT_DOUBLE_QUOTE = True
    DEFAULT_DATA_TYPE = "varchar"

    CREATE_STMT = "CREATE TABLE IF NOT EXISTS {} ({});"
    COPY_STMT = "COPY {} ({}) FROM stdin WITH CSV HEADER DELIMITER '{}' QUOTE '{}' ESCAPE '{}'"

    def __init__(self, connection_string): 
        """
        Constructs document with given database details.
        :param logger: the system logger 
        :param connection_string: database connection_string 
        """
        self._connection_string = connection_string

    def load_data(self, file_path, table_name=None, delimiter=DEFAULT_DELIMITER, quote_char=DEFAULT_QUOTE_CHAR,
                  escape_char=DEFAULT_ESCAPE_CHAR, create_table=True, encoding="utf-8", work_file_prefix="out_"):
        """
        Loads data from CSV file to the database.
        Table column names are based on CSV header and names are simplified:
        - all uppercase letters are replaced with underscore and lowercase letters.
        - special characters are replaced with underscore.
        Table name is specified based on CSV file name.
        :param file_path: path to a CSV file
        :param delimiter: a one-character string used to separate fields. It defaults to ','
        :param quote_char: a one-character string used to quote fields containing special characters,
        such as the delimiter or quotechar, or which contain new-line characters
        :param escape_char: a one-character string used by the writer to escape the delimiter
        :param create_table: if True, table will be created
        :param encoding file encoding
        """
        # doublequote=True by default
        # don't define escape char if it's the same as quote char
        escape_char = None if (escape_char == quote_char) else escape_char

        original_headers = self._read_headers(file_path, delimiter, quote_char, escape_char, encoding)

        headers = self._normalize_headers(original_headers)

        if table_name == None:
           table_name = self._generate_table_name(file_path)
           logging.getLogger('CsvLoader').info('Generated table name "{}"...'.format(table_name))

        logging.getLogger('CsvLoader').info('Connecting to database ...')

        connection = connect(self._connection_string)

        if create_table:
            logging.getLogger('CsvLoader').info('Creating table "{}"...'.format(table_name))
            self._create_table(connection, headers, table_name)
        
        logging.getLogger('CsvLoader').info('Correcting file "{}"...'.format(file_path))
        work_file = self._remove_invalid_characters(file_path, work_file_prefix)

        logging.getLogger('CsvLoader').info('Loading data to table "{}"...'.format(table_name))
        self._copy_from_csv(connection, work_file, file_path, table_name, headers, delimiter, quote_char, escape_char, encoding)

        logging.getLogger('CsvLoader').info('Finished loading to table "{}", closing connection.'.format(table_name))
        connection.close()

    def _read_headers(self, file_path, delimiter=DEFAULT_DELIMITER, quote_char=DEFAULT_QUOTE_CHAR,
                      escape_char=DEFAULT_ESCAPE_CHAR, encoding="utf-8"):
        """
        Reads CSV header and provides a list of columns.
        :param file_path: path to a CSV file
        :param delimiter: a one-character string used to separate fields. It defaults to ','
        :param quote_char: a one-character string used to quote fields containing special characters,
        such as the delimiter or quotechar, or which contain new-line characters
        :param escape_char: a one-character string used by the writer to escape the delimiter
        :param encoding file encoding
        :return: list of CSV columns
        """
        with open(file_path, "r", encoding=encoding) as csv_file:
            reader = csv.reader(csv_file, delimiter=delimiter, quotechar=quote_char, escapechar=escape_char)
            original_headers = next(reader)
        return original_headers

    def _remove_invalid_characters(self, file_path, work_file_prefix):
        """
        Reads the CSF file and checs/corrects the file for invalid characters
        :param file_path: path to the CSV file
        :param work_file: the tempory file
        :return: worf file name 
        """

        work_file = tempfile.mktemp(".csv", work_file_prefix)
 

        with open(file_path, "rb") as input:
           with open(work_file, "wb") as output:
               while True:

                   c = input.read(1)

                   if not c:
                       break;

                   elif c == b'\xc9':
                       logging.getLogger('CsvLoader').info("Found Byte - 0xc9 - correct to 'e'")
                       output.write(b'e')
                   elif c == b'\x00':
                       logging.getLogger('CsvLoader').info("Found Byte - 0x00 - ignored")
                   elif c > b'\xff':
                       logging.getLogger('CsvLoader').warn("Invalid Character: {0} - ignored".format(hex(ord(c))))
                   else:
                       output.write(c)

        return work_file

    def _normalize_headers(self, original_headers):
        """
        Simplifies column names:
        - all uppercase letters are replaced with underscore and lowercase letters.
        - special characters are replaced with underscore.
        :param original_headers: list of CSV columns
        :return: simplified headers
        """
        headers = [self._simplify_text(header) for header in original_headers]
        return headers

    def _generate_table_name(self, file_path):
        """
        Generates table name based on CSV file name.
        :param file_path: path to a CSV file
        :return: generated table name
        """
        base = os.path.splitext(os.path.basename(file_path))[0]
        return self._table_prefix + CsvLoader._simplify_text(base)

    def _create_table(self, connection, headers, table_name):
        """
        Creates database table.
        :param connection: open connection
        :param headers: a list of columns
        :param table_name: a table name
        """
        columns = ['"{}" {}'.format(column, self.DEFAULT_DATA_TYPE) for column in headers]
        columns_def = ",".join(columns)

        cursor = connection.cursor()
        cursor.execute(self.CREATE_STMT.format(table_name, columns_def))
        connection.commit()
        cursor.close()

    def _copy_from_csv(self, connection, work_file, file_path, table_name, headers, delimiter, quote_char, escape_char, encoding):
        """
        Copies data from CSV to database.
        :param connection: open connection
        :param file_path: path to a CSV file
        :param table_name: a table name
        :param headers: a list of columns
        :param delimiter: a one-character string used to separate fields. It defaults to ','
        :param quote_char: a one-character string used to quote fields containing special characters,
        such as the delimiter or quotechar, or which contain new-line characters
        :param escape_char: a one-character string used by the writer to escape the delimiter
        :param encoding file encoding
        """
        columns = ['"{}"'.format(column) for column in headers]
        columns_def = ",".join(columns)

        copy_from_escape_char = escape_char or quote_char  # use quote if escape is None
        command = self.COPY_STMT.format(table_name, columns_def, delimiter,
                                        quote_char, copy_from_escape_char)
        # https://www.postgresql.org/docs/current/static/sql-copy.html

        print("Started copy : {0} - '{1}' - [{2}] - '{3}'".format(table_name, file_path, delimiter, work_file))

        cursor = connection.cursor()
        with open(work_file, "r", encoding=encoding) as csv_file:
            cursor.copy_expert(command, csv_file)
            connection.commit()

        print("Completed copy : {0} - {1}".format(table_name, file_path))

    def _create_index(self, original_header):
        # TODO: implement indexing
        pass

    @staticmethod
    def _simplify_text(text):
        """
        Simplifies text:
        - all uppercase letters are replaced with underscore and lowercase letters.
        - special characters are replaced with underscore.
        :param text: text to simplify
        :return: simplified text
        """
        # replace <letter in uppercase> with <letter in lowercase prefixed by underscore>)
        # e.g. SimpleText -> _simple_text
        # replace all special characters with underscore
        unified = re.sub('[^0-9a-zA-Z]+', '_', text).lower()

        # remove underscore at the beginning
        if unified.startswith("_"):
            unified = unified[1:]
        return unified

