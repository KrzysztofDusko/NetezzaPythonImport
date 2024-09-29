import argparse
import datetime
import random
import sys
import win32pipe, win32file
import re
import csv
import time
# pipes from https://stackoverflow.com/questions/48542644/python-and-windows-named-pipes

class NetezzaDataType:
    def __init__(self, db_type:str, precision:int|None, scale:int|None, length:int|None):
        self.db_type = db_type
        self.PRECISION = precision
        self.SCALE = scale
        self.LENGTH = length
    def __str__(self) -> str:
        if self.db_type == 'BIGINT' or self.db_type == 'DATE' or self.db_type == 'DATETIME':
            return f"{self.db_type}"
        if self.db_type == 'NUMERIC':
            return f"{self.db_type}({self.PRECISION},{self.SCALE})"
        if self.db_type == 'NVARCHAR':
            return f"{self.db_type}({self.LENGTH})"

        return f"TODO !!! {self.db_type}"

class ColumnTypeChooser:
    def __init__(self):
        self.current_type = NetezzaDataType('BIGINT',None,None,None)
        self._decimal_delim_in_csv = '.'
        self._first_time = True

    def _get_type(self, str_val:str) -> NetezzaDataType:
        current_db_type = self.current_type.db_type
        str_len = len(str_val)
        if current_db_type == 'BIGINT' and str_val.isdigit() and str_len < 15:
            self._first_time = False
            return NetezzaDataType('BIGINT',None,None,None)
        
        decimal_cnt = str_val.count(self._decimal_delim_in_csv)
        if (current_db_type == 'BIGINT' or current_db_type == 'NUMERIC') and decimal_cnt <=1:
            str_val = str_val.replace(self._decimal_delim_in_csv,'')
            # 0005 -> '0005', but 0.005 = 0.005 (real number)
            if str_val.isdigit() and str_len < 15 and (not str_val.startswith('0') or decimal_cnt > 0):
                self._first_time = False
                return NetezzaDataType('NUMERIC',16,6,None)
            
        # 2024-06-07, 2024-6-7
        if (current_db_type == 'DATE' or self._first_time) and str_val.count('-') == 2 and str_len <= 10 and str_len >= 8:
            ind1 = str_val.index('-')
            ind2 = str_val.index('-',ind1+1)
            year_str = str_val[0:ind1]
            month_str = str_val[(ind1+1):ind2]
            day_str = str_val[(ind2+1):]
            if year_str.isdigit() and month_str.isdigit() and day_str.isdigit():
                try:
                    datetime.datetime(year=int(year_str),month=int(month_str),day=int(day_str))
                    self._first_time = False
                    return NetezzaDataType('DATE',None,None,None)
                except:
                    pass
        # 2024-06-07T21:15:12 / 2024-06-07 21:15:12 / 2024-06-07T21:15 / 2024-06-07 21:15
        if (current_db_type == 'DATETIME' or self._first_time) and str_val.count('-') == 2 and str_len <= 20 and str_len >= 12:
            result = re.search(r"^(\d{4})-(\d{1,2})-(\d{1,2})[\s|T](\d{2}):(\d{2})(:?(\d{2}))?$", str_val)
            if result is not None:
                try:
                    sec = 0
                    if len(result.groups()) == 7:
                        sec = int(result.group(7))

                    datetime.datetime(year=int(result.group(1)),month=int(result.group(2)),day=int(result.group(3)),hour=int(result.group(4)),
                                      minute=int(result.group(5)), second=sec)
                    self._first_time = False
                    return NetezzaDataType('DATETIME',None,None,None)
                except:
                    pass        

        tmp_len = str_len + 5
        if tmp_len < 20:
            tmp_len = 20
        if self.current_type.LENGTH is not None and tmp_len < self.current_type.LENGTH:
            tmp_len = self.current_type.LENGTH

        self._first_time = False
        return NetezzaDataType('NVARCHAR', 0,0,tmp_len)

    def refresh_current_type(self, str_val:str) -> NetezzaDataType:
        self.current_type = self._get_type(str_val)
        return self.current_type

class ImportClass:
    def __init__(self,file_path:str, log_dir:str, remotesource:str = 'dotnet', csv_dialect:str = "excel"):
        self._file_path = file_path
        self._DELIMITER = '\t'
        self._DELIMITER_PLAIN = r'\t'
        self._RECORD_DELIM = b'\n'
        self._RECORD_DELIM_PLAIN = r'\n'
        self._RECORD_DELIM_STR = '\n'
        self._ESCAPECHAR = '\\'
        self._REMOTESOURCE = remotesource
        self._LOGDIR = log_dir
        num = random.randint(0,1000)
        self._full_pipe_name = rf'\\.\pipe\JUSTY_BASE_{num}'

        self._sql_headers:list[str] = []
        self._values_to_escape = [self._ESCAPECHAR,self._RECORD_DELIM_STR,'\r',self._DELIMITER]
        self._csv_delimiter = ';'
        self._csv_dialect = csv_dialect
        self._rows_cnt = 0
        self._data_types = self.analyse_csv_data_types()

    def get_csv_rows(self):
        with open(self._file_path, newline='', encoding="utf-8") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=self._csv_delimiter, dialect=self._csv_dialect)
            i = 0 
            start = time.time()
            for row in csv_reader:
                if i==0:
                    pass
                else:
                    yield row
                i += 1
                if i%10_000 == 0:
                    end = time.time()
                    print(f"processed {i:_} / {self._rows_cnt:_} rows, {round(end-start,1)} seconds")
            
    def analyse_csv_data_types(self) -> list[ColumnTypeChooser]:
        with open(self._file_path, newline='', encoding="utf-8") as csvfile:
            line = csvfile.readline()
            if('|' in line):
                self._csv_delimiter = '|'
            if(';' in line):
                self._csv_delimiter = ';'
            if('\t' in line):
                self._csv_delimiter = '\t'
            if(',' in line):
                self._csv_delimiter = ','
            if line[0] == '\ufeff':
                csvfile.seek(3) # skip UTF-8 BOM if needed
            else:
                csvfile.seek(0) # do to start of file

            csv_reader = csv.reader(csvfile, delimiter=self._csv_delimiter, dialect=self._csv_dialect)
            i = 0 
            data_types = []
            start = time.time()
            for row in csv_reader:
                if i==0:
                    self._sql_headers = row
                    data_types = [ColumnTypeChooser() for _ in row]
                else:
                    j=0
                    for j in range(len(row)):
                        data_types[j].refresh_current_type(row[j])

                i+=1
                if i%10_000 == 0:
                    end = time.time()
                    print(f"processed {i:_} rows, {round(end-start,1)} seconds")
            self._rows_cnt = i - 1
            print(f"END -> processed {self._rows_cnt:_} rows")

        sql_headers_new = []
        for (a,b) in zip(data_types,self._sql_headers):
            col_name = self.fix_value(b)
            col_name = re.sub('[^0-9a-zA-Z]+','_',col_name).upper()
            sql_headers_new.append(f"{col_name} {a.current_type}")

        self._sql_headers = sql_headers_new
        return data_types

    def fix_value(self, val:object) -> str:
        result = str(val)
        result = result.strip()
        for v in self._values_to_escape:
            result = result.replace(v,f'{self._ESCAPECHAR}{v}')
        return result


    def fix_value_x(self, val:object,col_num:int) -> str:
        result = self.fix_value(val)
        if self._data_types[col_num].current_type.db_type == 'DATETIME':
            result = result.replace('T', ' ')
        return result

    # https://www.ibm.com/docs/en/netezza?topic=eto-option-summary
    def get_sql(self):
        num = random.randint(0,sys.maxsize)
        random_name = f"IMP_JUSTY_BASE_{num}"
        return rf"""
        CREATE TABLE {random_name} AS 
        (
            SELECT * FROM EXTERNAL '{self._full_pipe_name}'
            (
                {',\n'.join(self._sql_headers)}
            )
            USING
            (
                REMOTESOURCE '{self._REMOTESOURCE}'
                DELIMITER '{self._DELIMITER_PLAIN}'
                RecordDelim '{self._RECORD_DELIM_PLAIN}'
                ESCAPECHAR '{self._ESCAPECHAR}'
                NULLVALUE ''
                ENCODING 'utf-8'
                TIMESTYLE '24HOUR'
                SKIPROWS 0
                MAXERRORS 0
                LOGDIR '{self._LOGDIR}'
            )
        ) DISTRIBUTE ON RANDOM;
        --DROP TABLE {random_name};
        """
    def pipe_server(self):
        print("pipe server")

        pipe = win32pipe.CreateNamedPipe(
            self._full_pipe_name,
            win32pipe.PIPE_ACCESS_DUPLEX,
            win32pipe.PIPE_TYPE_MESSAGE | win32pipe.PIPE_READMODE_MESSAGE | win32pipe.PIPE_WAIT,
            1, 65536, 65536,
            0,
            None) # type: ignore
        try:
            print("waiting for client")
            win32pipe.ConnectNamedPipe(pipe, None)
            print("got client")
            for row in self.get_csv_rows():
                fixed_row = [self.fix_value_x(c,num) for (num,c) in enumerate(row)]
                str_data:str = self._DELIMITER.join(fixed_row)
                # convert to bytes
                byte_data = str.encode(str_data, encoding="utf-8", errors='ignore')
                win32file.WriteFile(pipe, byte_data)
                win32file.WriteFile(pipe, self._RECORD_DELIM)
        finally:
            win32file.CloseHandle(pipe)

def main():
    parser = argparse.ArgumentParser(
                    prog='nz_csv_pipe',
                    description='import data from csv to netezza database')
    parser.add_argument('filename')           # positional argument
    parser.add_argument('-l', '--log_dir',required=False, default=r'C:\log') 
    parser.add_argument('-d', '--driver',required=False, default='dotnet') 
    args = parser.parse_args()

    ic:ImportClass = ImportClass(args.filename, args.log_dir,args.driver)
    print(ic.get_sql())
    ic.pipe_server()
    print('FINISHED !!!')

if __name__ == '__main__':
    main();