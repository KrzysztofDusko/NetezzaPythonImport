import win32pipe, win32file

# pipes from https://stackoverflow.com/questions/48542644/python-and-windows-named-pipes

class ImportClass:
    def __init__(self, pipe_name:str):
        self._DELIMITER = '\t'
        self._DELIMITER_PLAIN = r'\t'
        self._RECORD_DELIM = b'\n'
        self._RECORD_DELIM_PLAIN = r'\n'
        self._RECORD_DELIM_STR = '\n'
        self._ESCAPECHAR = '\\'
        self._REMOTESOURCE = 'dotnet'
        self._LOGDIR = r'c:\log'
        self._pipe_name = pipe_name
        self._full_pipe_name = rf'\\.\pipe\{self._pipe_name}'
        # simulate CSV file
        self.sample_data:list[list] = [
            [1,'a\nb'],
            [2,'b   cd'],
            [3,'cą\ręź'],
            [4,'1😀2😉3♞4'],
        ]
        self._sql_headers:list[str] = self._get_netezza_header_list()
        self._values_to_escape = [self._ESCAPECHAR,self._RECORD_DELIM_STR,'\r',self._DELIMITER]

    ### TODO - analyse real data
    def _get_netezza_header_list(self) -> list[str]:
        return ["ID INT, DATA NVARCHAR(64)"]

    ### TODO - read real data source
    def _get_sample_data(self):
        for row in self.sample_data:
            yield row

    def fix_value(self, val) -> str:
        result = str(val)
        for v in self._values_to_escape:
            result = result.replace(v,f'{self._ESCAPECHAR}{v}')
        return result

    # https://www.ibm.com/docs/en/netezza?topic=eto-option-summary
    def get_sql(self):
        return rf"""
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
        );
        """
    def pipe_server(self):
        print("pipe server")
        count = 0
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
            for row in self._get_sample_data():
                fixed_row = [self.fix_value(c) for c in row]
                str_data:str = self._DELIMITER.join(fixed_row)
                # convert to bytes
                byte_data = str.encode(str_data, encoding="utf-8", errors='ignore')
                win32file.WriteFile(pipe, byte_data)
                win32file.WriteFile(pipe, self._RECORD_DELIM)
                

            print("finished now")
            print("finished now")
        finally:
            win32file.CloseHandle(pipe)

if __name__ == '__main__':
    ic:ImportClass = ImportClass('Foo')
    print(ic.get_sql())
    ic.pipe_server()
    print(ic.get_sql())


