# coding=utf8
import os
import csv
import datetime

try:
    from .record_struct import CLOSED_TRADE_FIELDS, CLOSED_TRADE_FIELDS_TYPES
except:
    from record_struct import CLOSED_TRADE_FIELDS, CLOSED_TRADE_FIELDS_TYPES


class PositionCsv:
    def __init__(self):
        self.rows = list()
        self.headers = CLOSED_TRADE_FIELDS

    # ------------------------------------------------------------------------------------------------------------------
    def clear(self):
        self.rows.clear()

    # ------------------------------------------------------------------------------------------------------------------
    def add_row(self, row):
        data_type = type(row)
        data = dict()
        if data_type is dict:
            for h in self.headers:
                if h in row:
                    data[h] = row[h]
                else:
                    data[h] = None
        elif data_type is list or data_type is tuple:
            if not len(row) == len(self.headers):
                return False
            for i, header in enumerate(self.headers):
                data[header] = row[i]
        else:
            return False
        #
        self.rows.append(data)
        return True

    # ------------------------------------------------------------------------------------------------------------------
    def get_rows(self):
        return self.rows

    # ------------------------------------------------------------------------------------------------------------------
    def save_to_file(self, filename):
        with open(filename, 'w') as fid:
            writer = csv.DictWriter(fid, self.headers, dialect=csv.unix_dialect)
            writer.writeheader()
            writer.writerows(self.rows)
        return

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def __convert_dt_from_str(s):
        if s.find(r'.') >= 0:
            fmt = r'%Y-%m-%d %H:%M:%S.%f'
        else:
            fmt = r'%Y-%m-%d %H:%M:%S'
        # try
        try:
            dt = datetime.datetime.strptime(s, fmt)
        except Exception as e:
            raise Exception('cant parse datetime str in position csv\n'
                            '%s\n%s\n' % (str(e), str(s)))
        return dt

    # ------------------------------------------------------------------------------------------------------------------
    def load_file(self, filename):
        if not os.path.exists(filename):
            return False
        self.clear()
        with open(filename) as fid:
            reader = csv.DictReader(fid, dialect=csv.unix_dialect)
            for r in reader:
                self.rows.append(r)
        # convert data types
        for row in self.rows:
            for h in row:
                content = row[h]
                if not content:
                    row[h] = None
                    continue
                # convert by type
                ty = CLOSED_TRADE_FIELDS_TYPES[h]
                if ty is datetime.datetime:
                    row[h] = self.__convert_dt_from_str(content)
                else:
                    row[h] = ty(content)
        return True


########################################################################################################################
def main():
    pc = PositionCsv()

    if 0:
        for i in range(3):
            d = dict()
            for h in CLOSED_TRADE_FIELDS:
                d[h] = i
            pc.add_row(d)

        pc.save_to_file('position_csv.csv')

    if 1:
        pc.load_file('position_csv.csv')
        data = pc.get_rows()
        for r in data:
            print(r)


########################################################################################################################
if __name__ == '__main__':
    main()
