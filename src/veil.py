import csv
import dateparser
import uuid
import pickle
import random
from datetime import timedelta, datetime
import re

def _get_random_nonzero_int(bound:int) -> int:
    """
    """
    is_zero = True
    while is_zero:
        a = random.randint(-bound, bound)
        if a != 0:
            is_zero = False
    return a

class veil:

    def __init__(self, max_days=365):
        """
        The internal representation of an object of the veil class is a dictionary 
        with the following structure:

        {
            'id': {},
            'offset': {}
        }

        In addition, some compiled regular expressions are included so that the 
        logic is carried forward with imports
        """
        self.veil = {
            'id': {},
            'offset': {}
            }
        self.max_days = max_days
        self.YMD_HMS_RE = re.compile(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')
        self.YMD_RE = re.compile(r'\d{4}-\d{2}-\d{2}$')

    
    def __repr__(self):
        r = """veil object with following internal representation: 
        {} 
        max_days: {}
        """.format(self.veil, self.max_days)
        return r

    def add_id_map(self, name:str):
        """ 
        Instantiates a new id-based map
        """
        if name in self.veil['id']:
            raise(KeyError, 'conflicting keys in id veil')
        self.veil['id'].update({name:{}})

    # TODO: if more than 1 datetime base is needed
    # def add_offset_map(self, name:str):
    #     """
    #     Instantiates a new offset map
    #     """
    #     if name in self.veil['offset']:
    #         raise(KeyError, 'conflicting keys in offset veil')
    #     self.veil['offset'].update({name: {}})
        
    def update_id_map(self, name:str, id):
        """
        Updates values in the id_map using UUID
        """
        if str(id) not in self.veil['id'][name]:
            self.veil['id'][name].update({str(id): uuid.uuid4()})
        pass
    
    def update_offset_map(self, id):
        """
        Updates values in the offset_map 
        """
        if str(id) not in self.veil['offset']:
            self.veil['offset'].update(
                {str(id):_get_random_nonzero_int(self.max_days)})

    def _deidentify_id_columns(self, row, id_columns, \
        debug=False):
        """
        a row object is passed in from a csv.DictReader
        # TODO: What happens if the id_col is not instantiated in 
        # veil? Add automatically?
        """
        if isinstance(id_columns, str):
            id_columns = [id_columns] 
        for id_col in id_columns:
                    current_id_to_replace = row[id_col]
                    if current_id_to_replace not in self.veil['id'][id_col]:
                        self.update_id_map(id_col, current_id_to_replace)
                    try:
                        if debug:
                            print(row)
                            print(row[id_col])
                        row[id_col] = self.veil['id'][id_col][row[id_col]]
                    except KeyError:
                        row[id_col] = None
                    except TypeError:
                        row[id_col] = None
        return row
    
    def _deidentify_time_columns(self, row, time_columns, datetime_base_column,\
        debug=False, date_formats=None):
        """ 
        date_formats is a list that contains the date format
        to pass to datetime.strptime() for each element in 
        time_columns 
        """
        if isinstance(time_columns, str):
            time_columns = [time_columns] 
        current_id = row[datetime_base_column]
        # Update the offset row using datetime_base
        if current_id not in self.veil['offset']:
            self.update_offset_map(current_id)
        for i,col in enumerate(time_columns):
            if row[col] == '':
                row[col] == None
            else:
                try:
                    if debug:
                        print('row: {}'.format(row))
                        print(row[col])
                    if date_formats:
                        if not date_formats[i]:
                            row[col] = dateparser.parse(row[col], languages=['en'])\
                            + timedelta(days=self.veil['offset'][str(current_id)])
                        elif date_formats[i]:
                            row[col] = datetime.strptime(row[col], date_formats[i])\
                            + timedelta(days=self.veil['offset'][str(current_id)])
                    elif not date_formats:
                        row[col] = dateparser.parse(row[col], languages=['en'])\
                        + timedelta(days = self.veil['offset'][str(current_id)])
                except KeyError:
                    row[col] = None
                except TypeError:
                    row[col] = None # This usually occurs when dateparser is unable 
                                    # to correclty parse the datetime (e.g. no date)
        return row

    def deidentify(self, reader: csv.DictReader, writer: csv.DictWriter,
                    time_columns=None, datetime_base_column=None,
                    id_columns=None, update=True, 
                    mapping_dict=None, to_drop=None, 
                    debug=False, verbose=False):
        """
        Takes in a csv reader and applies the various maps to mask all 
        identifiers and also shift datetimes accordingly. 
        Args:
            time_columns (str or List): A list of columns that contain datetimes 
                to parse and convert
            datetime_base_column (str): The base column to shift datetimes 
                according to. For example, this can be the name of a key in
                self.veil['id']
            id_columns (str or List): The id column to mask. Names must match 
                self.veil['id'] unless mapping_dict is specified
            update (bool): Whether or not to update the maps as the files are
                deidentified

        # TODO: Implement mapping_dict
        # TODO: Implement update = None path
        """ 
        writer.writeheader()
        if time_columns:
            time_parsing_infer = {
                t:{
                    '%Y-%m-%d %H:%M:%S': 0,
                    '%Y-%m-%d': 0,
                } for t in time_columns
            }
        if not mapping_dict:
            for i,row in enumerate(reader):
                if i % 10000 == 0 and i != 0:
                    print('Deidentifying Row: {}'.format(i))
                if time_columns is not None:
                    if i < 10:
                        for time_col in time_columns:
                            if row != "":
                                if self.YMD_HMS_RE.match(row[time_col]):
                                    time_parsing_infer[time_col]['%Y-%m-%d %H:%M:%S'] += 1
                                elif self.YMD_RE.match(row[time_col]):
                                    time_parsing_infer[time_col]['%Y-%m-%d'] += 1
                        row = self._deidentify_time_columns(row, time_columns, \
                            datetime_base_column, debug=debug) 
                
                    # Check the inference to potentially speed things up
                    elif i == 10:
                        format_list = []
                        for time_col in time_columns:
                            max_fmt = max(time_parsing_infer[time_col].values())
                            if max_fmt > 5:
                                fmt = [x for x in time_parsing_infer[time_col] \
                                    if time_parsing_infer[time_col][x] == max_fmt]
                                format_list.append(fmt[0])
                            else:
                                format_list.append(None)
                            
                        row = self._deidentify_time_columns(row, time_columns, \
                        datetime_base_column, debug=debug) 
                        print(format_list) 
                    else:
                        row = self._deidentify_time_columns(row, time_columns, \
                            datetime_base_column, debug=debug,\
                                date_formats=format_list)

                if id_columns is not None:
                    row = self._deidentify_id_columns(row, id_columns,
                    debug=debug)
                if to_drop:
                    for drop in to_drop:

                        row.pop(drop)
                writer.writerow(row)
        pass

