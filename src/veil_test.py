import csv
import dateparser
import uuid
import pickle
import random
from datetime import timedelta


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
    """
    veil is the main class that provides methods to deidentify 
    datasets.

    The internal representation of an object of the veil class is a dictionary 
    with the following structure:

    {
        'id': {},
        'offset': {}
    }

    """
    def __init__(self, max_days=365):
        self.veil = {
            'id': {},
            'offset': {}
            }
        self.max_days = max_days
    
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

    def _deidentify_id_columns(self, row, id_columns):
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
                        row[id_col] = self.veil['id'][id_col][row[id_col]]
                    except KeyError:
                        row[id_col] = None
        return row
    
    def _deidentify_time_columns(self, row, time_columns, datetime_base_column):
        if isinstance(time_columns, str):
            time_columns = [time_columns] 
        current_id = row[datetime_base_column]
        # Update the offset row using datetime_base
        if current_id not in self.veil['offset']:
            self.update_offset_map(current_id)
        for col in time_columns:
            try:
                row[col] = dateparser.parse(row[col]) \
                + timedelta(days = self.veil['offset'][str(current_id)])
            except KeyError:
                row[col] = None
        return row

    def deidentify(self, reader: csv.DictReader, writer: csv.DictWriter,
                    time_columns=None, datetime_base_column=None,
                    id_columns=None, update=True, 
                    mapping_dict=None):
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
        if not mapping_dict:
            for row in reader:
                if time_columns is not None:
                    row = self._deidentify_time_columns(row, time_columns, datetime_base_column)
                if id_columns is not None:
                    row = self._deidentify_id_columns(row, id_columns)
                writer.writerow(row)
        pass

# class id_map:
#     """
#     An ID_map is a lookup table from an original ID to its faked counterpart
#     """

#     def __init__(self):
#         # The keys in self.map have keys representing the original values
#         # and a UUID to replace the values
#         self.map = {}
#         pass

#     def update_map(self, id):
#         if id not in self.map:
#             self.map.update({str(id):uuid.uuid4()})
#         pass
    
#     def deidentify(self, column_name:str, 
#         reader: csv.DictReader, writer: csv.DictWriter,
#         update = True):
#         """
#         Currently, this method only supports csv writing. 
#         In the future, this can most likely be refactored to include
#         other input and output methods. 
#         """
#         writer.writeheader()
#         for row in reader:
#             if row[column_name] not in self.map:
#                 self.update_map(row[column_name])
#             # Replace the row
#             row[column_name] = self.map[row[column_name]]
#             # Write
#             writer.writerow(row)
#         pass
    
#     def reidentify(self):
#         pass

#     def save(self):
#         pass
    
# # Offset Map -----------------------------------------------------------------
# class offset_map:
#     """
# 	"""
#     def __init__(self, method = "random", max_days = None):
#         self.map = {}
#         self.method = method
#         self.max_days = max_days
#         pass
    
#     def update_map(self, id):
#         if id not in self.map:
#             if self.method = "random":
#                 self.map.update(
#                     {str(id):_get_random_nonzero_int(self.max_days)})
#         pass
    
#     def apply_offset(self, time_columns, id_column:str, 
#                     reader: csv.DictReader, writer: csv.DictWriter,
#                     update=True):
#         """
#         :time_columns: str or List[str] that contains the columns
#         that need to be time shifted
#         :id_column: str the id column that needs to act as the base
#         """
#         writer.writeheader()
#         if isinstance(time_columns, str):
#             time_columns = [time_columns]
#         for row in reader:
#             if row[id_column] not in self.map:
#                 self.update_map(row[id_column])
#             for col in time_columns:
#                 row[col] = dateparser.parse(row[col] + timedelta(days = self.map[row[id_column]]))
            




if __name__ == '__main__':
    # with open('../tests/fixture_base.csv', 'r', encoding='utf-8-sig') as r, open('../tests/write_out.csv', 'w') as w:
    #         reader = csv.DictReader(r)
    #         writer = csv.DictWriter(w, reader.fieldnames)

    #         v = veil()
    #         v.add_id_map('original_ID')
    #         v.deiden
    with open('../tests/test_with_ts2.csv', 'r', encoding='utf-8-sig') as r, open('../tests/test2_results.csv', 'w') as w:
        reader = csv.DictReader(r)
        writer = csv.DictWriter(w, reader.fieldnames)

        v = veil()
        v.add_id_map('ID')
        v.deidentify(reader, writer, time_columns = ['Timestamp 1 ', 'Timestamp 2'], datetime_base_column = 'ID', id_columns = 'ID')