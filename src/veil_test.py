import csv
import dateparser
import uuid
import pickle
import random


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
    def __init__(self, max_days = 365):
        self.veil = {'id': {}, 'offset': {}}
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
            self.veil['offset'].update({str(id):_get_random_nonzero_int(self.max_days)})

    def deidentify(self, time_columns, id_columns):
        #TODO
        pass

class id_map:
    """
    An ID_map is a lookup table from an original ID to its faked counterpart
    """

    def __init__(self):
        # The keys in self.map have keys representing the original values
        # and a UUID to replace the values
        self.map = {}
        pass

    def update_map(self, id):
        if id not in self.map:
            self.map.update({str(id):uuid.uuid4()})
        pass
    
    def deidentify(self, column_name:str, 
        reader: csv.DictReader, writer: csv.DictWriter,
        update = True):
        """
        Currently, this method only supports csv writing. 
        In the future, this can most likely be refactored to include
        other input and output methods. 
        """
        writer.writeheader()
        for row in reader:
            if row[column_name] not in self.map:
                self.update_map(row[column_name])
            # Replace the row
            row[column_name] = self.map[row[column_name]]
            # Write
            writer.writerow(row)
        pass
    
    def reidentify(self):
        pass

    def save(self):
        pass
    
# Offset Map -----------------------------------------------------------------
class offset_map:
    """
	"""
    def __init__(self, method = "random", max_days = None):
        self.map = {}
        self.method = method
        self.max_days = max_days
        pass
    
    def update_map(self, id):
        if id not in self.map:
            if self.method = "random":
                self.map.update({str(id):_get_random_nonzero_int(self.max_days)})
        pass
    
    def apply_offset(self, time_columns, id_column:str, 
                    reader: csv.DictReader, writer: csv.DictWriter,
                    update=True):
        """
        :time_columns: str or List[str] that contains the columns
        that need to be time shifted
        :id_column: str the id column that needs to act as the base
        """
        writer.writeheader()
        if isinstance(time_columns, str):
            time_columns = [time_columns]
        for row in reader:
            if row[id_column] not in self.map:
                self.update_map(row[id_column])
            for col in time_columns:
                row[col] = dateparser.parse(row[col] + timedelta(days = self.map[row[id_column]]))
            




if __name__ == '__main__':
    with open('../tests/fixture_base.csv', 'r', encoding='utf-8-sig') as r, open('../tests/write_out.csv', 'w') as w:
            reader = csv.DictReader(r)
            writer = csv.DictWriter(w, reader.fieldnames)

            v = id_map()
            v.deidentify('original_ID', reader, writer)