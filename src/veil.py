import pandas as pd
import numpy as np
import warnings

class id_veil:
	"""
	The following class instantiates objects which are associated with an ID 
	that must be replaced in the deidentification process. The methods can 
	be applied to DataFrames in order to apply the associated deidentification.
	In addition, the lookup tables generated can be exported for saving in 
	the case that re-identification needs to be performed

	Internally, objects of this class store the reference table as a dictionary
	for easy and fast reference.
	"""

	def __init__(self, dataframe, column_name = None, from_reference = False, key_col = None, val_col = None):
		if from_reference:
			"""
			This method creates the reference table from a key value pair in a 
			pandas DataFrame. This can be used to re-instantiate a reference table
			for re-use in a downstream script -- for example when trying to append new
			data that uses the same deidentification method
			"""
			assert key_col is not None, 'if using from_reference, must specify a key_col'
			assert val_col is not None, 'if using val_col, must specify a val_col'

			assert key_col in dataframe.columns, 'key_col must be in the dataframe column set'
			assert val_col in dataframe.columns, 'val_col must be in the dataframe column set'

			self.reference_table = dict(zip(dataframe[key_col], dataframe[val_col]))
			self.key_name = key_col
			self.val_name = val_col

		elif not from_reference:
			"""
			This method of instantiating a reference table uses a DataFrame of data
			to start the reference table creation. The upper bound on random sampling 
			is hard-coded in this case to 1 x 10^9, in the assumption that we will never
			have more identifiers than that (1 billion)
			"""
			assert column_name is not None, 'if from_reference is False, column_name of column to \
			be de-identified cannot be None'
			assert column_name in dataframe.columns, 'column_name must be in dataframe column set'

			keys = dataframe[column_name].unique()

			# Generate a unique list of random numbers that ensures no collision and also prevents us
			# from creating really large range() objects in memory

			values = set()
			value_size = keys.shape[0]
			# initialize a counter
			value_init = 0

			while value_init < value_size:
				r = np.random.randint(0, 100000000, 1000)
				# Generate 1000 at a time -- this should be reasonably fast
				for rand in r:
					if rand not in values:
						values.add(rand)
						value_init += 1
						if value_init == value_size:
							break

			values = pd.Series(list(values))
			self.reference_table = dict(zip(keys, values))
			self.key_name = column_name
			self.val_name = 'veil_id'

			assert self.key_name != self.val_name, 'Do not use veil_id as your column_name'

	def deidentify(self, dataframe, column_to_replace, update = True, debug = False):
		"""
		This function takes self.reference_table and maps it to a particular column in reference table
		If the update = True flag is set, it will also update the reference table with any new items
		that it encounters in the new column that it is searching over
		"""
		if update:
			# First, check that there are at least some overlap values:
			if len([x for x in dataframe[column_to_replace] if x in self.reference_table.keys()]) == 0:
				warnings.warn('There are no matches between the current reference table and the column \
that you are trying to replace. Please make sure that this is intended behavior')

			# Update the list of random numbers with new keys
			new_keys = [key for key in dataframe[column_to_replace].unique() if key not in self.reference_table]
			if len(new_keys) != 0:
				new_values = set()
				new_value_size = len(new_keys)
				value_init = 0

				while value_init < new_value_size:
					r = np.random.randint(0, 100000000, 1000)
					for rand in r:
						if (rand not in new_values) and (rand not in self.reference_table.values()):
							new_values.add(rand)
							value_init += 1
							if value_init == new_value_size:
								break
			new_values = pd.Series(list(new_values))

			new_table = dict(zip(new_keys, new_values))

			if debug:
				print(new_table)


			self.reference_table.update(new_table)
		else:
			if len([x for x in dataframe[column_to_replace] if x not in self.reference_table]) != 0:
				warnings.warn("There are values in this column which are not in this object's reference table. Additionally \
this function is being run with the update = False flag. Values which are not in the reference table are \
mapped to NaN")

		dataframe = dataframe.copy(deep = True)
		dataframe[column_to_replace] = dataframe[column_to_replace].map(self.reference_table)
		return dataframe

	def reidentify(self, dataframe, column_to_reidentify):
		"""
		This function performs the inverse operation of deidentify; that is, it takes the values
		of reference tables, looks for them in the `column_to_reidentify` argument, and then replaces
		them with the keys associated with those values. For any value which does not appear in 
		self.reference_table.values, a NaN is passed
		"""
		inverse_map = {v: k for k, v in self.reference_table.items()}
		dataframe = dataframe.copy(deep = True)
		dataframe[column_to_reidentify] = dataframe[column_to_reidentify].map(inverse_map)
		return dataframe

	def save(self, file, format = '.csv', debug = False):
		"""
		This function saves the current state of a self.reference table. Currently, the only format
		supported is csv, although others will be added soon. 
		"""
		save_df = pd.DataFrame.from_dict(self.reference_table, orient = 'index')
		save_df.index.name = self.key_name
		save_df.columns = [self.val_name]
		
		if debug:
			print(save_df.head())
			return None

		save_df.to_csv(file)
		return True


