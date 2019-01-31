import veil
import argparse
import yaml
import pandas as pd 
import os

parser = argparse.ArgumentParser(description='Run Deidentification Procedure')
parser.add_argument("-i", "--input-dir", type = str, default="./",
                    help="Directory that contains the input files")
parser.add_argument("-f", "--file", type = str, default = "./veil.yml",
                    help = "Configuration YML file")
parser.add_argument("-o", "--output-dir", type = str, default = "./deidentified/",
                    help="Directory where to output the files")
parser.add_argument("-p", "--prefix", type = str, default = "", help = "Prefix to append to the deidentified file names")
parser.add_argument("-v", "--verbose", help = "get progress on the status of deidentification", action = "store_true")
args = parser.parse_args()


if __name__ == '__main__':

    if not os.path.exists(args.output_dir):
        os.mkdir(args.output_dir)
    verbose = args.verbose


    with open(args.file, 'r') as stream:
        try:
            configs = yaml.load(stream)
            if verbose:
                print('configs loaded from {}'.format(args.file))
        except yaml.YAMLError as exc:
            print(exc)

    # Create the associated ID maps:
    
    # TODO: Should we make it so that the same column name can refer to 2 different things?
    # Inverse problem of what we're currently solving
    if verbose:
        print('checking for aliases')
    alias_check = [key for key in configs if 'alias' in str(key).lower()]
    if verbose:
        print('alias checked')
    id_dicts = {}
    if verbose:
        print('creating list of unique IDs')
    for f in configs['files']:
        try:
            id_dicts[f] = pd.read_csv(args.input_dir + str(f), usecols = configs['files'][f]['id'])
        except UnicodeDecodeError:
            id_dicts[f] = pd.read_csv(args.input_dir + str(f), usecols = configs['files'][f]['id'], encoding = "ISO-8859-1")

    # id_dicts:
    # =========
    # A dictionary whose keys are the filenames (i.e. 'test.csv')
    # The values are a dataframe with only the associated ID columns extracted

    id_cols = [configs['files'][x]['id'] for x in configs['files']]
    id_cols = set([item for sublist in id_cols for item in sublist]) # flattens the array

    # iterate through aliases and then remove those elements from the list
    base_ids = {}
    for alias in alias_check:
        valid_id_cols = configs[alias]
        # Pull the associated series out and concatenate them 
        id_list = [id_dicts[dataframe][id_col] for id_col in valid_id_cols for dataframe in id_dicts if id_col in id_dicts[dataframe].columns]
        # Run a unique operation -- this returns a unique series of IDs
    
        base_ids[alias] = pd.Series(pd.concat(id_list).unique())

        # Remove from id_cols if possible -- the remainder will be used 
        # to create maps
        for col in valid_aliases:
            if col in id_cols:
                id_cols.remove(col)
    
        
    # Add other cols to base_ids
    for remaining_col in id_cols:
        id_list = [id_dicts[dataframe][remaining_col] for dataframe in id_dicts if remaining_col in id_dicts[dataframe]]
        
        base_ids[remaining_col] = pd.Series(pd.concat(id_list).unique())

    # base_ids:
    # ========
    # base_ids is a Dictionary where the keys are either an alias name
    # or an ID column. The values is a pd.Series that contains all of the
    # unique values for that ID or alias.
    # To check if an ID is an alias, check against configs[alias]

    if verbose:
        print('Creating associated maps')
    # Create ID maps
    id_mapping_dict = {}
    for key in base_ids:
        id_mapping_dict[key] = veil.id_map(id_column = base_ids[key])

    # Create time_offset maps
    time_mapping_dict = {}
    time_mapping_dict[configs['datetime_base']] = veil.offset_map(dataframe = None, method = 'random', max_days = configs['max_days'], id_column = base_ids[configs['datetime_base']])
    
    # TODO: allow for another column to be in a different alias group
    # For now, must be a stand-alone column name

    
    if verbose:
        print('All setup completed. Begin deidentification')
    # Begin deidentification block:

    for filename in configs['files']:
        if verbose:
            print('Now deidentifying: {}'.format(filename))
        try:
            df = pd.read_csv(args.input_dir + str(filename))
        except UnicodeDecodeError:
            df = pd.read_csv(args.input_dir + str(filename), encoding = "ISO-8859-1")


        # If the datetime_base column or a column as a part of an 
        # alias is not in this dataframe:

        if 'alias' not in configs['datetime_base'].lower():
            temp_ids = [configs['datetime_base']]
        else:
            temp_ids = configs[configs['datetime_base']] # all aliased columns

        # Check to see if the column in df is in temp_ids
        datetime_id_column = None
        for col in df.columns:
            if col in temp_ids:
                datetime_id_column = col
                break

        # Need to join in something else
        if datetime_id_column is None:
            if verbose:
                print('datetime_id not found in dataframe')
                print('now attempting to join in IDs')
                # Find all instances in id_dicts where both 
            for key in id_dicts:
                for col in df.columns:
                    for ids in temp_ids:
                        if col in id_dicts[key].columns and ids in id_dicts[key].columns:
                            df_col = col
                            temp_id = ids
            
            # Now, using those, we need to create a lookup
            join_df = pd.DataFrame()
            for key in id_dicts:
                if df_col in id_dicts[key].columns and temp_id in id_dicts[key].columns:
                    join_df = join_df.append(id_dicts[key][[df_col, temp_id]])
            join_df.drop_duplicates(inplace=True)
            datetime_id_column = df_col

        df = pd.merge(df, join_df, left_on = [df_col], right_on = [temp_id], how = 'left')

                            



        #     # Select the first match -- TODO: Change this? Could use some more transparency
        #     temp_id_col = temp_id[0]

        #     else:
        #         temp_id_col = configs['datetime_base']
            
        df = time_mapping_dict[configs['datetime_base']].apply_offset(dataframe = df, time_columns = configs['files'][filename]['datetime'], id_column = datetime_id_column, update = True)
        
        for id_column in configs['files'][filename]['id']:
            # lookup which id_mapping_dict
            # id_mapping_dict is a dict with the key being a column or alias 
            # and the value is the associated id_map
            for alias in alias_check:
                valid_cols = configs[alias]
                if id_column in valid_cols:
                    df = id_mapping_dict[alias].deidentify(df, id_column, update=True)
            
            if id_column in id_mapping_dict:
                df = id_mapping_dict[id_column].deidentify(df, id_column, update=True)
        if configs['files'][filename]['exclude'] is not None:
            for col_to_exclude in configs['files'][filename]['exclude']:
                df.drop(col_to_exclude, axis = 1, inplace = True)
                if verbose:
                    print('Excluding column {} in dataframe {}'.format(col_to_exclude, filename))
            
        if verbose:
            print('Now writing deidentified dataframe to {}'.format(args.output_dir + str(args.prefix) + str(filename)))
        df.to_csv(args.output_dir + str(args.prefix) + str(filename))
        del df


    
        
    