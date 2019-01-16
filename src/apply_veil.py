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
    if verbose:
        print('Creating associated maps')
    # Create ID maps
    id_mapping_dict = {}
    for key in base_ids:
        id_mapping_dict[key] = veil.id_map(id_column = base_ids[key])

    # Create time_offset maps
    time_mapping_dict = {}
    time_mapping_dict[configs['datetime_base']] = veil.offset_map(dataframe = None, method = 'random', max_days = configs['max_days'], id_column = base_ids[configs['datetime_base']])
    
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
        df = time_mapping_dict[configs['datetime_base']].apply_offset(dataframe = df, time_columns = configs['files'][filename]['datetime'], id_column = configs['datetime_base'], update = True)
        
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


    
        
    