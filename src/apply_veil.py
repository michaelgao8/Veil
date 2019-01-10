import veil
import argparse
import yaml
import pandas as pd 

parser = argparse.ArgumentParser(description='Run Deidentification Procedure')
parser.add_argument("-i", "--input-dir", type = str, default="./",
                    help="Directory that contains the input files")
parser.add_argument("-f", "--file", type = str, default = "./veil.yml",
                    help = "Configuration YML file")
args = parser.parse_args()


if __name__ == '__main__':
    with open(args.file, 'r') as stream:
        try:
            configs = yaml.load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    # Create the associated ID maps:
    
    # TODO: Should we make it so that the same column name can refer to 2 different things?
    # Inverse problem of what we're currently solving
    alias_check = [key for key in configs if 'alias' in str(key).lower()]
    
    id_dicts = {}
    for f in configs['files']:
        id_dicts[f] = pd.read_csv(args.input_dir + str(f), usecols = configs['files'][f]['id'])
    
    id_cols = [configs['files'][x]['id'] for x in configs['files']]
    id_cols = set([item for sublist in id_cols for item in sublist]) # flattens the array

    # iterate through aliases and then remove those elements from the list
    base_ids = {}
    for alias in alias_check:
        valid_aliases = configs[alias]
        # Pull the associated series out and concatenate them 

        # Run a unique operation -- this returns a unique series of IDs
    
        base_ids[alias] = <unique_list>

        # Remove from id_cols if possible -- the remainder will be used 
        # to create maps
        for col in valid_aliases:
            if col in id_cols:
                id_cols.remove(col)
        
    # === Code block implementing datetime shifting (which base to look at for time_offsets)

    
    # Create ID maps
    # Create time_offset maps
        

    
        
    