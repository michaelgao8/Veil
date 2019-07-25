from veil import *
import argparse
import yaml
import os
from pathlib import Path
import argparse

# Eventualy CLI
parser = argparse.ArgumentParser(description='deidentify a directory of csv files')
parser.add_argument('-i', '--input-dir', help='input directory', type=str)
parser.add_argument('-o', '--output-dir', help='output directory', type=str)
parser.add_argument('-c', '--config', help='configuration file path', type=str)

if __name__ == '__main__':
    args = parse.parse_args()

    input_dir = args.input_dir
    output_dir = args.output_dir
    config_filepath = args.config

    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    
    # Read in the configuration file
    with open(args.config_filepath, 'r') as stream:
        try:
            configs = yaml.load(stream, Loader=yaml.FullLoader)
        except yaml.YAMLError as exc:
            print(exc)
        
    # Create associated ID maps:
    id_cols = [configs['files'][x]['id'] for x in configs['files']]
    # Flatten the array and take unique elements
    id_cols = set([id_col for id_col_list in id_cols for id_col in id_col_list]) 
    
    # instantiate veil instance
    new_veil = veil()
    for i in id_cols:
        new_veil.add_id_map(i)
    
    for f in os.listdir(input_dir):
        print('Now Deidentifying: {}'.format(f))
        fname = Path(f).stem
        fullpath = os.path.join(input_dir, f)
        with open(fullpath, 'r') as r, \
            open(output_dir + f, 'w') as w:

                to_drop = configs['files'][f]['exclude']
                reader = csv.DictReader(r)
                final_columns = reader.fieldnames.copy()
                if to_drop:
                    for drop in to_drop:
                        final_columns.remove(drop)
                writer = csv.DictWriter(w, final_columns)
                
                new_veil.deidentify(reader, writer,  \
                    time_columns = configs['files'][f]['datetime'], \
                    datetime_base_column = configs['datetime_base'], \
                    id_columns = configs['files'][f]['id'],
                    to_drop = to_drop) 
                