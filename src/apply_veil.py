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
    
    