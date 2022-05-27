import logging
import sys
import emoji
import argparse
from joblib import Parallel, delayed
import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(description="get emoji counts from emoji file counts")
    parser.add_argument("--output_file", required=True, help="the csv file")
    parser.add_argument("--input_files", nargs="+", help="input files to read")
    return parser.parse_args()

def read_file(file_path):
    """
    Reads file and gets counts. Expects
    lang_id \t emoji \t count
    """
    with open(file_path):
        rows = pd.read_csv(file_path, sep='\t', header=None, names=['lang', 'emoji', 'count'])
    chunks = file_path.split("/")[-1].split("_")
    rows['date'] = [f"{chunks[1]}-{chunks[2]}-{chunks[3]}" for _ in range(len(rows))]
    return rows


def main():
    args = parse_args()

    logging.info(f"Doing {len(args.input_files)} files")
    pandases = Parallel(n_jobs=8, verbose=10)(delayed(read_file)(f) for f in args.input_files)
    big_df = pd.concat(pandases, ignore_index=True)
    big_df = big_df.groupby(['lang', 'emoji', 'date']).sum().reset_index()
    big_df.to_csv(args.output_file, index=False)
    return

if __name__ == "__main__":
    main()
