"""
Accept list of Twitter JSON files and write them to tab-delimited files
    - skip retweets
    - replace usernames and URLs with @USER and HTTPURL, respectively
    - replace all whitespace with a single space (i.e., tabs and newlines)
    - remove tweets with less than 3 tokens, not including usernames and URLs

And track stats
    - keep list of deleted tweets
    - number of tweets, retweets, and tokens
    - number of tweets per language

Authors: Mark Dredze, Alexandra DeLucia
"""
import sys
import regex as re
import ujson as json
import gzip
import argparse
import time
from collections import Counter
from tqdm import tqdm
import logging
import emoji
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

##########
# Methods
##########
URL_RE = re.compile(r"https?:\/\/[\w\.\/\?\=\d&#%_:/-]+")
WHITESPACE_RE = re.compile(r"\s+")
HANDLE_RE = re.compile(r"@\w+")


def tokenize_tweet(text):
    text = HANDLE_RE.sub("@USER", text)
    text = URL_RE.sub("HTTPURL", text)
    text = WHITESPACE_RE.sub(" ", text)
    length = sum([1 for t in text.split() if t!="@USER" and t!="HTTPURL"])
    return text, length


def process_file(input, output, no_retweets, langs, delete_ids_writer, stats, emoji_counts):
    for num, line in enumerate(tqdm(input, ncols=0, desc="Tweets")):
        line = line.strip()
        if len(line) == 0:
            continue
        doc = json.loads(line)

        if no_retweets and doc.get('retweeted_status', False):
            stats["retweets"] += 1
            continue

        if 'delete' in doc and delete_ids_writer:
            delete_ids_writer.write('{}\n'.format(doc.get('delete').get('status').get('id_str')))
            stats["deleted_tweets"] += 1
            continue

        text = doc.get('extended_tweet', {}).get('full_text', None)
        if not text:
            text = doc.get('text', None)
        if not text:
            continue

        text, length = tokenize_tweet(text)
        if length < 3:
            continue
        
        lang = doc.get('lang')
        id = doc.get('id_str')
        created_at = doc.get('created_at')
        user_id = doc.get('user').get('id_str')
        timestamp = int(time.mktime(time.strptime(created_at, '%a %b %d %H:%M:%S +0000 %Y')))
        # emoji
        emoji_list = emoji.distinct_emoji_list(text)
        if len(emoji_list) == 0 or len(emoji_list) > 1:
            continue
        if lang not in emoji_counts:
            emoji_counts[lang] = {}
        if emoji.demojize(emoji_list[0]) in emoji_counts[lang]:
            emoji_counts[lang][emoji.demojize(emoji_list[0])] += 1
        else:
            emoji_counts[lang][emoji.demojize(emoji_list[0])] = 1

        if langs != None:
            langs[lang] += 1

        output.write('{}\t{}\t{}\t{}\t{}\n'.format(id, user_id, timestamp, lang, text))
        stats["tweets"] += 1
        

##########
# Main
##########
def parse_args():
    parser = argparse.ArgumentParser(description='Dump data to text (gzip).')
    parser.add_argument('--input-files', nargs='+', help='input files to dump')
    parser.add_argument('--output-file', required=True, help='the text file (gzip)')
    parser.add_argument('--no-retweets', action='store_true', help='skip retweets')
    parser.add_argument('--language-file', default=None, help='save language counts to this file')
    parser.add_argument('--delete-ids-file', default=None, help='save deleted tweet ids to this file')
    parser.add_argument('--stats-file', default=None, help='save tweet stats to this file')
    parser.add_argument('--emoji-list', default=None, help='save emoji lists to this file')
    return parser.parse_args()


def main():
    args = parse_args()

    langs = None
    if args.language_file:
        langs = Counter()

    delete_ids_writer = None
    if args.delete_ids_file:
        delete_ids_writer = gzip.open(args.delete_ids_file, 'wt', encoding='utf-8')

    stats = {
        "total_tokens": 0,
        "tweets": 0,
        "deleted_tweets": 0,
        "retweets": 0
    }

    emoji_counts = {}

    with gzip.open(args.output_file, 'wt', encoding='utf-8') as output:
        for input_file in tqdm(args.input_files, ncols=0, desc="File"):
            try:
                with gzip.open(input_file) as input:
                    process_file(input, output, args.no_retweets, langs, delete_ids_writer, stats, emoji_counts)
            except Exception as e:
                logger.error(f"Encountered error {e} on {input_file}")
                pass

    if args.language_file:
        with open(args.language_file, 'wt', encoding='utf-8') as output:
            for language, count in langs.most_common(len(langs)):
                output.write('{}\t{}\n'.format(language, count))

    if delete_ids_writer:
        delete_ids_writer.close()

    with open(args.stats_file, "w") as f:
        for key, value in stats.items():
            f.write(f"{key}\t{value}\n")

    with open(args.emoji_list, "w") as f:
        for lang, emojis in emoji_counts.items():
            for key, value in emojis.items():
                f.write(f"{lang}\t{key}\t{value}\n")


if __name__ == '__main__':
    main()
