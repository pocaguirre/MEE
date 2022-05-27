#!/bin/bash

#$ -wd /export/b03/caguirr/emoji-eval/logs
#$ -N count_emojis
#$ -j y -o $JOB_NAME-$JOB_ID.$TASK_ID.out
#$ -M caguirr4@jhu.edu
#$ -l ram_free=20G,mem_free=20G
#$ -pe smp 8

# Activate dev environments and call programs
conda activate emoji-eval
# Define input directory, temp directory, and path to final feature output file
INPUT_DIR="/export/b03/caguirr/emoji-eval/twitter_data/raw"
OUTPUT_DIR="/export/b03/caguirr/emoji-eval/twitter_data/counts"
mkdir -p "${OUTPUT_DIR}"

# Tweets organized in year/month folders
YEARS=( "2022" )
MONTHS=( "01" "02" "03" "04" "05" )
INPUT_FILES=()
for y in "${YEARS[@]}"
do
  for m in "${MONTHS[@]}"
  do
    # Add to input files
    INPUT_FILES+=( "${INPUT_DIR}/${y}/${m}"/emoji*.txt )

  done
done

#echo "${#INPUT_FILES[@]}"

#export file="${INPUT_FILES[$SGE_TASK_ID]}"
#echo "On file ${file}"

  # Get correct year/month folder for file
#folder=$( python -c $'import re\nimport os\nprint(re.findall("\d{4}/\d{2}", os.environ["file"])[0])' )
#file_prefix=$( python -c $'import re\nimport os\nprint(os.environ["file"].split("/")[-1].split(".")[0])' )

  # echo -e "$file\n$folder\n$file_prefix"

  # Make sure file isn't already done
  # If so, move on to the next file
#  FILE="${OUTPUT_DIR}/${folder}/langs.txt"
#  if [ -f "$FILE" ]; then
#      echo "$FILE exists. Skipping."
#      continue
#  fi

  # Process files for task
#python "${MEE}filter_tweets.py" \
#    --input-files "${INPUT_FILES[$SGE_TASK_ID]}" \
#    --output "${OUTPUT_DIR}/${folder}/output_${file_prefix}.gz" \
#    --language-file "${OUTPUT_DIR}/${folder}/langs_${file_prefix}.txt" \
#    --delete-ids-file "${OUTPUT_DIR}/${folder}/deleted_${file_prefix}.gz" \
#    --stats-file "${OUTPUT_DIR}/${folder}/stats_${file_prefix}.txt" \
#    --no-retweets \
#    --emoji-list "${OUTPUT_DIR}/${folder}/emoji_${file_prefix}.txt"

python "${MEE}emoji_counts.py" \
	--output_file "${OUTPUT_DIR}/counts.csv" \
	--input_files "${INPUT_FILES[@]}"



  # Check exit status
status=$?
if [ $status -ne 0 ]
then
    echo "Task ${SGE_TASK_ID} failed"
    exit 1
fi

done

echo "Done"
