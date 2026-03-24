#!/bin/bash

#SBATCH --job-name=youtube_hashtag_collection
#SBATCH --output=yt_hashtag_collection.out
#SBATCH --error=yt_hashtag_collection.err
#SBATCH --time=10:00:00
#SBATCH --partition=general
#SBATCH --nodes=1
#SBATCH --mem=30G
#SBATCH --ntasks=1


source ~/.bash_profile
conda init
conda activate hashtag-hijacking

python collect_hashtags.py \
    --start_date 2025-10-02T00:00 \
    --end_date 2025-10-03T00:00 \
    --DEBUG