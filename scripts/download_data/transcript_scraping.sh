#!/bin/bash

#SBATCH --job-name=youtube_data_collection
#SBATCH --output=yt_data_collection.out
#SBATCH --error=yt_data_collection.err
#SBATCH --time=10:00:00
#SBATCH --partition=general
#SBATCH --nodes=1
#SBATCH --mem=30G
#SBATCH --ntasks=1

source ~/.bash_profile
conda init
conda activate hashtag-hijacking

python collect_transcripts.py \
    --start_date 2023-07-01T00:00 \
    --end_date 2023-07-02T00:00  \
    --query bmw \
    --DEBUG