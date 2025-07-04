#!/bin/bash

#SBATCH --job-name=youtube_data_collection
#SBATCH --output=yt_data_collection.out
#SBATCH --error=yt_data_collection.err
#SBATCH --time=5:00:00
#SBATCH --partition=general
#SBATCH --nodes=1
#SBATCH --mem=30G
#SBATCH --ntasks=1
#SBATCH --mail-type=END,FAIL


source ~/.bash_profile
conda hashtag-hijacking

cd scripts/download_data

python main.py \
    --start_date 2023-07-01T00:00 \
    --end_date 2025-07-01T00:00  \
    --queries data/hashtags.txt \
    --api_key AIzaSyDz-hIiz3-c2D73yAl6BWvr-Y1k71KZtV0


