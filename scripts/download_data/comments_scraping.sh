#!/bin/bash

#SBATCH --job-name=youtube_comments_collection
#SBATCH --output=comments_collection.out
#SBATCH --error=comments_collection.err
#SBATCH --time=10:00:00
#SBATCH --partition=general
#SBATCH --nodes=1
#SBATCH --mem=30G
#SBATCH --ntasks=1


source ~/.bash_profile
conda init
conda activate hashtag-hijacking

python collect_comments.py \
    --max_comments 1000 \
    --DEBUG