#!/bin/bash

#sbatch --job-name=youtube_data_collection
#sbatch --output=yt_data_collection.out
#sbatch --error=yt_data_collection.err
#sbatch --time=5:00:00
#sbatch --partition=general
#sbatch --nodes=1
#sbatch --mem=8G
#sbatch --ntasks=1
#sbatch --mail-type=END,FAIL
#sbatch --mail-user=psen1@uvm.edu


source ~/anaconda3/etc/profile.d/conda.sh
conda activate /gpfs1/home/p/s/psen1/hashtag-hijacking/.conda

cd /gpfs1/home/p/s/psen1/hashtag-hijacking

python scripts/download_data/main.py \
    --start_date 2023-07-01T00:00 \
    --end_date 2025-07-01T00:00  \
    --queries data/hashtags.txt \
    --api_key AIzaSyDz-hIiz3-c2D73yAl6BWvr-Y1k71KZtV0