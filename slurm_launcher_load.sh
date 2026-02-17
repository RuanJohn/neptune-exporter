#!/bin/bash
#SBATCH --account=maths
#SBATCH --partition=ada
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=33
#SBATCH --mem=16G
#SBATCH --time=192:00:00
#SBATCH --job-name=wandb-loader
#SBATCH --mail-user=dkcrua001@myuct.ac.za
#SBATCH --mail-type=END,FAIL
#SBATCH --output=/home/dkcrua001/neptune-exporter/slurm-%x-%j.out
#SBATCH --error=/home/dkcrua001/neptune-exporter/slurm-%x-%j.err

export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1

export WANDB_DIR="/scratch/$USER/wandb"
export WANDB_CACHE_DIR="/scratch/$USER/wandb-cache"
export WANDB_CONFIG_DIR="/scratch/$USER/wandb-config"
export WANDB_ARTIFACT_DIR="/scratch/$USER/wandb-artifacts"
mkdir -p "$WANDB_DIR" "$WANDB_CACHE_DIR" "$WANDB_CONFIG_DIR" "$WANDB_ARTIFACT_DIR"

cd /home/dkcrua001/neptune-exporter

source .venv/bin/activate

which python
which neptune-exporter

python batch_load_wandb.py
