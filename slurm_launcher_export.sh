#!/bin/bash
#SBATCH --account=maths
#SBATCH --partition=ada
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=33
#SBATCH --mem=16G
#SBATCH --time=192:00:00
#SBATCH --job-name=neptune-export
#SBATCH --mail-user=dkcrua001@myuct.ac.za
#SBATCH --mail-type=END,FAIL
#SBATCH --output=/home/dkcrua001/neptune-exporter/slurm-%x-%j.out
#SBATCH --error=/home/dkcrua001/neptune-exporter/slurm-%x-%j.err

cd /home/dkcrua001/neptune-exporter

source .venv/bin/activate

which python
which neptune-exporter

python batch_export_v3.py
