#!/bin/bash
#SBATCH --account=[HPC allocation]
#SBATCH --ntasks-per-node=1
#SBATCH --mail-user=[your email here]
#SBATCH --mail-type=END
#SBATCH --mem=246000
#SBATCH -L gurobi:1
#SBATCH --time=01:00:00
#SBATCH --partition=debug

source ~/.bashrc
module purge
module load anaconda3
module load gurobi
conda activate Opt_3
cd /projects/last10p/mvanatta/County_Transmission/

python Transmission_LP_multiperiod_iter.py --reg CA.WA.OR --type st --hr_sample 4 --exportname NARIS_plusERBA --importname transmission_capacity_init_AC_county_NARIS2024