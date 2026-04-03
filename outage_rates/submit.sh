#!/bin/bash
#SBATCH --account=reedsweto
#SBATCH --time=4:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --mail-user=pbrown@nrel.gov
#SBATCH --mail-type=FAIL
#SBATCH --mem=246000
#SBATCH --output=/projects/reedsweto/pbrown/logs/slurm-%j.out
#SBATCH --job-name=temperature_profiles

# add >>> #SBATCH --qos=high <<< above for quicker launch at double AU cost
# add >>> #SBATCH --qos=standby <<< above for slower launch at no AU cost

python temperature_profiles.py
### $ sbatch submit.sh
