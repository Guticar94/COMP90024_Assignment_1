#!/bin/bash
#
#SBATCH -p physical                     # partition (queue)
#SBATCH --nodes=1                       # node count
#SBATCH --ntasks-per-node=1             # number of tasks per node
#SBATCH --cpus-per-task=1               # number of cpus per task
#SBATCH -t 0-3:00                       # time (D-HH:MM)
#SBATCH -o /home/hromanocuro/COMP90024_ASSIGNMENT_1/output/slurm-1node-1core.%N.%j.out          # STDOUT
#SBATCH -e /home/hromanocuro/COMP90024_ASSIGNMENT_1/output/slurm-1node-1core.%N.%j.err          # STDERR
#SBATCH --mail-type=ALL,ARRAY_TASKS
#SBATCH --mail-user=hromanocuro@student.unimelb.edu.au,agutierrezca@student.unimelb.edu.au

cd /home/$USER/COMP90024_ASSIGNMENT_1/
module load gcc/10.2.0 
module load openmpi/4.1.1
module load python/3.8.6
mkdir ./virtualenv
virtualenv ./virtualenv/python3.8.6
source ./virtualenv/python3.8.6/bin/activate
pip install -r requirements.txt

mpiexec python3 main.py -p ./output/ -c 100000 -t "1-node-1-core" -n "$SLURM_JOB_NODELIST"
deactivate