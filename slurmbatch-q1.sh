#!/bin/bash
#
#SBATCH -p physical                 # partition (queue)
#SBTACH --nodes=1                   # node count
#SBATCH -c 1                        # number of cores
#SBATCH -t 0-2:00                   # time (D-HH:MM)
#SBATCH -o /home/hromanocuro/question-1/output/slurm.%N.%j.out          # STDOUT
#SBATCH -e /home/hromanocuro/question-1/output/slurm.%N.%j.err          # STDERR
#SBATCH --mail-type=ALL,ARRAY_TASKS
#SBATCH --mail-user=hromanocuro@student.unimelb.edu.au,agutierrezca@student.unimelb.edu.au

cd /home/$USER/question-1/
module load gcc/10.2.0 
module load openmpi/4.1.1
module load python/3.8.6
module load pip/21.2.4-python-3.8.6
pip3 install -r requirements.txt

mpiexec python3 main.py -p output/ -c 500 

