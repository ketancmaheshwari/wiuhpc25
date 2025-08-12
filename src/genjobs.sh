#!/bin/bash

# 
# A stochastic simulator for a scheduling scheme that generates jobs with randomly chosen parameters and optionally submits them.
# Usage: ./genjobs.sh [options]
# Example: ./genjobs.sh 
# 

set -eou pipefail
#set -x

csvfname="$(whoami)_$(date +%s).csv"

numjobs=1
submitjobs=0
clearjobs=0

while getopts "scn:" option
do
  case $option in
    n     ) numjobs="$OPTARG" ;;
    s     ) submitjobs=1 ;;
    c     ) clearjobs=1 ;;
    *     ) echo "Unimplemented option chosen." ;;   # Default.
  esac
done

touch "$csvfname"
echo "timeofsub, user, jobname, nodes, walltime, qos" > "$csvfname"

for i in $(seq "$numjobs")
do
nodes=$(shuf -i 1-10 -n 1)
qos="normal"
qosnum=$(shuf -i 1-10 -n 1)
if [ "$qosnum" -eq 1 ]; then
  qosline='#SBATCH --qos=high'
else
  qosline='##'
fi
#case $qosnum in
#	1 ) qos="normal" ;;
#	2 ) qos="staff" ;;
#	3 ) qos="medium" ;;
#	4 ) qos="high" ;;
#esac

hh=$(shuf -i 0-0 -n 1)
mm=$(shuf -i 1-10 -n 1)
walltime="$hh:$mm:00"
totsec=$((hh*3600 + mm*60))
sleepsec=$(shuf -i 120-$((totsec + 60)) -n 1) # induce a few timeouts
jobname="$(whoami)-$(head /dev/urandom | tr -dc A-Za-z0-9 | head -c4)"

cat <<END>job"${i}".sh
#!/bin/bash
#SBATCH --nodes=$nodes
#SBATCH --tasks-per-node=64
##SBATCH -C nvme
#SBATCH -J $jobname
#SBATCH -o %x-%j.out
#SBATCH -e %x-%j.err
#SBATCH -A testing
#SBATCH -p batch-cpu
#SBATCH -t $walltime
$qosline
##SBATCH --qos=$qos
#SBATCH --reservation="system_testing"

##srun --export=ALL,MPICH_OFI_USE_PROVIDER="verbs;ofi_rxm" ./mpicatnap infile outfile.$(date +%s) $sleepsec
date +%s
srun ./mpicatnap infile outfile.$(date +%s) $sleepsec
date +%s
END

  # Submit the generated jobs if -s option provided
  if [ $submitjobs -eq 1 ]
  then
      sbatch job"${i}".sh
      # Record info in the csv
      echo "$(date +%s), $(whoami), $jobname, $nodes, $walltime, $qos" >> "$csvfname"
      sleep $(shuf -i 60-300 -n 1)
      #sleep 2
  fi

done

# Delete job script files if -c option provided
if [ $clearjobs -eq 1 ]
then
    for i in $(seq "$numjobs")
    do
        rm -f job"${i}".sh
    done
fi

