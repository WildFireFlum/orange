buildir=/specific/disk1/home/mad/Galois-PQ/inputs/students/orange/build/
read -p "`echo -e 'Enter user \n'`" username \
ssh -A -t -l $username nova.cs.tau.ac.il "ssh -A -t -l ${username} rack-mad-04.cs.tau.ac.il"

rmdir $buildir
mkdir $buildir
cd $buildir
cmake ../galois
make