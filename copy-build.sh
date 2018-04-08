export DIR_IN_RACK="/specific/disk1/home/mad/Galois-PQ/inputs/students/orange/" 
export username="ynonflum"
#read -p "`echo -e 'Enter user \n'`" username \

echo user is ${username}

echo copy local dir to nova
scp -r ./orange ${username}@nova.cs.tau.ac.il:~/orange-tmp/

echo copy from nova to rack-mad-04
ssh -A -t -l $username nova.cs.tau.ac.il
scp -r ~/orange-tmp/ $username@rack-mad-04.cs.tau.ac.il:${DIR_IN_RACK}