DIR_IN_RACK="/specific/disk1/home/mad/Galois-PQ/inputs/students/orange/" 
DIR_IN_NOVA="~/orange-tmp/"
username="ynonflum"
#export first_server="nova.cs.tau.ac.il"
first_server="192.168.38.128"
#read -p "`echo -e 'Enter user \n'`" username \

printf "user is ${username} \n"

printf "copy local dir to nova\n"
rsync -r ./galois $username@$first_server:$DIR_IN_NOVA

#echo copy from nova to rack-mad-04
#ssh -A -t -l $username nova.cs.tau.ac.il
#scp -r ~/orange-tmp/ $username@rack-mad-04.cs.tau.ac.il:${DIR_IN_RACK}