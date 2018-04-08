read -p "`echo -e 'Enter user \n'`" username \
ssh -A -t -l $username nova.cs.tau.ac.il "ssh -A -t -l ${username} rack-mad-04.cs.tau.ac.il"