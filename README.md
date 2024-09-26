<h1>Read ME<h1>

The chord is currently set to be of size 2^10, but can be extended to up to 2^160 by changing the "M" variable in the node class.

When running the shell script you need to specify the number of nodes be initiated in the circle. This will create the nodes and automatically connect them up into a functional distributed hash table storage.
```bash
run.sh 4
```
To run the Chord-tester you take the output from the bash script:
```bash
python3 chord-tester.py c6-6:65170 c6-6:65170 c11-12:60459 ....
```
The first variable is the client and after that a list of all the nodes in the system as shown above.
<p>The servers will automatically close after 10 minutes</p>

