# Paxos Total Order Algorithm used by Treasure Island game
DISCLAIMER:
This project only contains the main algorithm used for paxos message delivery as not all content in the complete project including the GUI and gcl was purely of my own. Thus for copyright reasons, only the code which belongs totally to me has been posted on this repository, although it does not represent all code written by me.

Description:
The code provides a functionning paxos based consensus algorithm to receive and deliver moves from a game in total order delivery.
There are three key components to this algorithm:
1. A listener thread which continously reads messages from the gcl and distributes them to the appropriate handler (Proposer vs Acceptor).
2. An acceptor thread which is always available to answer to requests from proposers on other processes.
3. A proposer which runs synchronously with its calling thread to start a paxos round for its proposed value. It only returns when a majority of processes have accepted the value.
