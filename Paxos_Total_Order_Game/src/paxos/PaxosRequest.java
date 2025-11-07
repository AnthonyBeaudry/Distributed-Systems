package src.paxos;

import java.io.Serializable;
import java.util.HashSet;

record PaxosRequest(PaxosRequestType requestType, PlayerMove playerMove, boolean isSuccess, String origin, HashSet<PlayerMove> history, int round) implements Serializable
{
    public PaxosDestination paxosDestination()
    {
        if(requestType == PaxosRequestType.PROMISE || requestType == PaxosRequestType.ACCEPTACK)
            return PaxosDestination.PROPOSER;
        else
            return PaxosDestination.ACCEPTOR;
    }

    public double ballotID()
    {
        if(playerMove == null)
            return 0.0;
        return playerMove().ballotID();
    }
}

enum PaxosRequestType { PROPOSE, PROMISE, ACCEPT, ACCEPTACK, CONFIRM }
enum PaxosDestination { PROPOSER , ACCEPTOR }

record PlayerMove(double ballotID, int playerNum, char cmd, String uniqueHash) implements Serializable
{
    public int hashCode()
    {
        return uniqueHash.hashCode();
    }

    public boolean equals(Object o)
    {
        if(!(o instanceof PlayerMove move))
            return false;

        return move.uniqueHash().equals(uniqueHash());
    }

    public int round()
    {
        return (int) ballotID;
    }
}
