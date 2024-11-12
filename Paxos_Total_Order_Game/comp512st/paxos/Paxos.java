package comp512st.paxos;

import comp512.gcl.*;
import comp512.utils.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.*;
import java.net.UnknownHostException;

/*
 * Paxos class follows a total order delivery system based on Paxos consensus algorithm.
 * Used to play a game TreasureIsland and assumes one node containing both Proposer and Acceptor.
 */
public class Paxos
{
	// Variables for to determine message order and paxos instance rounds
	private int completedRounds = 0;
	private int processID;
	private double ballotID;

	// Constants for each paxos instance
	private final String myProcess;
	private final int groupLength;
	private final int majorityCount;
	private final Logger logger;
	private final String[] otherProcesses;
	private final int timeoutDuration = 250; // 250 millisecond timeout

	// Communication layer and shutdown utilities
	private final GCL gcl;
	private final FailCheck failCheck;
	private Thread taThread = null;
	private volatile boolean isShutdown = false;

	// Threads and members of Paxos algorithm
	private Proposer proposer = null;
	private final Acceptor acceptor;
	private final Thread listener;

	// Paxos Instance value to try and get majority for
	private PlayerMove playerMove = null;

	// Thread safe data structures to handle storing information for values/acknowledgements
	private final Lock paxosLock = new ReentrantLock(true);
	private final BlockingQueue<PlayerMove> messagesToDeliver = new ArrayBlockingQueue<>(50);
	private final ConcurrentHashMap<Integer, PlayerMove> confirmedMoves = new ConcurrentHashMap<>(20);
	private final HashSet<PlayerMove> history = new HashSet<>();

	// FailCheck allows for automatic testing at desired moments
	public Paxos(String myProcess, String[] allGroupProcesses, Logger logger, FailCheck failCheck) throws IOException, UnknownHostException
	{
		// Remember to call the failCheck.checkFailure(..) with appropriate arguments throughout your Paxos code to force fail points if necessary.
		this.failCheck = failCheck;
		this.myProcess = myProcess;
		this.logger = logger;

		// Initialize the GCL communication system
		gcl = new GCL(myProcess, allGroupProcesses, null, logger) ;
		groupLength = allGroupProcesses.length;
		majorityCount = ((allGroupProcesses.length) / 2) + 1;

		// Determine unique ballotID since the list of processes contains the same processes for each system (could be unordered)
		String[] groupCopy = allGroupProcesses.clone();
		Arrays.sort(groupCopy);
		processID = Arrays.binarySearch(groupCopy, myProcess) + 1;
		ballotID = completedRounds + (0.1 * processID);

		// Make array of other processes excluding myProcess for multicast messages
		List<String> temp = new ArrayList<>(Arrays.asList(groupCopy));
		temp.remove(processID-1);
		otherProcesses = temp.toArray(new String[0]);

		// Start threads
		acceptor = new Acceptor();
		Listener listenerTask = new Listener();
		acceptor.start();
		listener = Thread.startVirtualThread(listenerTask); // Virtual thread as responsible for mostly I/O and blocking operations
		proposer = new Proposer();
	}


	// This is what the application layer is going to call to send a message/value, such as the player and the move
	// This method call blocks, and is returned only when a majority (and immediately upon majority) of processes have accepted the value
	public void broadcastTOMsg(Object val) throws NotSerializableException
	{
		Object[] info = (Object[]) val;

		// Wait for previous proposer round to be completed when it is confirming messages
		try
		{
			if(proposer.thirdPhase != null && proposer.thirdPhase.isAlive())
				proposer.thirdPhase.join();
		}
		catch (InterruptedException e) {}

		// Set player move with thread safe method and start synchronous run of proposer
		setPlayerMove(ballotID, (Integer)info[0], (Character)info[1], true);
		proposer.run();
	}

	// This is what the application layer is calling to figure out what is the next message in the total order.
	// Messages delivered in ALL the processes in the group should deliver this in the same order.
	public Object acceptTOMsg() throws InterruptedException
	{
		// Identifying the current thread that takes care of game representation
		taThread = Thread.currentThread();

		// Verify is a shutdown was initiated. If so, we only interrupt if queue is empty
		if(isShutdown && messagesToDeliver.isEmpty())
		{
			throw new InterruptedException();
		}

		// Wait until we have a messageToDeliver then deliver it
		PlayerMove move = messagesToDeliver.take();
		return new Object[] { move.playerNum(), move.cmd() }; // Format followed by TreasureIslandApp
	}

	public void shutdownPaxos()
	{
		// Wait on proposer to finish the process of confirming values for a round
		if(proposer.thirdPhase != null && proposer.thirdPhase.isAlive())
		{
			try
			{
				proposer.thirdPhase.join();
			}
			catch (InterruptedException e) { }
		}

		isShutdown = true;

		// Stop any threads as interrupt will raise exceptions on each of the threads in their run method or their read methods
		try
		{
			listener.join(5000); // Longer delay for a graceful shutdown
			listener.interrupt();
		}
		catch (InterruptedException e) {}
		acceptor.shutdown();

		// Shut down own gcl
		gcl.shutdownGCL();

		// If there are no messages to deliver, then we can shut down the game, else wait and signal the end of game through other thread
		try
		{
			if(!messagesToDeliver.isEmpty() && taThread != null)
				taThread.join();
			proposer.executor.shutdown();
			proposer.executor.awaitTermination(timeoutDuration, TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e) { }
	}

	private synchronized void setPlayerMove(double ballotID, int playerNum, char cmd, boolean createUniqueHash)
	{
		playerMove = new PlayerMove(ballotID, playerNum, cmd, createUniqueHash ? "Player " + playerNum + " " + System.currentTimeMillis() : playerMove.uniqueHash());
	}


	// Will check if the messages in the queue have the expected round and if so, will send them to messagesToDeliver
	private void processConfirmedMovesForDelivery(PlayerMove confirmedMove)
	{
		paxosLock.lock();
		if(confirmedMove != null && !confirmedMoves.containsKey(confirmedMove.round())) // Double check for concurrency concerns for no duplicates
			confirmedMoves.put(confirmedMove.round(), confirmedMove);
		try
		{
			while(confirmedMoves.containsKey(completedRounds))
			{
				PlayerMove move = confirmedMoves.remove(completedRounds);
				try
				{
					messagesToDeliver.put(move);
				}
				catch (InterruptedException e)
				{
					confirmedMoves.put(completedRounds, move);
					break;
				}
				history.add(move);

				// Increment round in successful rounds
				// For Fairness purposes, we always change the processID to change priority of each process at each round
				processID = processID == groupLength ? 1 : processID + 1;
				ballotID = ++completedRounds + (0.1 * processID);
			}
		}
		finally
		{
			paxosLock.unlock();
		}
	}

	// Upon receiving a history from another process with non-matching round numbers
	private void addMissingConfirmedMoves(HashSet<PlayerMove> receivedHistory)
	{
		// Don't want to add duplicates or moves from earlier rounds so locking to avoid processConfirmedMoves to be called concurrently
		paxosLock.lock();
		try
		{
			for(PlayerMove move : receivedHistory)
			{
				int moveRound = move.round();
				if(moveRound >= completedRounds && !confirmedMoves.containsKey(moveRound))
					confirmedMoves.put(moveRound, move);
			}
		}
		finally
		{
			paxosLock.unlock();
		}
	}

	// Thread safe method used to store a history copy and clear it to message pass from the proposer to other processes
	private HashSet<PlayerMove> copyHistoryAndClear()
	{
		paxosLock.lock();
		HashSet<PlayerMove> copy = history;
		try
		{
			copy = new HashSet<PlayerMove>(history);
			history.clear();
		}
		finally
		{
			paxosLock.unlock();
		}
		return copy;
	}


	/*
	 * Responsible for listening for any incoming messages from the GCL and distributing
	 * that message to the correct origin which is either for a Proposer or Acceptor.
	 */
	private class Listener implements Runnable
	{
		public void run()
		{
			while(!isShutdown && !listener.isInterrupted())
			{
				try
				{
					GCMessage gclMessage = gcl.readGCMessage();

					// Ensure the value is a PaxosRequest
					if(!(gclMessage.val instanceof PaxosRequest paxosRequest))
						continue;

					// keepRunning check to ensure we don't overflow the queue and end up blocking the listener thread
					if(paxosRequest.paxosDestination() == PaxosDestination.PROPOSER && proposer.keepRunning)
					{
						proposer.queue.put(paxosRequest);
					}
					else if (paxosRequest.paxosDestination() == PaxosDestination.ACCEPTOR)
					{
						acceptor.queue.put(paxosRequest);
					}
				}
				catch(InterruptedException | IllegalStateException e)
				{
					logger.log(Level.SEVERE, "LISTENER: GCL failure. Shutting down initiated. \nException:" + e.getMessage());
					listener.interrupt();
					return;
				}
				catch(Exception ex)
				{
					logger.log(Level.WARNING, "LISTENER: Unexpected exception : " + ex.getMessage());
				}
			}
			logger.log(Level.INFO, "LISTENER: Done.");
		}
	}

	// Acceptor from Paxos Consensus which is always available to treat incoming request by being a different thread
	private class Acceptor extends Thread {

		double maxBallotID = -1.0;
		PaxosRequest request;
		PlayerMove previousAcceptedMove = null;

		// BlockingQueue for treating requests which priorities higher ballotID's to keep the algorithm quick
		BlockingQueue<PaxosRequest> queue = new PriorityBlockingQueue<PaxosRequest>(groupLength * 2, Comparator.comparing(PaxosRequest::ballotID).reversed());

		// Timeout utilities
		private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
		private ScheduledFuture<?> timeoutOnAcceptWait = null;
		private ScheduledFuture<?> timeoutOnConfirmWait = null;

		public void run()
		{
			while(!isInterrupted())
			{
				try
				{
					request = queue.take();
					handleRequest();
				}
				catch(InterruptedException e)
				{
					logger.log(Level.SEVERE, "ACCEPTOR: Unexpected interrupted exception of thread. Shutting down initiated");
					return;
				}
				catch(Exception ex)
				{
					logger.log(Level.WARNING, myProcess + "ACCEPTOR: Unexpected exception: " + ex.getMessage());
				}
			}
			logger.log(Level.INFO, myProcess + "ACCEPTOR: Done.");
		}

		// Handles all cases for a message intended for the Acceptor (PROPOSE, ACCEPT?, CONFIRM)
		private void handleRequest()
		{
			PlayerMove move = request.playerMove();
			PaxosRequestType requestType = null;
			boolean isSuccess = false;
			boolean sendHistory = false;

			switch(request.requestType())
			{
				case PROPOSE ->
				{
					// FailCheck components
					failCheck.checkFailure(FailCheck.FailureType.RECEIVEPROPOSE);

					requestType = PaxosRequestType.PROMISE;

					// Occurs when another process has missed confirmed requests so we will send our own history
					if(move.round() < completedRounds)
					{
						sendHistory = true;
						break;
					}

					// Case where we are missing confirmed requests, note we are fine with blocking as acceptor is not up to date with group
					if(move.round() > completedRounds)
					{
						addMissingConfirmedMoves(request.history());
						processConfirmedMovesForDelivery(null);
						previousAcceptedMove = null;
					}

					isSuccess = handlePropose(move);
				}

				case ACCEPT ->
				{
					requestType = PaxosRequestType.ACCEPTACK;
					isSuccess = handleAcceptMove(move);
				}

				case CONFIRM ->
				{
					handleConfirmMove(move, false);
					return;
				}
			}

			PaxosRequest sendRequest = new PaxosRequest(requestType, previousAcceptedMove, isSuccess, null, sendHistory ? new HashSet<PlayerMove>(history) : null, (int)maxBallotID);
			try
			{
				gcl.sendMsg(sendRequest, request.origin());
			}
			catch(IllegalStateException e)
			{
				interrupt();
				logger.log(Level.SEVERE, "ACCEPTOR: GCL Failure as sending a message resulted in IllegalStateException.");
			}
			finally
			{
				if(request.requestType() == PaxosRequestType.PROPOSE)
					failCheck.checkFailure(FailCheck.FailureType.AFTERSENDVOTE);
			}
		}

		// Determine whether we accept or deny the proposed value
		synchronized boolean handlePropose(PlayerMove move)
		{
			// In case of isProposer true, then we only want to do it if no other leaders, i.e. maxBallotID is int
			if(move.ballotID() > maxBallotID)
			{
				// Set a timeout task that such that if we don't receive accept request within 2 secs, we notify timeout
				if(timeoutOnAcceptWait == null)
					timeoutOnAcceptWait = scheduler.schedule(this::timedOut, timeoutDuration, TimeUnit.MILLISECONDS);
				maxBallotID = move.ballotID();
				return true;
			}

			return false;
		}

		// Determine whether we accept or deny the proposed value
		synchronized boolean handleAcceptMove(PlayerMove move)
		{
			if(Math.abs(move.ballotID() - maxBallotID) < 0.0001)
			{
				// Cancel timeout and reset it
				if(timeoutOnAcceptWait != null && !timeoutOnAcceptWait.isDone())
					timeoutOnAcceptWait.cancel(false);

				timeoutOnAcceptWait = null;
				previousAcceptedMove = move;

				// Set timeout task for confirm expectation
				if(timeoutOnConfirmWait == null)
					timeoutOnConfirmWait = scheduler.schedule(this::timedOut, timeoutDuration, TimeUnit.MILLISECONDS);

				return true;
			}
			return false;
		}

		// When receiving a confirm operation from a proposer, need to check if the confirmed round is at least current
		synchronized void handleConfirmMove(PlayerMove move, boolean isProposer)
		{
			int moveRound = move.round();
			if(moveRound >= completedRounds && !confirmedMoves.containsKey(moveRound)) // Confirm round
			{
				// Cancel timeout and reset it
				if(timeoutOnConfirmWait != null && !timeoutOnConfirmWait.isDone())
					timeoutOnConfirmWait.cancel(false);

				// Reset variables for new rounds/requests
				timeoutOnConfirmWait = null;
				previousAcceptedMove = null;
				if(moveRound == (int)maxBallotID)
					maxBallotID = (int)maxBallotID + 1;

				// Execute processing of messages
				processConfirmedMovesForDelivery(move);
				proposer.notifyRound(false, playerMove.equals(move) && !isProposer);
			}
		}

		// If timed out due to not receiving an answer after a certain time, we signal our proposer to try again for the round
		private void timedOut()
		{
			// Do nothing as there is no active proposer that can handle anything
			if(!proposer.keepRunning)
				return;

			proposer.notifyRound(true, false);
		}

		private void shutdown()
		{
			this.interrupt();
			// Cancel timeouts
			if(timeoutOnConfirmWait != null && !timeoutOnConfirmWait.isDone())
				timeoutOnConfirmWait.cancel(true);
			timeoutOnConfirmWait = null;
			if(timeoutOnAcceptWait != null && !timeoutOnAcceptWait.isDone())
				timeoutOnAcceptWait.cancel(true);
			timeoutOnAcceptWait = null;
		}
	}

	// Provides synchronous execution of paxos phases to receive a majority of accepts for a particular value
	// and asynchronous execution of the end phase after.
	private class Proposer implements Runnable {

		// Used for operations on proposer
		final BlockingQueue<PaxosRequest> queue = new ArrayBlockingQueue<>(groupLength*10);
		PlayerMove curVal;
		Thread thirdPhase = null;

		// Determines when paxos rounds are initiated and when they can try again in case of failure
		final Lock lock = new ReentrantLock(true);
		final Condition condition = lock.newCondition();
		boolean executePaxosRound = true;
		volatile boolean keepRunning = true;

		// Timeouts utilities for failure handling
		final ExecutorService executor = Executors.newSingleThreadExecutor();
		Future<Boolean> proposeFuture = null;
		Future<Boolean> acceptFuture = null;

		// Intended to be called synchronously inside broadcastToMsg in Paxos
		public void run()
		{
			keepRunning = true;
			while(keepRunning)
			{
				waitOnCorrectRound();
				try
				{
					this.lock.lock();
					startPaxosRound();
				}
				finally
				{
					this.lock.unlock();
				}
			}
		}

		// Blocks while we can't execute a paxos round
		private void waitOnCorrectRound()
		{
			this.lock.lock();
			try
			{
				if(!executePaxosRound)
				{
					logger.log(Level.INFO, "PROPOSER: Blocked waiting for a signal to propose again.");
					if(!condition.await(timeoutDuration, TimeUnit.MILLISECONDS))
						setPlayerMove(acceptor.maxBallotID + (0.001*processID), playerMove.playerNum(), playerMove.cmd(), false);
					logger.log(Level.INFO, "PROPOSER: Unblocked and starting a proposal.");
				}
				else
					curVal = playerMove;
			}
			catch (Exception e) { }
			finally
			{
				this.lock.unlock();
			}
		}

		// Will unblock the proposer to try a new round when called and if necessary, with another value for failure cases
		void notifyRound(boolean fromFailure, boolean receivedALMoveFromAcceptor)
		{
			this.lock.lock();
			try
			{
				if(!executePaxosRound)
				{
					executePaxosRound = true;
					double updatedBallotID = fromFailure ? acceptor.maxBallotID + (0.001*processID) : ballotID;
					setPlayerMove(updatedBallotID, playerMove.playerNum(), playerMove.cmd(), false);
					if(keepRunning)
					{
						condition.signal();
						if(receivedALMoveFromAcceptor)
							keepRunning = false;
					}
				}
			}
			finally
			{
				this.lock.unlock();
			}
		}

		private void startPaxosRound()
		{
			// Empty queue as previous round messages might still be in place
			queue.clear();

			if(!keepRunning)
				return;

			curVal = playerMove;
			executePaxosRound = false;

			// Trying to become the leader for this round
			boolean isLeader = false;
			try
			{
				proposeFuture = executor.submit(this::propose);

				// Try to get the result within 2 seconds or timeout
				isLeader = proposeFuture.get(timeoutDuration, TimeUnit.MILLISECONDS);
			}
			catch(Exception e)
			{
				executePaxosRound = true;
				proposeFuture.cancel(true);
				logger.log(Level.WARNING, "PROPOSER: Propose error or timeout. Will try again.");
			}

			// If we didn't succeed in being the leader, we give up for the round and wait until we can try again
			if(!isLeader)
				return;

			// Try to get the current value accepted by majority
			boolean isAccepted = false;
			try
			{
				acceptFuture = executor.submit(this::accept);

				// Try to get the result within 2 seconds or timeout
				isAccepted = acceptFuture.get(timeoutDuration, TimeUnit.MILLISECONDS);
			}
			catch(Exception e)
			{
				executePaxosRound = true;
				acceptFuture.cancel(true);
				logger.log(Level.WARNING, "PROPOSER: Accept error or timeout. Will try again.");
			}

			if(!isAccepted)
				return;

			// Then at this point, the current value was accepted with majority
			thirdPhase = Thread.startVirtualThread(() -> confirm(new PlayerMove(curVal.ballotID(), curVal.playerNum(), curVal.cmd(), curVal.uniqueHash())));
			if(keepRunning)
				keepRunning = !curVal.equals(playerMove);
		}

		private boolean propose()
		{
			// Check if propose to own acceptor works, otherwise give up
			int promised = 1;
			int refused = 0;
			if(!acceptor.handlePropose(curVal))
				return false;

			PlayerMove initialMove = new PlayerMove(curVal.ballotID(), curVal.playerNum(), curVal.cmd(), curVal.uniqueHash());

			// Send the request with the history for any behind processes and clear it
			HashSet<PlayerMove> historyCopy = copyHistoryAndClear();
			PaxosRequest sendRequest = new PaxosRequest(PaxosRequestType.PROPOSE, initialMove, false, myProcess, historyCopy, initialMove.round());
			gcl.multicastMsg(sendRequest, otherProcesses);

			// Verifies for no duplicates coming from acceptor previously
			if(historyCopy.contains(initialMove))
			{
				if(initialMove.equals(playerMove)) // If own move, then stop iteration
					keepRunning = false;
				return false; // Stop proposal for this value and restart a new round
			}

			// This is called after as there is the history to send no matter what but it is not a real proposal if fails
			failCheck.checkFailure(FailCheck.FailureType.AFTERSENDPROPOSE);

			while(promised < majorityCount && refused < majorityCount)
			{
				try
				{
					// Handle request and make sure it is a Promise and not an AcceptAck and matches current round
					PaxosRequest request = queue.take();
					if(request.requestType() != PaxosRequestType.PROMISE  || request.round() != curVal.round())
						continue;

					if(request.isSuccess())
					{
						++promised;
						PlayerMove requestMove = request.playerMove();

						// If we receive values from other acceptors, if multiple, take one with largest ballotID, else take the one we received
						if(requestMove != null && (curVal.equals(initialMove) || requestMove.ballotID() > curVal.ballotID()))
							curVal = requestMove;
					}
					else
					{
						++refused;

						// The acceptor may send a history if it notices that we are in a round behind it
						if(request.history() != null)
						{
							addMissingConfirmedMoves(request.history());
							processConfirmedMovesForDelivery(null);
							if(request.history().contains(playerMove)) // Check to avoid duplicates
								keepRunning = false;

							return false;
						}
					}
				}
				catch (InterruptedException e)
				{
					logger.log(Level.WARNING, "PROPOSER: Interrupted Exception while waiting for promise acknowledgements.");
					return false;
				}
				catch(Exception ex) { }
			}

			if(promised < majorityCount)
				return false;

			curVal = new PlayerMove(initialMove.ballotID(), curVal.playerNum(), curVal.cmd(), curVal.uniqueHash());
			failCheck.checkFailure(FailCheck.FailureType.AFTERBECOMINGLEADER);
			return true;
		}

		private boolean accept()
		{
			// If own acceptor doesn't accept it, then simply assume another proposer has leadership
			if(!acceptor.handleAcceptMove(curVal))
				return false;

			int accepted = 1;
			int denied = 0;

			PaxosRequest sendRequest = new PaxosRequest(PaxosRequestType.ACCEPT, curVal, false, myProcess, null, curVal.round());
			gcl.multicastMsg(sendRequest, otherProcesses);

			while(accepted < majorityCount && denied < majorityCount)
			{
				try
				{
					// Handle request and make sure it is an AcceptAck and not a Promise
					logger.log(Level.INFO, myProcess+"PROPOSER-WAITING-FOR-ACCEPTACK");
					PaxosRequest request = queue.take();
					if(request.requestType() != PaxosRequestType.ACCEPTACK || request.round() != curVal.round())
						continue;

					if(request.isSuccess())
					{
						++accepted;
					}
					else
					{
						++denied;
					}
				}
				catch (InterruptedException e)
				{
					logger.log(Level.WARNING, "PROPOSER: Interrupted Exception while waiting for acceptAck acknowledgements.");
					return false;
				}
				catch(Exception ex) { }
			}

			if(accepted < majorityCount)
				return false;

			failCheck.checkFailure(FailCheck.FailureType.AFTERVALUEACCEPT);
			return true;
		}

		private void confirm(PlayerMove val)
		{
			PaxosRequest sendRequest = new PaxosRequest(PaxosRequestType.CONFIRM, val, true, myProcess, null, val.round());
			gcl.multicastMsg(sendRequest, otherProcesses);
			acceptor.handleConfirmMove(val, true);
		}
	}
}
