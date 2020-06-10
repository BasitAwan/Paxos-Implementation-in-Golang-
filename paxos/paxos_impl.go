/*
17030005 - Huda Tariq (partner 1)
20100153 - Muhammed Basit Iqbal Awan (partner 2)
*/

package paxos

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"paxosapp/rpc/paxosrpc"
	"sync"
	"time"
)

var PROPOSE_TIMEOUT = 15 * time.Second

type paxosNode struct {
	myHostPort       string //the hostport string of the new node
	srvId            int
	numNodes         int
	numRetries       int                    //# of times required for connection with other node
	quorum           int                    // majority vote needed to reach consesus
	clientMap        map[int]*rpc.Client    //a map from all client IDs to the paxo node
	hostMap          map[int]string         // a map from all node IDs to their hostports
	getProposal      map[string]int         // mapping from key to proposalNmber
	accepted         map[string]interface{} //mapping from key to value for proposal.
	acceptedN        map[string]int         //keep the key with highest value of N.
	acceptedProposal map[string]int         //keep the key with accepted value of proposal.
	acceptedLock     *sync.Mutex            //lock for key to accepted N
	clientLock       *sync.Mutex            //lock to exclude any race conditions
	getProposalLock  *sync.Mutex            //lock for key to value for the proposal
	acceptedNLock    *sync.Mutex            //lock with key mapping to highest value of N accepted
	acceptedPLock    *sync.Mutex            //lock with key mapping to accepted proposal.
}

// Desc:
// NewPaxosNode creates a new PaxosNode. This function should return only when
// all nodes have joined the ring, and should return a non-nil error if this node
// could not be started in spite of dialing any other nodes numRetries times.
//
// Params:
// myHostPort: the hostport string of this new node. We use tcp in this project.
//			   	Note: Please listen to this port rather than hostMap[srvId]
// hostMap: a map from all node IDs to their hostports.
//				Note: Please connect to hostMap[srvId] rather than myHostPort
//				when this node try to make rpc call to itself.
// numNodes: the number of nodes in the ring
// numRetries: if we can't connect with some nodes in hostMap after numRetries attempts, an error should be returned
// replace: a flag which indicates whether this node is a replacement for a node which failed.
var LOGF *log.Logger

func NewPaxosNode(myHostPort string, hostMap map[int]string, numNodes, srvId, numRetries int, replace bool) (PaxosNode, error) {
	paxoNode := &paxosNode{
		//a single instane of paxonode, needed for intialization
		myHostPort:       myHostPort,
		srvId:            srvId,
		numNodes:         numNodes,
		quorum:           numNodes/2 + 1,
		clientMap:        make(map[int]*rpc.Client),
		hostMap:          hostMap,
		numRetries:       numRetries,
		getProposal:      make(map[string]int),
		getProposalLock:  &sync.Mutex{},
		accepted:         make(map[string]interface{}),
		acceptedLock:     &sync.Mutex{},
		clientLock:       &sync.Mutex{},
		acceptedN:        make(map[string]int),
		acceptedProposal: make(map[string]int),
		acceptedPLock:    &sync.Mutex{},
		acceptedNLock:    &sync.Mutex{},
	}

	//Register the node to make RPC calls.
	rpc.RegisterName("PaxosNode", paxosrpc.Wrap(paxoNode))
	rpc.HandleHTTP()
	//Listen on Hostport for tcp
	ln, err := net.Listen("tcp", myHostPort)
	if err != nil {
		return nil, err
	}
	go http.Serve(ln, nil)
	const (
		name = "log.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
	}
	defer file.Close()

	LOGF = log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	//establish connection with all other nodes in the ring
	paxoNode.clientLock.Lock()
	count := 0
	for Id, hostPort := range hostMap {
		cli, err := rpc.DialHTTP("tcp", hostPort)
		//contact a paxos node and retry for a certain number of times
		for err != nil && count < numRetries {
			count++
			time.Sleep(1 * time.Second)
			cli, err = rpc.DialHTTP("tcp", hostPort)
		}
		//save incoming connections of other nodes.
		paxoNode.clientMap[Id] = cli
		if count == numRetries {
			//exit with error if connection not established after retries.
			return nil, errors.New("Can't connect to node " + hostPort)
		}
	}
	// a node is crashed. Then, to take its place, a replacement node is started using the flag replace.
	if replace {
		errorChan := make(chan error)
		ReplaceServerReplyChan := make(chan struct{})
		args := &paxosrpc.ReplaceServerArgs{
			SrvID:    srvId,
			Hostport: myHostPort,
		}
		go paxoNode.replaceServerNode(args, ReplaceServerReplyChan, errorChan)
		for _, nodes := range paxoNode.clientMap {
			//retrieve data from one of the nodes.
			paxoNode.retrieveData(nodes, errorChan)
		}
	}
	paxoNode.clientLock.Unlock()
	return paxoNode, nil

}

// get node data
func (pn *paxosNode) retrieveData(clients *rpc.Client, errChan chan error) {
	args := &paxosrpc.ReplaceCatchupArgs{} //prepare ReplaceCatchupArgs RPC to get other nodes data.
	var reply paxosrpc.ReplaceCatchupReply //prepare its reply too
	err := clients.Call("PaxosNode.RecvReplaceCatchup", args, &reply)
	if err != nil {
		errChan <- err
		return
	} else {
		nodeData := make(map[string]uint32)
		json.Unmarshal(reply.Data, &nodeData) //retreive the data recieved from one of the nodes.

		for key, data := range nodeData {
			pn.accepted[key] = data
		}
	}

}

//RPC to handle replacement of servers after crashing
func (pn *paxosNode) replaceServerNode(args *paxosrpc.ReplaceServerArgs, recieve chan struct{}, errChan chan error) {
	for _, client := range pn.clientMap {
		LOGF.Println(pn.clientMap)
		var repReply paxosrpc.ReplaceServerReply
		err := client.Call("PaxosNode.RecvReplaceServer", args, &repReply)
		if err != nil {
		} else {
		}
	}
}

// Desc:
// GetNextProposalNumber generates a proposal number which will be passed to
// Propose. Proposal numbers should not repeat for a key, and for a particular
// <node, key> pair, they should be strictly increasing.
//
// Params:
// args: the key to propose
// reply: the next proposal number for the given key
func (pn *paxosNode) GetNextProposalNumber(args *paxosrpc.ProposalNumberArgs, reply *paxosrpc.ProposalNumberReply) error {
	//save key in a variable and sync it until proposal number not generated.
	proposedkey := args.Key
	pn.getProposalLock.Lock()
	defer pn.getProposalLock.Unlock()
	//Check value for proposal number exists or not.
	max, value := pn.getProposal[proposedkey]
	if !value {
		//when there is no value, Incremental server id.
		pn.getProposal[proposedkey] = pn.srvId
		reply.N = pn.getProposal[proposedkey]

	} else {
		//To generate a new proposal number: Increment maxRound & Concatenate with Server Id
		pn.getProposal[proposedkey] = ((max/pn.numNodes)+1)*pn.numNodes + pn.srvId
		reply.N = pn.getProposal[proposedkey]
	}
	return nil
}

// Desc:
// Propose initializes proposing a value for a key, and replies with the
// value that was committed for that key. Propose should not return until
// a value has been committed, or PROPOSE_TIMEOUT seconds have passed.
//
// Params:
// args: the key, value pair to propose together with the proposal number returned by GetNextProposalNumber
// reply: value that was actually committed for the given key
func (pn *paxosNode) Propose(args *paxosrpc.ProposeArgs, reply *paxosrpc.ProposeReply) error {
	errorChan := make(chan error) //error channel for any error during any stage of proposal.
	go func(pn *paxosNode, args *paxosrpc.ProposeArgs, reply *paxosrpc.ProposeReply, errorChan chan error) {
		//Prepare
		prepareReplyChan := make(chan *paxosrpc.PrepareReply)
		prepareArgs := &paxosrpc.PrepareArgs{
			Key:         args.Key,
			N:           args.N,
			RequesterId: pn.srvId,
		}
		// Call Prepare for all other nodes
		go pn.prepare(prepareArgs, prepareReplyChan, errorChan)
		_, accepted_Value := pn.prepareChan(args, prepareReplyChan, errorChan) //Response to Prepare(n)
		//Accept
		args.V = accepted_Value
		acceptArgs := &paxosrpc.AcceptArgs{
			Key:         args.Key,
			N:           args.N,
			V:           args.V,
			RequesterId: pn.srvId,
		}
		acceptReplyChan := make(chan *paxosrpc.AcceptReply)
		go pn.accept(acceptArgs, acceptReplyChan, errorChan) //Respond to accepted value
		//Broadcast accepted value to acceptors to accept a specific value through accept channel
		pn.acceptChan(acceptReplyChan, errorChan)
		//Commit
		commitArgs := &paxosrpc.CommitArgs{
			Key:         args.Key,
			V:           args.V,
			RequesterId: pn.srvId,
		}
		commitReplyChan := make(chan *paxosrpc.CommitReply)
		go pn.commit(commitArgs, commitReplyChan, errorChan) //Commit the final accepted value
		pn.commitChan(commitReplyChan, errorChan)            //wait for the majority to commit for the committed value
		reply.V = args.V
		errorChan <- nil
	}(pn, args, reply, errorChan)
	select {
	case <-time.After(PROPOSE_TIMEOUT):
		return nil
	case <-errorChan:
		_, present := pn.accepted[args.Key] //check for value accepted until PROPOSE_TIMEOUT.

		for {
			if present {
				break
			}
			time.Sleep(PROPOSE_TIMEOUT / 2)
			_, present = pn.accepted[args.Key]

		}
		reply.V = pn.accepted[args.Key] //send back the committed value
		return nil
	}
	return nil
}

func (pn *paxosNode) prepare(args *paxosrpc.PrepareArgs, recieve chan *paxosrpc.PrepareReply, errChan chan error) {
	//Prepare RPC for all the nodes.
	pn.clientLock.Lock()
	for _, paxonode := range pn.clientMap {
		go func(paxonode *rpc.Client) {
			//wait for reply after prepare has been processed for all the nodes.
			var prepareReply *paxosrpc.PrepareReply
			err := paxonode.Call("PaxosNode.RecvPrepare", args, &prepareReply)
			if err != nil {
				errChan <- err
			} else {
				recieve <- prepareReply
			}
		}(paxonode)

	}
	pn.clientLock.Unlock()

}

func (pn *paxosNode) prepareChan(args *paxosrpc.ProposeArgs, recieve chan *paxosrpc.PrepareReply, errChan chan error) (int, interface{}) {
	higestN := args.N
	count := 0
	accepted_Value := args.V
	for {
		response := <-recieve
		if response == nil {
			errChan <- errors.New("Prepare Error")
			return -1, nil
			//check the status of the reponse
		} else if response.Status == paxosrpc.OK {
			count++
			if response.N_a > higestN {
				//find max highest N accepted and its value
				higestN = response.N_a
				accepted_Value = response.V_a
			}
			if count >= pn.numNodes {
				//once majority accepted, send back the highest value of N and corresponding value
				return higestN, accepted_Value

			}
		} else {
			errChan <- errors.New("Prepare Reject")
			return -1, nil
		}

	}
	//Send an error message in case no quorum(majority) is reached.
	if count <= pn.numNodes {
		errChan <- errors.New("Prepare unsuccesful in recieving majority vote")
		return -1, nil
	}
	return higestN, accepted_Value

}

func (pn *paxosNode) accept(args *paxosrpc.AcceptArgs, recieve chan *paxosrpc.AcceptReply, errChan chan error) {

	//Accept RPC for all the nodes.
	pn.clientLock.Lock()

	for _, paxonode := range pn.clientMap {
		go func(paxonode *rpc.Client) {
			//wait for accepted reply from RecvAccept
			var acceptReply *paxosrpc.AcceptReply
			err := paxonode.Call("PaxosNode.RecvAccept", args, &acceptReply)
			if err != nil {
				errChan <- err
			} else {
				recieve <- acceptReply
			}
		}(paxonode)

	}
	pn.clientLock.Unlock()

}
func (pn *paxosNode) acceptChan(recieve chan *paxosrpc.AcceptReply, errChan chan error) {
	count := 0
	for { //check for all nodes, the value has been accepted.
		response := <-recieve
		if response == nil {
			errChan <- errors.New("Accept Error")
			return
		} else if response.Status == paxosrpc.OK {
			//check until the count has exceed the majority.
			count++
			if count >= pn.numNodes {
				return

			}
		} else {
			errChan <- errors.New("Accept Reject")
			return
		}

	}

}

func (pn *paxosNode) commit(args *paxosrpc.CommitArgs, recieve chan *paxosrpc.CommitReply, errChan chan error) {

	//SEnd Commit RPC for all the nodes.
	pn.clientLock.Lock()
	for _, paxonode := range pn.clientMap {
		go func(paxonode *rpc.Client) {
			//wait for all the nodes to get a value committed
			var commitReply *paxosrpc.CommitReply
			err := paxonode.Call("PaxosNode.RecvCommit", args, &commitReply)
			if err != nil {
				errChan <- err
			} else {
				//send back the reply on recieve channel
				recieve <- commitReply
			}
		}(paxonode)

	}
	pn.clientLock.Unlock()

}
func (pn *paxosNode) commitChan(recieve chan *paxosrpc.CommitReply, errChan chan error) {
	count := 0
	for {
		response := <-recieve
		if response == nil {
			//check if response is nil, then send back an error, as no value committed.
			errChan <- errors.New("Commit Error")
			return
		} else {
			count++
			if count > pn.quorum { //if majority has commited, send back the value on commit channel
				return

			}
		}
	}

}

// Desc:
// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the value and status for this lookup of the given key
func (pn *paxosNode) GetValue(args *paxosrpc.GetValueArgs, reply *paxosrpc.GetValueReply) error {
	//the value for a given key. This function should not block.
	//If a key is not found, the Status in reply should be KeyNotFound.
	pn.acceptedLock.Lock()
	getReply, value := pn.accepted[args.Key]
	if value {
		//Incase key existed in the existing accepted storage, return it with value for the key.
		reply.Status = paxosrpc.KeyFound
		reply.V = getReply
	} else {
		reply.Status = paxosrpc.KeyNotFound
	}
	pn.acceptedLock.Unlock()
	return nil
}

// Desc:
// Receive a Prepare message from another Paxos Node. The message contains
// the key whose value is being proposed by the node sending the prepare
// message. This function should respond with Status OK if the prepare is
// accepted and Reject otherwise.
//
// Params:
// args: the Prepare Message, you must include RequesterId when you call this API
// reply: the Prepare Reply Message
func (pn *paxosNode) RecvPrepare(args *paxosrpc.PrepareArgs, reply *paxosrpc.PrepareReply) error {

	pn.acceptedNLock.Lock()
	highestProposal, valueExists := pn.acceptedN[args.Key]
	if !valueExists || args.N > highestProposal {
		//if incoming key value is higher than proposed one, or it did not exist before hand
		pn.acceptedN[args.Key] = args.N //store it
		reply.Status = paxosrpc.OK
		pnNum, pnValue := pn.acceptedProposal[args.Key] //Lookup for proposal for given key
		//N, _ := strconv.ParseInt(pnNum, 0, 64)
		accNum, accValue := pn.accepted[args.Key] //Lookup for accepted value for that proposal's given key
		if pnValue && accValue {                  //Send the prepared key, value with look up values.
			reply.N_a = pnNum
			reply.V_a = accNum

		} else {
			//No proposal, value existent for given key
			reply.N_a = -1
			reply.V_a = nil
		}
	} else {
		reply.Status = paxosrpc.Reject
	}
	pn.acceptedNLock.Unlock()
	return nil

}

// Desc:
// Receive an Accept message from another Paxos Node. The message contains
// the key whose value is being proposed by the node sending the accept
// message. This function should respond with Status OK if the prepare is
// accepted and Reject otherwise.
//
// Params:
// args: the Please Accept Message, you must include RequesterId when you call this API
// reply: the Accept Reply Message
func (pn *paxosNode) RecvAccept(args *paxosrpc.AcceptArgs, reply *paxosrpc.AcceptReply) error {

	pn.acceptedNLock.Lock()
	proposalAccepted, keyExist := pn.acceptedN[args.Key]
	if keyExist && args.N >= proposalAccepted { //In case of any accepted proposal later than proposal sent
		//If any acceptedValues returned, replace value with acceptedValue for highest acceptedProposal
		pn.acceptedProposal[args.Key] = args.N
		pn.accepted[args.Key] = args.V
		reply.Status = paxosrpc.OK
	} else {

		reply.Status = paxosrpc.Reject //otherwise Reject.
	}

	pn.acceptedNLock.Unlock()
	return nil
}

// Desc:
// Receive a Commit message from another Paxos Node. The message contains
// the key whose value was proposed by the node sending the commit
// message.
//
// Params:
// args: the Commit Message, you must include RequesterId when you call this API
// reply: the Commit Reply Message
func (pn *paxosNode) RecvCommit(args *paxosrpc.CommitArgs, reply *paxosrpc.CommitReply) error {
	pn.acceptedLock.Lock()
	pn.accepted[args.Key] = args.V
	pn.acceptedLock.Unlock()
	return nil
}

// Desc:
// Notify another node of a replacement server which has started up. The
// message contains the Server ID of the node being replaced, and the
// hostport of the replacement node
//
// Params:
// args: the id and the hostport of the server being replaced
// reply: no use
func (pn *paxosNode) RecvReplaceServer(args *paxosrpc.ReplaceServerArgs, reply *paxosrpc.ReplaceServerReply) error {
	//establish connection until no.of tries with the replaced server hostport and id for each client node
	pn.clientLock.Lock()
	count := 0
	for count < pn.numRetries {
		count++
		client, err := rpc.DialHTTP("tcp", args.Hostport)

		if err != nil {
			time.Sleep(time.Duration(1) * time.Second) //wait until complete retries for server to start up
		} else {
			pn.clientMap[args.SrvID] = client      //store node for established connection with replaced node
			pn.hostMap[args.SrvID] = args.Hostport //Likewise hostport.
			break
		}
	}
	if count == pn.numRetries {
		errors.New("Unable to establish connection with node " + args.Hostport)
	}
	pn.clientLock.Unlock()

	return nil
}

// Desc:
// Request the value that was agreed upon for a particular round. A node
// receiving this message should reply with the data (as an array of bytes)
// needed to make the replacement server aware of the keys and values
// committed so far.
//
// Params:
// args: no use
// reply: a byte array containing necessary data used by replacement server to recover
func (pn *paxosNode) RecvReplaceCatchup(args *paxosrpc.ReplaceCatchupArgs, reply *paxosrpc.ReplaceCatchupReply) error {
	pn.acceptedLock.Lock()
	data, err := json.Marshal(pn.accepted) //Send back the values and keys committed back to the replacement server
	if err != nil {
		return err
	}
	reply.Data = data
	pn.acceptedLock.Unlock()
	return nil
}