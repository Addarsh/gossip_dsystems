/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

#define GOSSIP_B 2

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	
	/*TODO: are these lines needed?*/
	//int id = *(int*)(&memberNode->addr.addr);
	//int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/* Send either JOINREQ or JOINREP message according
   to the message type argument */
void MP1Node::sendJoinMessge (Address *dest, enum MsgTypes mtype){
	#ifdef DEBUGLOG
    static char s[1024];
	#endif
	
	size_t msgsize = sizeof(MessageHdr) + sizeof(char)*6 + sizeof(long) + 1;
	MessageHdr *msg = (MessageHdr *) malloc(msgsize);

	// create JOINREQ message: format of data is {struct Address myaddr}
	msg->msgType = mtype;
	memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
	memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

	#ifdef DEBUGLOG
	if (mtype == JOINREQ){
		sprintf(s, "Trying to join...");
		log->LOG(&memberNode->addr, s);
	}
	#endif

	// send JOINREQ or JOINGREP message to another member
	emulNet->ENsend(&memberNode->addr, dest, (char *)msg, msgsize);
  free(msg);
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
		// I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
		log->LOG(&memberNode->addr, "Starting up group...");
#endif
		memberNode->inGroup = true;
		addSelfToMemberList ();
	}
	else {
		sendJoinMessge (joinaddr, JOINREQ);
	}
					
	return 1;
}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){	
	/* Free all elements in the recv queue */	
  while ( !memberNode->mp1q.empty() ) {
		void *ptr = memberNode->mp1q.front().elt;
    memberNode->mp1q.pop();
		free(ptr);
	}

	return 1;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }	

		// Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    	free (ptr); /*TODO: confirm ptr to be freed */
		}
    return;
}

/* Returns true if the given member is
   the coordinator node and false otherwise */
bool MP1Node::isCoordinator(){
	char* addr = memberNode->addr.addr;
	int id = *((int *)addr);
	short port = *((short *) (&addr[4]));
	return (id == 1 && port == 0);
}

/* Returns type of message contained in the data */
enum MsgTypes MP1Node::getDataType (char *data, int size){
	assert (size >= (int)(sizeof (MessageHdr)));	
	MessageHdr *jdata = (MessageHdr *)data;
	
	return jdata->msgType;
}

/* Update membership list of member node 
   according to given data */
void MP1Node::updateMemberShip (char *data, int size){	
	MessageHdr *mhdr = (MessageHdr *)data;
	MemberListEntry *mentry = (MemberListEntry *)(mhdr + 1); 	

	/*Pointer to member list table of sender*/
	vector<MemberListEntry>::iterator myit;
	int numelems = (size - sizeof(MessageHdr))/sizeof(MemberListEntry);

	bool found = false;	
	for(int i = 0; i < numelems; i++){
		for (myit = memberNode->memberList.begin(); myit != memberNode->memberList.end (); myit++){
			if ((myit->getid() == mentry[i].getid()) &&	(myit->getport() == mentry[i].getport())){
				if (mentry[i].getheartbeat() > myit->getheartbeat()){	
					/* Address has been matched, update heartbeat
						 and time stamp fields */
					myit->setheartbeat (mentry[i].getheartbeat ());
					myit->settimestamp (par->getcurrtime());
					cout << "LOGGING time: " << par->getcurrtime () << endl;
				}
				found = true;
				break;
			}
		}
		if (!found){
			/* New entry, add to my list */
			mentry[i].settimestamp (par->getcurrtime());
			memberNode->memberList.push_back (mentry[i]);
			
			/* Log the addition */
			Address newaddr;
			int id = mentry[i].getid();
			short port = mentry[i].getport();
			memcpy (newaddr.addr, &id, sizeof(int));
			memcpy (&newaddr.addr[4], &port, sizeof(port));
			log->logNodeAdd(&memberNode->addr, &newaddr);
		}
	}
}

/* Add self node value to member list */
void MP1Node::addSelfToMemberList (){
	MemberListEntry entry;
	char *addr = memberNode->addr.addr;
	entry.setid (*(int*)(addr));
	entry.setport (*(short*)(&addr[4]));
	entry.setheartbeat (0);
	entry.settimestamp (par->getcurrtime());		

	memberNode->memberList.push_back (entry);
}


/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	bool res = false;
	switch (getDataType(data, size))
		{
			case JOINREQ:
				if (isCoordinator()){ /* Send JOINREP response */
						Address dest;
						char *daddr = (char*)((MessageHdr *)data + 1);
						memcpy (dest.addr, daddr, 6*sizeof (char));
						cout << "Got JOINREQ from: " << dest.getAddress() << endl;
						sendJoinMessge (&dest, JOINREP);
						
						/*Add to member list */
						MemberListEntry entry;
						entry.setid (*(int *)dest.addr);
						entry.setport (*(short *)(&dest.addr[4]));
						entry.setheartbeat (0);
						entry.settimestamp (par->getcurrtime());					
	
						memberNode->memberList.push_back (entry);
						res = true;
					}
				break;
			case JOINREP:
				if (!isCoordinator ()){ /* Add to group */
						cout << "Got JOINREP from coordinator. My addr: " << memberNode->addr.getAddress() << endl;
						
						memberNode->inGroup = true;	
						/* Create new Member list entry for self*/
						addSelfToMemberList ();
						res = true;
					}	
				break;

			case UPDATE:
				updateMemberShip (data, size);
				res = true;
				break;
			
			default:
				break;
		}
	
	return res;
}

/* Remove timed out data in membership list if, any.
   Note that timeout is after TREMOVE and TFAIL time period. */
void MP1Node::removeOldMembers() {
	
	for(vector<MemberListEntry>::iterator it = memberNode->memberList.begin()+1; it != memberNode->memberList.end (); it++){
		long currtime = par->getcurrtime();
		long lasttime = it->gettimestamp ();
		if ((currtime - lasttime) >= (TREMOVE + TFAIL)){
			//Remove from member list
			vector<MemberListEntry>::iterator oldit = it;
			it++;
	
			/* Log removal */
			Address oldaddr;
			int id = oldit->getid();
			short port = oldit->getport();
			memcpy (oldaddr.addr, &id, sizeof(int));
			memcpy (&oldaddr.addr[4], &port, sizeof(short));
			log->logNodeRemove(&memberNode->addr, &oldaddr);
			
			memberNode->memberList.erase(oldit);
			continue;		
		}	
	}
}

/* Disseminate current membership 
   to other nodes using gossip */
void MP1Node::spreadGossip(){
	set<int> npos;
	int listsz = memberNode->memberList.size(); 	
	if (listsz <= 1)
		return;

	cout << "from: " << memberNode->addr.getAddress() << " ,listsz: " << listsz << endl;
	/* Find all neighbors to spread to */
	int cnt = 0;
	while (cnt < GOSSIP_B && cnt < (listsz-1)){
		int r = rand() % (listsz-1) + 1;
		if (npos.find(r) == npos.end()){
			npos.insert(r);
			cout << "	inserted: "<< r << endl;
			cnt++;
		}
	}

	/*Update heartbeart and time stamp of self*/
	cout << "		My addr is:  " << memberNode->memberList.begin()->getid() << endl;
	vector<MemberListEntry>::iterator mentry = memberNode->memberList.begin();
	mentry->setheartbeat(mentry->getheartbeat()+1);
	mentry->settimestamp(par->getcurrtime());
	
	set<int>::iterator setit = npos.begin();
	cnt = 0;
	for(vector<MemberListEntry>::iterator it = memberNode->memberList.begin() + 1; it != memberNode->memberList.end(); it++){
		if (setit == npos.end())
			break;
		if (cnt == *setit){
			/* Send membership list message to the peer */
			int msgSize = sizeof(MessageHdr) + listsz*sizeof(MemberListEntry);
			char *msg = (char*) malloc (msgSize);
			((MessageHdr*)msg)->msgType = UPDATE;
			MemberListEntry *data = (MemberListEntry *)((MessageHdr *)msg + 1);
			copy (memberNode->memberList.begin(), memberNode->memberList.end(), data);	

			/* Create dest address */
			Address dest;
			int id = it->getid();
			short port = it->getport();
			memcpy(dest.addr,&id,sizeof(int));
			memcpy(&dest.addr[4],&port,sizeof(short));
			
			emulNet->ENsend (&memberNode->addr, &dest, msg, msgSize);
			
			free (msg);
			setit++;
		}
		cnt++;
	}
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {	
	//removeOldMembers();

	spreadGossip();
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
