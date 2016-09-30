from twisted.internet.protocol import Factory
from twisted.internet.protocol import ClientFactory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
import time, datetime, re, sys,urllib, json
import project_config
import logging


API_KEY=project_config.API_KEY


if len(sys.argv)!=2:
        logname=""
else:
        logname=sys.argv[1]+".log"
        
def log(msg):
        logging.basicConfig(format='<%(asctime)s> %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',filename=logname, filemode='w', level=logging.DEBUG)
        logging.info(': %s',msg)
        

match_ports= {
        "Alford":12250,
        "Powell": 12251,
        "Parker": 12252,
        "Bolden": 12253,
        "Hamilton": 12254
        }

border = {
        "Alford": ["Powell" , "Parker"],
        "Powell": ["Alford", "Bolden"],
        "Parker":["Alford", "Hamilton"],
        "Hamilton":["Parker"],
        "Bolden":["Powell"]
        }

class ServerProtocol(LineReceiver):
	def __init__(self, factory):
		self.factory = factory
		self.ClientName = None
                self.serverName=self.factory.name
                
	
	def connectionMade(self):
		self.sendLine("Connection Succesful with server %s" %(self.serverName,) ) #delete
                log("Connection Succesful with client")

	def connectionLost(self, reason):
		if self.ClientName in self.factory.users:
                        print "%s left the server." %(self.ClientName)
			del self.factory.users[self.ClientName]
                        log("Connection with client "+self.ClientName+" Lost")
                else:
                        log("Connection with client Lost")
		

	def lineReceived(self, line):
		splitter=line.split()
               
                log("INPUT--> "+" "+ line)
                if splitter[0]=="IAMAT":
                        if len(splitter)!=4:
                                self.sendLine("? %s" %(line))
                                log("Bad format IAMAT")
                                return

                        self.handle_IAMAT(splitter)
                        
                elif splitter[0]=="WHATSAT":
                        if len(splitter)!=4:
                                self.sendLine("? %s" %(line))
                                log("Bad format WHATSAT")
                                return

                        self.handle_WHATSAT(splitter)

                elif splitter[0]=="AT":                
                        self.handle_AT(splitter)
                elif splitter[0]=="ASK":
                        self.handle_ASK(splitter)
                else:
                        self.sendLine ("? %s" % (line))
                        log("OUTPUT--> ? "+ line)

        def handle_IAMAT(self, line):
                                
                current_time=time.time()
                self.ClientName=line[1]
                location=line[2]
                client_time=line[3]
                time_diff= current_time - float(client_time)
               # self.sendLine("Clinet Time= %f" %(current_time))
                answer= "AT " + self.serverName+ " " + str(time_diff)+" "+ self.ClientName+" "+location+" "+client_time
                self.sendLine(answer)
                log("OUTPUT-->" +" "+answer)
                self.factory.users[self.ClientName]= answer #store the users of each client
                
                #print "IAMAT-users: (local) "
                #self.print_users(self.factory.users)

                #Connect to peers and propagate info
                info= answer+" "+ self.serverName
                self.connectToPeers(info, "")
                log("Propagating AT information to peer servers")

        def handle_AT(self,line):
                if len(line)!=7:
                        log( "Bad format AT command") #should never be called unless used by a client who makes a mistake
                        return

                origin_ServerName= line[1]
                time_diff= line[2]
                self.ClientName=line[3]
                location=line[4]
                client_time=line[5]
                #self.factory.users[self.ClientName]= location+" "+ client_time

                #construct answer to pass to its own peers
                answer= "AT" +" "+ origin_ServerName +" "+ time_diff+ " " + self.ClientName+ " "+ location+ " "+ client_time
                self.factory.users[self.ClientName]=answer
                prevServer=line[6]
                info=answer+ " "+self.serverName
                self.connectToPeers(info, prevServer)
                log("Propagating At information to peers")

                #Tese two lines are just testing
                #info= line[3]+" "+line[4]+ " "+line[5] #name plus location as pased from IAMAT -answer (splitter)
                #self.sendLine(info)


                #print "AT-users: (other servers) "
                #self.print_users(self.factory.users)

        def handle_WHATSAT(self,line):
                
                UserToLook=line[1]
                radius=line[2]
                upper_b=line[3]

                if (radius.isdigit()==False) or (upper_b.isdigit()==False):
                        self.sendLine("Radius and Quantity need to be numbers")
                        return
                
                        
                if ((int(radius)>50) or (int(upper_b) >20)):
                        self.sendLine( "Usage: radius<50 and upper<20")
                        log("BAD FORMAT WHATSAT-->Radius>50 or Quantity>20")
                        return
                        
                all_users=self.factory.users
                         
                if (all_users.has_key(UserToLook)==True):
                        answer= all_users.get(UserToLook)
                        #self.sendLine(answer)
                        splitter=answer.split()
                        location=splitter[4]
                        #print location
                        if len(location)!=21:
                                self.sendLine( "Invalid Lat-Long pair, try again")
                                log("Invalid Lat-Long pair")
                                return
                        lat=location[:10]
                        longit=location[10:]
                        LatLong= lat+","+longit
                        URL='https://maps.googleapis.com/maps/api/place/nearbysearch/json?location=%s&radius=%s&sensor=false&key=%s' %(LatLong, radius, API_KEY)

                        page= urllib.urlopen(URL)
                        places=page.read()
                        j_string=json.loads(places)
                        results= len(j_string['results'])
                        if results > int(upper_b):
                                j_string['results']=j_string['results'][:int(upper_b)]

                        places=json.dumps(j_string, sort_keys=True,indent=4, separators=(',', ': '))
                        final_answer= answer+"\n"+places
                        self.sendLine(final_answer)
                        log("OUTPUT--> WHATSAT info (ommitted here)")
                else:
                        self.sendLine("No such user on the servers")
                        log("WHATSAT failed--No such user on the servers")

                
                
                
        def handle_ASK(self,line):
                #idea is to ask servers for users if the server has just come online
                log("Asked by booting peer to send user data")
                for user in self.factory.users:
                        self.sendLine( "STORE"+" " +user + " "+ self.factory.users[user])

        
        def connectToPeers(self, info, prevServer):
#need the prevServer to avoid infinite loops. Now a node (server) will not send the information back again from where it recieved it but only to his other peers

                for peer in border[self.serverName]:
                        peer_port=match_ports[peer]
                        if peer != prevServer:
                                reactor.connectTCP("lnxsrv.seas.ucla.edu",peer_port, ClientFactory(info,None) )
                                log("Passed information to "+peer)

        #helper function
        def print_users(self, names):
                print "Known Users are:"
                log("Helper function print_users called")
                for key, value in names.iteritems():
                        print key,value


                        

class ServerFactory(Factory):
	def __init__(self):
		self.users = {} #Users connected to Server 
                self.name= sys.argv[1] #Name of server
                
                log("Server"+ " "+self.name+" "+"came online")
                for peer in border[self.name]:
                        peer_port=match_ports[peer]
                        reactor.connectTCP("lnxsrv.seas.ucla.edu",peer_port,ClientFactory("ASK",self.users))
                log("Retrieved information from peer servers")

	def buildProtocol(self, addr):
		return ServerProtocol(self)


# a client protocol

class ClientProtocol(LineReceiver):


        def __init__(self,factory):
                self.factory=factory

        
        def connectionMade(self):
                self.sendLine(self.factory.answer)
                log("Connected with peer server")


        def lineReceived(self, line) :
                #print " %s" %(line)
                log("INPUT-->"+ line)
                splitter=line.split()
                if splitter[0]=="STORE":
                        self.handle_STORE(splitter)
                               
        def connectionLost(self,reason):
                #print "Connection Lost"
                log("Connection with Server Lost")


        def handle_STORE(self,line):
                #print "STORE RECORDED"
                user=line[1]
                details_arr=line[2:]
                details=' '.join(details_arr) #make array to string
               # print "User %s, details: %s" %(user, details)
                log("Upon booting added user "+user+" with user details: \n"+details)
                self.factory.users[user]=details
        
        

class ClientFactory(ClientFactory):
        def __init__(self, answer, users):
                self.answer=answer
                if (users !=None ):
                        #print ("Users passed to Client")
                        self.users=users

        def buildProtocol(self, addr):
                return ClientProtocol(self)
                






        
def main():
        if( (len(sys.argv)!=2) or (match_ports.has_key(sys.argv[1]) ==False ) ):
                print "Usage: python proxyHerd.py <Server Name>"
                log ("Unable to Initiate Server. Wrong number of arguments or invalid Server Name")
                exit(0)
        

        port= match_ports[sys.argv[1]]
        reactor.listenTCP(port, ServerFactory())
        reactor.run()

if __name__=='__main__':
        main()
        
