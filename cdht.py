import socket
import sys
import threading
import time
import os
import random
import pickle

lock = threading.Lock()
Transfer_process = False

###########################################################################
###########################################################################
class W_content:
    def __init__(self,content):
        self.w_content = content

class Packet:
    def __init__(self,name, segment_content,filename_int,peer,Transfer_process,seq,rec_seq,segment_length):
        self.name = name
        self.trans_data = segment_content
        self.filename_int = filename_int
        self.peer = peer
        self.Transfer_process = Transfer_process
        self.seq = seq
        self.rec_seq = rec_seq
        self.segment_length = segment_length
        self.rtx = False

def Log(logname, event, time, sequence_number, number_bytes_data, ack_number):
    with open(logname, "a") as logfile:
        logfile.write(f"{event:>8} {time:>14.3f} {sequence_number:>8} {number_bytes_data:>8} {ack_number:>8}   \n")

# File_transfer
# in File transfer part, we still need to create a TCP, UDP SEQ ACK to deal
# with the problem, but we allready built many functions in this programming
# So, Class will be better for us

# TCP message
class Send_File_Requester:
    global predecessor

    # Find the reciever in TCP_Recieve
    def file_request(self,filename_int):
        
        # As required, use TCP to send request message
        file_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # ask my successor
        file_socket.connect( (host,Fix_port + peer_successor[0]) )
        file_socket.send( f"File_request {current_peer} request {filename_int}".encode() )
        # here TCP do not need to consider lost
        file_socket.close
        print(f"File {filename_int} is not stored here.")
        print(f"File request message for {filename_int} has been sent to my successor.")
    
    # Find the reciever in TCP_Recieve
    def file_forward_request(self,predecessor_message):

        file_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        file_socket.connect( (host,Fix_port + peer_successor[0]) )

        file_socket.send( predecessor_message.encode() )
        file_socket.close
        print("File request message has been forwarded to my successor.")

    # Find the reciever in TCP_Recieve
    def file_request_respond(self,predecessor_message):

        file_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        request_peer = int( predecessor_message.split()[1] )
        # send message back to the requester
        file_socket.connect( (host, Fix_port + request_peer ) )
        file_socket.send( f"file_here myID {current_peer}".encode() )

        file_socket.close()
       
        print(f"A response message, destined for peer {request_peer}, has been sent.")

    def is_file_here(self,filename_int): # return True or False
        hash_number = filename_int%256
        if hash_number <= current_peer:
            return True
            # file is here
        elif hash_number > current_peer and hash_number > predecessor[0] and current_peer < predecessor[0]:
            return True
            # the end of the circle,file is here
        else:
            return False

###########################################################################

# identify does the process finish
Transfer_process = False
# use the same socket as previous
# two UDP sockets can not bind the same port, because when response is sent back
# it doesn't know which socket shoud recieve this packet!

# if we use only one socket, we need to use multithreading.

class UDP_Transfer_File:

    global host, Fix_port, Transfer_process

    def __init__(self,file_content,drop_prob,MSS,UDP_send_socket,requester_ID,current_peer,int_filename):

        self.file_content = file_content
        self.drop_prob = drop_prob
        self.MSS = MSS
        self.UDP_socket = UDP_send_socket

        self.thread = threading.Thread(target = self.file_send)
        self.port = requester_ID + Fix_port
        self.host = host
        self.seq  = 0
        self.ack  = 0
        self.request_addr = (host, Fix_port + requester_ID)

        self.peer = current_peer
        self.filename_int = int_filename
        self.start_time = time.time()
        self.fp = 0
        self.segment_length = 0

    def open_file(self):

        PDFfilename = f"{self.filename_int}.pdf"
        fp = open(PDFfilename,"rb")
        self.fp = fp

    def read_file(self, segment_length):

        segment_content = self.fp.read(segment_length)
        return segment_content
    
    
    def start(self):
        self.thread.start()
    
    def send_segment(self,drop_prob,segment,seq,ack,segment_length,interval):
        global is_rtx

        if random.random() < drop_prob:

            if is_rtx == True:
                Log("responding_log.txt", "RTX/Drop", interval ,seq+1,  segment_length,  0 )

            else:
                Log("responding_log.txt", "Drop", interval ,seq+1,  segment_length,  0 )
                #is_rtx = False
        else:
            #segment = pickle.dumps(data_packet)
            
            # snd
            # decode bytes as string and connect it with other information
            self.UDP_socket.sendto( segment  , self.request_addr )

            if is_rtx == True:
                Log("responding_log.txt", "RTX", interval ,ack+1,  segment_length,  0 )
                is_rtx = False

            else:
                Log("responding_log.txt", "snd", interval ,seq+1,  segment_length,  0 )
                is_rtx = False

    # return bytes directly 
    # def take_content(self,segment_length):
    #     return content
    def file_send(self):

        # when peer found that file is in their location
        # send file to requester
        Transfer_process = False

        # use the same UDP_socket (self.UDP_socket = UDP_socket )
        file_length = len(self.file_content)
        packet_length = self.MSS
        # open the fille by "rb"
        self.open_file()

        #print(f"1 SEQ {self.seq} 2 ACK {self.ack} ")
        global is_rtx
        is_rtx = False
        

        while self.ack < file_length:

            if self.seq == self.ack:
            
                if self.seq + self.MSS <= file_length :
                    segment_length = self.MSS
                    self.segment_length = segment_length
                else:
                    segment_length = file_length - self.seq
                    self.segment_length = segment_length
                    # this means this is the last packet
                    Transfer_process = True
                
                file_data = self.read_file(segment_length)
                #print(file_data)
                rec_seq = self.seq + segment_length - 1

                data_packet = Packet("File_transfer",file_data,self.filename_int, self.peer, Transfer_process, self.seq, rec_seq, segment_length)
                #segment = pickle.dumps(data_packet)

                segment = pickle.dumps(data_packet)
                interval = (time.time() - self.start_time)
                self.send_segment(self.drop_prob, segment, self.seq, self.ack, segment_length, interval )

                # drop probability
                # if random.random() < self.drop_prob :
                #     # lost packet
                #     #Log drop
                #     if data_packet.rtx == True:
                #         Log("responding_log.txt", "RTX/Drop", time.time() - self.start_time ,self.seq,  segment_length,  0 )
                #     else:
                #         Log("responding_log.txt", "Drop", time.time() - self.start_time ,self.seq,  segment_length,  0 )
                # else:
                #     segment = pickle.dumps(data_packet)
                #     # snd
                #     # decode bytes as string and connect it with other information

                #     self.UDP_socket.sendto( segment  , self.request_addr )
                #     # Log
                #     Log("responding_log.txt", "snd", time.time() - self.start_time ,self.seq,  segment_length,  0 )
                
                self.seq += self.MSS
                # start to record send time now
                send_time = time.time()
            # when time out happens
            # RTX
            elif (self.ack < file_length) and ( time.time() - send_time > 3):

                is_rtx = True
                segment = pickle.dumps(data_packet)

                interval = (time.time() - self.start_time)
                self.send_segment(self.drop_prob, segment, self.seq, self.ack, segment_length, interval )

                # self.UDP_socket.sendto(  segment, self.request_addr  )
                # #Log
                # Log("responding_log.txt", "RTX", time.time() - self.start_time ,self.ack,  segment_length,  0 )
                send_time = time.time()

    def recieve_ack(self, ack_message):
        self.ack =  int ( ack_message.split()[1] )
        Log("responding_log.txt", "rcv", time.time() - self.start_time ,0,  self.segment_length,  self.ack+1 )


class UDP_Recieve_File:

    global host, Fix_port
    global wh_content

    def __init__(self,UDP_rec_socket,rec_seq, sender_peer,str_filename):

        self.UDP_socket = UDP_rec_socket
        self.ack = rec_seq + 1
        self.response_addr = (host, Fix_port + sender_peer)
        self.start_time = time.time()
        self.whole_rec_content = b''
        # read from UDP segment
        self.filename = str_filename + ".pdf"
        # self.w_file = 0

    def write_in_file(self):
        with open("recieved"+self.filename, "wb") as w_file:
            w_file.write(wh_content.w_content)
        #print("The file is received.")
        w_file.close()

    def accumulate_segment(self, b_segment_content):
        self.whole_rec_content = self.whole_rec_content + b_segment_content

    # file recieve should be finished in UDP_Recieve function
    # distingush with the header "File_transfer"
    def file_recieve_response(self, rec_packet):
        
        pre_seq = rec_packet.seq
        segment_length = rec_packet.segment_length

        Finish_tip = rec_packet.Transfer_process
        # Log request
        # rcv
        Log("requesting_log.txt", "rcv", time.time() - self.start_time  ,pre_seq +1,  segment_length, 0 )
        # accumulate the segment in UDP packet
        b_segment_content = rec_packet.trans_data
        wh_content.w_content += b_segment_content
        # recieved segment, start to send back ack message
        ack_message = f"File_transfer_response {self.ack}".encode()
        self.UDP_socket.sendto(   ack_message  ,self.response_addr)
        # Log request
        # snd
        Log("requesting_log.txt", "snd", time.time() - self.start_time  ,0 ,  segment_length,  self.ack+1 )
        # According to Finish_tip, does the process finished
        if Finish_tip == True:
            self.write_in_file()
            print("The file is received.")

###########################################################################
###########################################################################



# define a UDP Socket
# We can only bind the Ip and port one time, so we need to deal with Send and Recieve under the same bind and socket
def UDP_Send_to(): # Client
    #request = "Request".encode()
    global UDP_1_SEQ, UDP_2_SEQ, UDP_1_ACK, UDP_2_ACK
    global peer_successor, terminate_peer
    global UDP_SEQ, UDP_ACK

    while True:
        if terminate_peer == True:
            break
        time.sleep(10)
        #lock.acquire()
        Server_1 = (host,Fix_port + peer_successor[0] )
        Server_2 = (host,Fix_port + peer_successor[1] )
        ServerB =[ Server_1,  Server_2]

        if UDP_1_SEQ - UDP_1_ACK < 3:
            UDP_socket.sendto(f"Request UDP_1_SEQ {UDP_1_SEQ}".encode() ,Server_1)
            UDP_1_SEQ += 1
        else:
            # Server1 killed
            print(f"My First successor {peer_successor[0]} leave")
            # Kill_Peer_1 = threading.Thread(target = Peer_killed, args = (peer_successor[0],) )
            # Kill_Peer_1.start()
            Peer_killed(peer_successor[0])

        if UDP_2_SEQ - UDP_2_ACK < 3:
            UDP_socket.sendto(f"Request UDP_2_SEQ {UDP_2_SEQ}".encode()  ,Server_2)
            UDP_2_SEQ += 1
        else:
            # Server2 Killed, you find your second successor leave
            print(f"My Second successor {peer_successor[1]} leave")
            # Kill_Peer_2 = threading.Thread(target = Peer_killed, args = (peer_successor[1],) )
            # Kill_Peer_2.start()
            Peer_killed(peer_successor[1])  

        #lock.release()
    UDP_socket.close()


def UDP_Recieve():  # Server
    #response = "Response".encode()
    global UDP_1_SEQ, UDP_2_SEQ, UDP_1_ACK, UDP_2_ACK
    global predecessor, terminate_peer

    while True:
        if terminate_peer == True:
            break
        data, addr = UDP_socket.recvfrom(1024)
        try:
            data = data.decode()
        except:
             # Accept Fille_transfer packet
            rec_packet = pickle.loads(data)
            if rec_packet.name == "File_transfer":

                str_filename = str(rec_packet.filename_int)
                sender_peer = rec_packet.peer
                Transfer_process = rec_packet.Transfer_process
                Rec_seq = rec_packet.rec_seq
                # create a UDP RECIEVE FILE Class
                UDP_File_Rec = UDP_Recieve_File(UDP_socket, Rec_seq, sender_peer, str_filename )

                UDP_File_Rec.file_recieve_response( rec_packet )

        # Accept the Ack message from Reciever
        if data.split()[0] == "File_transfer_response":

            # call UDP_Send_Packet_Requester
            # this function is used to update the ack in Object
            # because we need to compare the seq and ack in order to exectute this sendinhg
            UDP_Send_Packet_Requester.recieve_ack( data )

        if data.split()[0] == "Request":
            print("A ping request message was received from Peer "+ str(int(addr[1])-Fix_port) )
            
            # accumulate the predecessor
            construct_predecessor(int(addr[1])-Fix_port)
            if len(predecessor) > 2:
                predecessor = []
            # if len(predecessor) >=2:
            #     print(f"---------------- predecessor {predecessor[0],predecessor[1]}")
            mark = data.split()[1]
            seq_ack  = int(data.split()[2])
            if mark == "UDP_1_SEQ":
                UDP_socket.sendto(f"Response UDP_1_ACK {seq_ack}".encode()     ,addr)
            if mark == "UDP_2_SEQ":
                UDP_socket.sendto(f"Response UDP_2_ACK {seq_ack}".encode()     ,addr)

        if data.split()[0] == "Response":
            print("A ping response message was received from Peer "+ str(int(addr[1])-Fix_port) )
            
            if data.split()[1] == "UDP_1_ACK":
                UDP_1_ACK = int(data.split()[2])
            if data.split()[1] == "UDP_2_ACK":
                UDP_2_ACK = int(data.split()[2])

    UDP_socket.close()


## construct predecessor
predecessor = []
def construct_predecessor(received_ID):

    global predecessor

    if received_ID not in predecessor:
        predecessor.append(received_ID)
    
    ## if we get all two predecessor, we need to arragne the 1 2 predecessor
    if len(predecessor) == 2:
        predecessor.sort()
        if current_peer < predecessor[0] and current_peer < predecessor[1]:
            tem_1 = min(predecessor)
            tem_2 = max(predecessor)
            predecessor = [tem_2,tem_1]
        elif current_peer > predecessor[0] and current_peer > predecessor[1]:
            tem_1 = min(predecessor)
            tem_2 = max(predecessor)
            predecessor = [tem_2,tem_1]
        else:
            tem_1 = min(predecessor)
            tem_2 = max(predecessor)
            predecessor = [tem_1,tem_2]
            

        # if predecessor[0] < current_peer and predecessor[1] > current_peer:
        #     temp = predecessor[0]
        #     predecessor[0] = predecessor[1]
        #     predecessor[1] = temp

# Peer departure
def Peer_departure():

    global terminate_peer
    # current peer will departure
    # send message to it's predecessor
    # round 1
    TCP_socket_1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #TCP_socket_1.bind(Local) # may no need to bind
    TCP_socket_1.connect((host,Fix_port + predecessor[0]))
    information = f"successor_quit 1 peer {current_peer} add {peer_successor[1]} as 2 successor".encode()
    TCP_socket_1.send(information)
    TCP_socket_1.recv(1024)
    TCP_socket_1.close()

    # round 2
    TCP_socket_2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #TCP_socket_2.bind(Local)  # may no need to bind
    TCP_socket_2.connect((host,Fix_port + predecessor[1]))
    information = f"successor_quit 2 peer {current_peer} add {peer_successor[0]} as 2 successor".encode()
    TCP_socket_2.send(information)
    TCP_socket_2.recv(1024)
    TCP_socket_2.close()

    # send message to it's sucessors
    # round 1
    TCP_socket_3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #TCP_socket_3.bind(Local)
    TCP_socket_3.connect((host,Fix_port + peer_successor[0]))
    information = "predecessor_quit".encode()
    TCP_socket_3.send(information)
    TCP_socket_3.recv(1024)
    TCP_socket_3.close()
    # round 2
    TCP_socket_4 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #TCP_socket_4.bind(Local)
    TCP_socket_4.connect((host,Fix_port + peer_successor[1]))
    information = "predecessor_quit".encode()
    TCP_socket_4.send(information)
    TCP_socket_4.recv(1024)
    TCP_socket_4.close()

    # tell Peer now you can terminate your programming
    terminate_peer = True


def Peer_killed(which_peer):
    global UDP_1_SEQ, UDP_2_SEQ, UDP_1_ACK, UDP_2_ACK
    global peer_successor
    # print which peer leave?
    print(f"Peer {which_peer} is no longer alive.")
    #print(f"  SSSSSSuccessor {peer_successor[0]}, {peer_successor[1]} ")

    if which_peer == peer_successor[0]:

        # second message to second successor with temperory TCP socket
        # TCP
        temp_1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # don't need to bind
        temp_1.connect( (host, Fix_port + peer_successor[1]) )
        information = f"Killed {which_peer} predecessor_quit return your 1 successor".encode()

        temp_1.send(information)
        new_1_successor = int( temp_1.recv(1024).decode() )
        peer_successor.remove(peer_successor[0])
        peer_successor.append(new_1_successor)
        temp_1.close()
        UDP_1_SEQ =0
        UDP_2_SEQ =0
        UDP_1_ACK =0
        UDP_2_ACK =0
    
    if which_peer == peer_successor[1]:
        # second the message to first successor
        # TCP
        #print("111Why not HERE")
        temp_2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # don't need to bind
        #temp_2.listen(1)
        #print("222Why not HERE")
        #print(f"TTTTTTTTTTTTTTTTT succerrsor {peer_successor[0],peer_successor[1]}")
        #print((host, Fix_port + peer_successor[0]))
        temp_2.connect( (host, Fix_port + peer_successor[0]) )
        information = f"Killed {which_peer} successor_1_quit return your 1 successor".encode()
        temp_2.send(information)
        #print("333Why not HERE")
        #print(f"Successor {peer_successor[0]}, {peer_successor[1]}")
        new_2_successor = int( temp_2.recv(1024).decode() )
        # update my successor
        peer_successor.remove(peer_successor[1])
        peer_successor.append(new_2_successor)
        temp_2.close()
        #print("6666666 Why not HERE")
        #lock.release()
        UDP_1_SEQ =0
        UDP_2_SEQ =0
        UDP_1_ACK =0
        UDP_2_ACK =0

    # print(f"---------------- succerrsor {peer_successor[0],peer_successor[1]}")
    # print(f"---------------- predecessor {predecessor[0],predecessor[1]}")
    print(f"My first successor is now peer {peer_successor[0]}.")
    print(f"My second successor is now peer {peer_successor[1]}.")
    



def TCP_Recieve():

    global Drop_p, MSS, current_peer
    global predecessor

    # global this object, we need to call it in UDP_Rec object
    global UDP_Send_Packet_Requester
    global Peer_Send_File_Request

    # get the message from quiting peers
    while True:
        rec_socket, addr = Rec_TCP.accept()
        #print(addr)
        rec_data = rec_socket.recv(1024).decode()
        #print(rec_data)

        # identify the peer action
        if rec_data.split()[0] == "successor_quit":
            data = rec_data.split()
            
            removed_successor = int(data[3])
            new_2_successor = int(data[5])
            print(f"Peer {removed_successor} will depart from the network")
            # update the peer successor
            peer_successor.remove(removed_successor)
            peer_successor.append(new_2_successor)

            print(f"My first successor is now peer {peer_successor[0]}.")
            print(f"My second successor is now peer {peer_successor[1]}.")

            # send back the message
            back_message = "departure recieved".encode()
            rec_socket.send(back_message)

        if rec_data.split()[0] == "predecessor_quit":
            print("Update predecessor")
            # update predecessor
            predecessor = []
        
        if rec_data.split()[0] == "Killed":

            #print("GET GET GET GET")
            # Deliver_Thread = threading.Thread(target = Deliver_info, args = (int(rec_data.split()[1]), rec_socket)   )
            # Deliver_Thread.start()
            peer = int(rec_data.split()[1])
            
            #Deliver_info(int(rec_data.split()[1]), rec_socket)

            if peer_successor[0] == peer:
                #print(f"3333 Send {peer_successor[1]} back")
                # identify two situation [4,5] [5,8]
                # peer successor already updated???
                if peer in peer_successor:

                    rec_socket.send(f"{peer_successor[1]}".encode())
                else:
                    rec_socket.send(f"{peer_successor[0]}".encode())

                #print("DONEDONEDONE")
            else:
                rec_socket.send(f"{peer_successor[0]}".encode())
                predecessor = []


        
        if rec_data.split()[0] == "File_request":

            request_peer = int( rec_data.split()[1] )
            int_filename = int( rec_data.split()[3] )

            if Peer_Send_File_Request.is_file_here(int_filename):
                # Send back a TCP message, means your file is here
                predecessor_message = rec_data
                print(f"File {int_filename} is here")
                Peer_Send_File_Request.file_request_respond(predecessor_message)

                # start to send packets back
                # use UDP_socket here
                print("We now start sending the file ………")
                PDFfilename = f"{int_filename}.pdf"

                fpointer = open(PDFfilename,"rb")
                whole_content = fpointer.read()

                fpointer.close()
                # create a Class
                # fille_content is byte
                UDP_Send_Packet_Requester = UDP_Transfer_File( whole_content, Drop_p, MSS, UDP_socket, request_peer, current_peer, int_filename)

                # send file
                # start the send threading here 
                UDP_Send_Packet_Requester.start()

                # ready to use UDP send file back
            else:
                # forward to my successor
                print(f"File {int_filename} is not stored here.")
                predecessor_message = rec_data
                Peer_Send_File_Request.file_forward_request(predecessor_message)
        #print("QQQQQQQQQQ DONE")
        rec_socket.close()
        #print("WWWWWWWWWW DONE")

def Moniter_input():
    global Peer_Send_File_Request

    while True:
        input_message = input()
        # command message
        if input_message == "quit":
            # execute Peer departure
            Peer_departure()
            if terminate_peer == True:
                print("This peer will depart")
                break
        if input_message.split()[0] == "request":
            # get the request filename
            filename_int = int(input_message.split()[1])
            # Is file in your own place
            if Peer_Send_File_Request.is_file_here(filename_int) :
                print("File is in your own place")
            # send request to my successor
            else:
                Peer_Send_File_Request.file_request(filename_int)


# get the input
current_peer = int(sys.argv[1])
peer_successor = [ int(sys.argv[2]) ,int(sys.argv[3]) ]
MSS = int(sys.argv[4])
Drop_p = float(sys.argv[5])
host = '127.0.0.1'
Fix_port = 52146

port = Fix_port + current_peer
Local = (host,port)
terminate_peer = False
UDP_1_SEQ =0
UDP_2_SEQ =0
UDP_1_ACK =0
UDP_2_ACK =0

# create a common socket, which is used to bind the port
# we can not create two or more socket in this problem, because only one socket should be used to send and recieve message
UDP_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
UDP_socket.bind(Local)

Rec_TCP = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
Rec_TCP.bind(Local)
Rec_TCP.listen(1)

Thread_UDP_Send_to = threading.Thread(target= UDP_Send_to)
Thread_UDP_Send_to.start()

Thread_UDP_Rec = threading.Thread(target= UDP_Recieve)
Thread_UDP_Rec.start()

Thread_TCP_Rec = threading.Thread(target = TCP_Recieve )
Thread_TCP_Rec.start()

Thread_Moniter_input = threading.Thread(target = Moniter_input )
Thread_Moniter_input.start()

Peer_Send_File_Request = Send_File_Requester()
wh_content = W_content(b'')





















