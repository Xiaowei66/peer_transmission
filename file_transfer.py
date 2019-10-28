import threading
import pickle
import random
import time

def log(filename, event, time, seq, nb_bytes, ack):
    with open(filename, 'a') as file:
        file.write(f'{event:>4}{time:>10.3f}{seq:>8}{nb_bytes:>8}{ack:>8}\n')
    

class Packet:
    def __init__(self,seq,ack,payload, MSS, finish = False):
        self.seq = seq
        self.ack = ack
        self.payload = payload
        self.MSS = MSS
        self.finish = finish

class FileTransfer:
    def __init__(self,requester,filename,MSS,socket,start_time,drop_prob):
        self.requester_port = 9406 + requester
        self.MSS = MSS
        self.socket = socket
        self.start_time = start_time
        self.drop_prob = drop_prob
        self.seq = 0
        self.ack = 0
        self.received_ack = 0
        self.send_time = None
        self.transfer_thread = threading.Thread(target = self.transfer)
        with open(filename, 'rb') as file:
            self.content = file.read()
        with open('responding_log.txt','w'):
            pass
        

    def start(self):
        self.transfer_thread.start()

    def transfer(self):
        
        while self.received_ack < len(self.content):
            if self.seq < len(self.content) and self.seq == self.received_ack:
                if self.seq+self.MSS >= len(self.content):
                    packet = Packet(self.seq, self.ack, self.content[self.seq : ], self.MSS, finish=True)
                    nb_bytes = len(self.content) - self.seq
                else:
                    packet = Packet(self.seq, self.ack, self.content[self.seq : self.seq+self.MSS], self.MSS)
                    nb_bytes = self.MSS
                message = pickle.dumps(packet)
                if random.random() < self.drop_prob:
                    log("responding_log.txt", "drop", time.time()-self.start_time, self.seq, nb_bytes, self.ack)
                else:
                    self.socket.sendto(message,('127.0.0.1',self.requester_port))
                    log("responding_log.txt", "snd", time.time()-self.start_time, self.seq, nb_bytes, self.ack)
                self.seq += self.MSS
                last_time = time.time()
            elif self.received_ack < len(self.content) and time.time() - last_time > 1:
                self.socket.sendto(message,('127.0.0.1',self.requester_port))
                log("responding_log.txt", "snd", time.time()-self.start_time, self.received_ack, nb_bytes, self.ack)
                last_time = time.time()

    def receive_ACK(self,message):
        ack_packet = pickle.loads(message)
        log("responding_log.txt", "rcv", time.time()-self.start_time, ack_packet.seq, 0, ack_packet.ack)
        self.received_ack = ack_packet.ack
        self.ack = ack_packet.seq + 1
        if ack_packet.finish == True:
            print('The file is sent.')

class FileReceive:
    def __init__(self,socket,filename,start_time):
        self.socket = socket
        self.filename = filename
        self.start_time = start_time
        self.content = b''
        self.seq = 0
        self.sender_address = None
        with open('requesting_log.txt','w'):
            pass
    
    def receive_file(self,message,sender_address):
        if self.sender_address == None:
            self.sender_address = sender_address
        file_packet = pickle.loads(message)
        log("requesting_log.txt", "rcv", time.time()-self.start_time, file_packet.seq, len(file_packet.payload), file_packet.ack)
        self.content += file_packet.payload
        self.ack = file_packet.seq + len(file_packet.payload)
        ack_packet = Packet(self.seq,self.ack,b'',0,file_packet.finish)
        message = pickle.dumps(ack_packet)
        self.socket.sendto(message,self.sender_address)
        log("requesting_log.txt", "snd", time.time()-self.start_time, ack_packet.seq, 0, ack_packet.ack)
        self.seq += 1
        if file_packet.finish == True:
            with open('received'+self.filename,'wb') as file:
                file.write(self.content)
            print('The file is received.')
        


