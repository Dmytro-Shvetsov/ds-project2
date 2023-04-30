import cmd
import threading
import grpc
import time
from concurrent import futures
import sys
import time
from datetime import datetime, timedelta
import shop_pb2
import shop_pb2_grpc
import process
from queue import Queue
from numpy import random
import numpy as np

BASE_PORT= 8080

node = None

class ShopServicer(shop_pb2_grpc.BookShopServicer):
    def __init__(self, node_id):
        self.node_id = node_id
        self.cli = Node(node_id)

    def GetNumProc(self, request, context):
        print(f'My number of processes is {self.cli.k}')
        return shop_pb2.ProcessCount(num=self.cli.k)
    
    def ChainNotify(self, request, context):
        self.cli.chain = request.chain
        self.cli.proc2node = request.proc2node
        self.cli.chain_id2proc = request.chain_id2proc
        print(f'Chain received: {self.cli.chain}')
        print(f'Proc2node received: {self.cli.proc2node}')
        print(f'Chain_id2proc received: {self.cli.chain_id2proc}')

        return shop_pb2.Empty()
    
    def Write(self, request, context):
        # print(f'Write request received for key {request.key} and value {request.value}')
        global_pos = request.pos
        
        if global_pos == len(self.cli.chain) - 1:
            # last node, write clean and return
            # return success
            proc_idx = self.cli.chain_id2proc[self.cli.chain[global_pos]]
            proc:process.Process = self.cli.processes[proc_idx]
            print(f'Got to the last node (chain_pos={global_pos}). Performing clean write. {request.key}={request.value}')
            proc.in_queue.put((request.key, request.value, process.Event.WRITE_CLEAN))
            
            msg = proc.out_queue.get()
            if msg:
                print(f'Successful clean write (chain_pos={global_pos}). {request.key}={request.value}')
            else:
                print(f'Failed clean write (chain_pos={global_pos}). {request.key}={request.value}')
            proc.out_queue.task_done()
            return shop_pb2.Status(status=True)
        else:
            # PERFORMING DIRTY WRITE
            proc_idx = self.cli.chain_id2proc[self.cli.chain[global_pos]]
            proc:process.Process = self.cli.processes[proc_idx]
            print(f'Current node is {self.node_id}. (chain_pos={global_pos}). Performing dirty write. {request.key}={request.value}')
            proc.in_queue.put((request.key, request.value, process.Event.WRITE_DIRTY))
            
            msg = proc.out_queue.get()
            if msg:
                print(f'Successful dirty write (chain_pos={global_pos}). {request.key}={request.value}')
            else:
                print(f'Failed dirty write (chain_pos={global_pos}). {request.key}={request.value}')
            proc.out_queue.task_done()

            # MOVING DOWN THE CHAIN
            global_pos += 1
            next_node_id = self.cli.proc2node[self.cli.chain[global_pos]]
            if next_node_id == self.node_id:
                print('Next node is myself. Moving to', global_pos)
                return self.Write(shop_pb2.WriteRequest(key=request.key, value=request.value, pos=global_pos), context)
            
            print('Moving to the next node', next_node_id)
            response = self.cli.stubs[next_node_id].Write(
                shop_pb2.WriteRequest(key=request.key, value=request.value, pos=global_pos)
            )
            if not response.status:
                print('Failed at node', global_pos)
                return shop_pb2.Status(status=False)

            # GOT RESPONSE FROM THE TAIL - PERFORMING CLEAN WRITE
            proc_idx = self.cli.chain_id2proc[self.cli.chain[global_pos]]
            proc:process.Process = self.cli.processes[proc_idx]
            print(f'Traversed up to the tail (chain_pos={global_pos}). Performing clean write. {request.key}={request.value}')
            proc.in_queue.put((request.key, request.value, process.Event.WRITE_CLEAN))
            
            msg = proc.out_queue.get()
            if msg:
                print(f'Successful clean write (chain_pos={global_pos}). {request.key}={request.value}')
            else:
                print(f'Failed clean write (chain_pos={global_pos}). {request.key}={request.value}')
            proc.out_queue.task_done()
            return shop_pb2.Status(status=True)


class Node(cmd.Cmd):
    prompt = '> '
    def __init__(self, node_id):
        super().__init__()
        self.node_id = node_id
        self.prompt = f'Node-{node_id}> '
        self.channels = {i:grpc.insecure_channel(f"localhost:{BASE_PORT + i}") for i in range(3) if i != node_id}
        self.stubs = {i:shop_pb2_grpc.BookShopStub(self.channels[i]) for i in range(3) if i != node_id}

        self.k = 0
        self.processes = []
        self.proc2node = []
        self.chain_id2proc = []

    def do_Local_store_ps(self, args):
        self.k = int(args)
        self.processes = [process.Process(p) for p in range(self.k)]
        for p in self.processes:
            p.start()

    def do_Create_chain(self, args):
        num_procs = [self.k]
        self.proc2node = [self.node_id] * self.k
        self.chain_id2proc = list(range(self.k))
        for node_id, st in self.stubs.items():
            n = st.GetNumProc(shop_pb2.Empty()).num
            num_procs.append(n)
            self.proc2node.extend([node_id]*n)
            self.chain_id2proc.extend(range(n)) 

        num_procs = sum(num_procs)

        self.chain = np.random.choice(range(num_procs), num_procs, replace=False)
        for st in self.stubs.values():
            st.ChainNotify(shop_pb2.Chain(chain=self.chain, proc2node= self.proc2node, chain_id2proc=self.chain_id2proc))

        print(f"Chain is {self.chain}")

    def do_List_chain(self, args):
        head_ps = self.chain[0]
        head_node = self.proc2node[head_ps]

        tail_ps = self.chain[0]
        tail_node = self.proc2node[tail_ps]

        body = ''.join('Node{}-PS{}'.format(self.proc2node[ps], ps) for ps in self.chain[1:-1])
        print(f'Node{head_node}->PS{head_ps}(Head)->' + body + f'->Node{tail_node}->PS{tail_ps}(Tail)')

    def do_Write(self, args):
        global node
        args = args.split(' ')
        head_id = self.proc2node[self.chain[0]]
        if head_id == self.node_id:
            response = node.Write(shop_pb2.WriteRequest(key=args[0], value=args[1], pos=0), None)
        else:
            head_st = self.stubs[head_id]
            response = head_st.Write(shop_pb2.WriteRequest(key=args[0], value=args[1], pos=0))

    def do_List_books(self, args):
        pass

    def do_Read(self, args):
        pass

    def do_Time_out(self, args):
        pass

    def do_Data_status(self, args):
        pass
    
    def finalize(self):
        for p in self.processes:
            # TODO fix deadlocks
            p.in_queue.put_nowait(None)
        for p in self.processes:
            p.join()

    def __del__(self):
        self.finalize()

if __name__ ==  "__main__":
    node_id = int(sys.argv[1])
    port = BASE_PORT + node_id
    node = ShopServicer(node_id)
    node.cli.do_Local_store_ps('2') # FIXME
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    shop_pb2_grpc.add_BookShopServicer_to_server(node, server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    while True:
        try:
            node.cli.cmdloop()
        except Exception as exc:
            print(exc)
            server.stop(0)
            raise exc 
            break
        finally:
            node.cli.finalize()
