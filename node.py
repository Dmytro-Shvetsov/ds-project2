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

    def Read(self, request, context):
        key = request.key
        self.cli.processes[0].in_queue.put((key, process.Event.READ_SINGLE))
        msg = self.cli.processes[0].out_queue.get()
        
        self.cli.processes[0].out_queue.task_done()
        return shop_pb2.ReadResponse(value=msg[0], kind= msg[1])


class Node(cmd.Cmd):
    prompt = '> '
    def __init__(self, node_id):
        super().__init__()
        self.node_id = node_id
        self.prompt = f'Node-{node_id}> '
        self.channels = {i:grpc.insecure_channel(f"localhost:{BASE_PORT + i}") for i in range(3) if i != node_id}
        self.stubs = {i:shop_pb2_grpc.BookShopStub(self.channels[i]) for i in range(3) if i != node_id}

        self.k = 0
        self.head_idx = 0
        self.processes = []
        self.proc2node = []
        self.chain_id2proc = []
        self.chain = []

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
        if len(self.chain) == 0:
            print('Chain is not created')
            return
        head_ps = self.chain[self.head_idx]
        head_node = self.proc2node[head_ps]

        tail_ps = self.chain[self.head_idx]
        tail_node = self.proc2node[tail_ps]

        body = '->'.join('Node{}-PS{}'.format(self.proc2node[ps], ps) for ps in self.chain[1:-1])
        print(f'Node{head_node}->PS{head_ps}(Head)->' + body + f'->Node{tail_node}->PS{tail_ps}(Tail)')

    def do_Write(self, args):
        if len(self.chain) != 0: 
            global node
            args = args.split(' ')
            head_id = self.proc2node[self.chain[self.head_idx]]
            if head_id == self.node_id:
                response = node.Write(shop_pb2.WriteRequest(key=args[0], value=args[1], pos=0), None)
            else:
                head_st = self.stubs[head_id]
                response = head_st.Write(shop_pb2.WriteRequest(key=args[0], value=args[1], pos=0))
        else:
            print('Chain is not created yet')
            return

    def do_List_books(self, args):
        global node
        target_proc:process.Process = random.choice(self.processes, 1)[0]
        target_proc.in_queue.put(process.Event.READ)
        
        data = target_proc.out_queue.get()
        if not data:
            return print('No books available')
        for i, (k, v) in enumerate(data.items()):
            print(f'{i+1}) {k} = {v} EUR')
        target_proc.out_queue.task_done()

    def do_Read(self, args):
        global node
        random_proc:process.Process = random.choice(self.processes, 1)[0]
        random_proc.in_queue.put((args, process.Event.READ_SINGLE))
        data = random_proc.out_queue.get()

        random_proc.out_queue.task_done()

        if data[1] == 'write_dirty':
            print(f'Uncommitted write on local process {random_proc.pid}')
            print(f'Asking head')
            head_id = self.proc2node[self.chain[self.head_idx]]
            if head_id == self.node_id:
                print(f'Key is not commited. Try again later.')
                return

            head_st = self.stubs[head_id]
            response = head_st.Read(shop_pb2.ReadRequest(key=args))

            value, kind = response.value, response.kind

            if kind == None:
                print(f'Item is missing')
                return
                
            if kind == 'write_dirty':
                print(f'KEy is not commited. Try again later.')
                return

            print(f'Book {args} costs {value} EUR')


        if data[0] == None:
            print(f'Item is missing. Try again later.')

        if data[1] == 'write_clean':
            print(f'Book {args} costs {data[0]} EUR')

    def do_Remove_head(self, args):
        if len(self.chain) == 0:
            print('Chain is not created')
            return
        
        if self.head_idx == len(self.chain) - 1:
            print('Cannot remove the head - only one process left')
            return
        
        self.head_idx = self.head_idx + 1
        self.do_List_chain('')

    def do_Time_out(self, args):
        pass

    def do_Data_status(self, args):
        global node
        target_proc:process.Process = random.choice(self.processes, 1)[0]
        target_proc.in_queue.put(process.Event.DATA_STATUS)
        
        data = target_proc.out_queue.get()
        if not data:
            return print('No books available')
        for i, (k, v) in enumerate(data.items()):
            print(f'{i+1}) {k} - {v}')
        target_proc.out_queue.task_done()
    
    def finalize(self):
        for p in self.processes:
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
