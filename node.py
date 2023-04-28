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

class ShopServicer(shop_pb2_grpc.BookShopServicer):
    def __init__(self, node_id):
        self.node_id = node_id
        self.channels = {i:grpc.insecure_channel(f"localhost:{BASE_PORT + i}") for i in range(3) if i != node_id}
        self.stubs = {i:shop_pb2_grpc.BookShopStub(self.channels[i]) for i in range(3) if i != node_id}

    def GetNumProc(self, request, context):
        print(f'My number of processes is {Node.k}')
        return shop_pb2.ProcessCount(num=Node.k)

class Node(cmd.Cmd):
    k = 0
    prompt = '> '
    def __init__(self, node_id):
        super().__init__()
        self.node_id = node_id
        self.prompt = f'Node-{node_id}> '
        self.channels = {i:grpc.insecure_channel(f"localhost:{BASE_PORT + i}") for i in range(3) if i != node_id}
        self.stubs = {i:shop_pb2_grpc.BookShopStub(self.channels[i]) for i in range(3) if i != node_id}

        self.queues = []
        self.processes = []

    def do_Local_store_ps(self, args):
        Node.k = int(args[0])
        self.queues = [Queue() for _ in range(self.k)]
        self.processes = [process.Process(p, self.queues[p]) for p in range(self.k)]
        for p in self.processes:
            p.start()

    def do_Create_chain(self, args):
        num_procs = []
        for st in self.stubs.values():
            num_procs.append(st.GetNumProc(shop_pb2.Empty()).num)

        num_procs = sum(num_procs)

        self.chain = np.random.choice(range(num_procs), num_procs, replace=False)
        print(f"Chain is {self.chain}")

    def do_List_chain(self, args):
        pass

    def do_Write(self, args):
        pass

    def do_List_books(self, args):
        pass

    def do_Read(self, args):
        pass

    def do_Time_out(self, args):
        pass

    def do_Data_status(self, args):
        pass
    
    def __del__(self):
        for q in self.queues:
            # TODO fix deadlocks
            q.put_nowait(None)
        for p in self.processes:
            p.join()


if __name__ ==  "__main__":
    node_id = int(sys.argv[1])
    port = BASE_PORT + node_id
    cli = Node(node_id)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    shop_pb2_grpc.add_BookShopServicer_to_server(ShopServicer(node_id), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    while True:
        try:
            cli.cmdloop()
        except Exception as exc:
            print(exc)
            server.stop(0)
            break
