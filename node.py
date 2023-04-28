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

class ShopServicer(shop_pb2_grpc.ShopServicer):
    def __init__(self):
        pass


class Node(cmd.Cmd):
    prompt = '> '
    def __init__(self, node_id):
        super().__init__()
        self.node_id = node_id
        self.prompt = f'Node-{node_id}> '

    def do_Local_store_ps(self, args):
        pass

    def do_Create_chain(self, args):
        pass

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


if __name__ ==  "__main__":
    node_id = sys.argv[1]
    cli = Node(node_id)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    shop_pb2_grpc.add_TicTacToeServicer_to_server(ShopServicer(node_id), server)
    while True:
        try:
            cli.cmdloop()
        except Exception as exc:
            server.stop(0)
            raise exc
