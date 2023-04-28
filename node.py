import cmd

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

    

