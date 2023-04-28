from threading import Thread
from queue import Queue
import logging

logging.basicConfig(level=logging.INFO)

class Event:
    READ = 'read'
    WRITE = 'write'
    DATA_STATUS = 'data_status'


class Process(Thread):
    data = {}

    def __init__(self, pid, queue, is_head=False, is_tail=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pid = pid
        self.queue = queue
        self.is_head = is_head
        self.is_tail = is_tail
    
    def run(self) -> None:
        logging.info(f'Started process #{self.pid}')
        while True:
            event = self.queue.get()
            if event is None:
                break
            self._process_event(event)
            self.queue.task_done()
        logging.info(f'Finalized process #{self.pid}')

    def _process_event(self, event):
        if event == Event.READ:
            pass

