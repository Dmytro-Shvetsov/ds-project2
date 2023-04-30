from threading import Thread
from queue import Queue
import logging

logging.basicConfig(level=logging.INFO)

class Event:
    READ = 'read'
    WRITE_DIRTY = 'write_dirty'
    WRITE_CLEAN = 'write_clean'
    DATA_STATUS = 'data_status'


class Process(Thread):
    in_queue = Queue()
    out_queue = Queue()
    data = {}

    def __init__(self, pid, is_head=False, is_tail=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pid = pid
        self.is_head = is_head
        self.is_tail = is_tail
    
    def run(self) -> None:
        logging.info(f'Started process #{self.pid}')
        while True:
            event = self.in_queue.get()
            logging.info(f'Received an event {event}')
            if event is None:
                break
            self._process_event(event)
            self.in_queue.task_done()
        logging.info(f'Finalized process #{self.pid}')

    def _process_event(self, event):
        if event == Event.READ:
            self.out_queue.put({k: v for k, (v, kind) in self.data.items() if kind == Event.WRITE_CLEAN})
        if len(event) == 3:
            key, value, kind = event
            self.data[key] = (value, kind)
            logging.info(f'Written {key}={value} ({kind}) to process #{self.pid}')
            self.out_queue.put_nowait(True)

