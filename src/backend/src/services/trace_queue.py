import queue

class TraceQueue:
    """Singleton holder for the agent trace queue."""
    _instance = None
    _queue = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TraceQueue, cls).__new__(cls)
            cls._instance._queue = queue.Queue()
        return cls._instance

    def get_queue(self) -> queue.Queue:
        """Get the singleton queue instance."""
        return self._queue

# Function to get the singleton queue instance easily
def get_trace_queue() -> queue.Queue:
    return TraceQueue().get_queue() 