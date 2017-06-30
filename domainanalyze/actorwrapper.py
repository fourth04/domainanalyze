from multiprocessing import Process, Queue

class ActorWrapper(Process):

    """将一个函数转换成带有核心操作--send()的Actor，这是一个进程对象，以便消息发布/订阅模型中的交换机可以发布消息"""

    def __init__(self, func, q):
        """TODO: to be defined1. """
        Process.__init__(self)
        self.func = func
        self._mailbox = Queue()
        self.q = q

    def send(self, msg):
        '''
        Send a message to the actor
        '''
        self._mailbox.put(msg)

    def recv(self):
        '''
        Receive an incoming message
        '''
        msg = self._mailbox.get()
        return msg

    def run(self):
        '''
        Run method to be implemented by the user
        '''
        while True:
            msg = self.recv()
            result = self.func(msg)
            self.q.put(result)
