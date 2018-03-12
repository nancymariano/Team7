import pickle

#class to define a reply to client
#status = 1, 0 to represent whether fail or success
#result = reply from data_node if any
#err = error message
class Reply:
    def __init__(self, status, result, err):
        self.status = status
        self.result = result
        self.err = err

    def is_ok(self):
        return self.status == 0

    def is_err(self):
        return self.status == 1

    def __repr__(self):
        return '{' + 'status=' + str(self.status) + ',result=' + str(self.result) + ',err=' + str(self.err) + '}'

    @staticmethod
    def reply(result = None):
        reply = Reply(status = 0, result = result, err = None)
        return pickle.dumps(reply)

    @staticmethod
    def error(msg):
        reply = Reply(status = 1, result = None, err = msg)
        return pickle.dumps(reply)

    @staticmethod
    def Load(str):
        return pickle.loads(str)