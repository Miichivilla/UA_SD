"""AdminClient / NewTopic falsos (ver __init__)."""
from concurrent.futures import Future
from . import KafkaException, _addr, _rpc, _Md


class NewTopic:
    def __init__(self, topic, num_partitions=1, replication_factor=1, config=None):
        self.topic = topic


class AdminClient:
    def __init__(self, conf):
        self.addr = _addr(conf)

    def list_topics(self, timeout=5):
        return _Md(_rpc(self.addr, {"op": "topics"}, timeout)["topics"])

    def create_topics(self, new_topics):
        out = {}
        for nt in new_topics:
            f = Future()
            try:
                r = _rpc(self.addr, {"op": "create", "topic": nt.topic})
                if r.get("ok"):
                    f.set_result(None)
                else:
                    f.set_exception(KafkaException(r.get("err")))
            except KafkaException as e:
                f.set_exception(e)
            out[nt.topic] = f
        return out
