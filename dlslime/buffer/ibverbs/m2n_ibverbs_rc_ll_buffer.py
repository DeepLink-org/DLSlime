from dlslime import _slime_c


class M2NIBVerbsRCLLBuffer:
    def __init__(
        self,
        max_bs: int,
        msg_size: int,
        device: str,
        role: int,
        m_world_size: int,
        n_world_size: int,
        rank: int,
        num_concurrency: int,
        qp_num: int,
    ):
        self.max_bs = max_bs
        self.msg_size = msg_size
        self.device = device
        self.role = role
        self.m_world_size = m_world_size
        self.n_world_size = n_world_size
        self.rank = rank
        self.num_concurrency = num_concurrency
        self.qp_num = qp_num

        self._buffer = _slime_c.M2NIBVerbsRCLLBuffer()

    @property
    def buffer_info(self):
        return self._buffer.buffer_info()

    def connect_full_mesh(self, peer_all_buffer_info):
        return self._buffer.connect_full_mesh(peer_all_buffer_info)

    def send(self):
        raise NotImplementedError

    def recv(self):
        raise NotImplementedError
