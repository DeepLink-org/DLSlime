from dlslime import _slime_c


class AllGatherLLBuffer:
    def __init__(self, bs, msg_size, dtype, rank, world_size, rdma_only=True):
        raise NotImplementedError

    def connect_full_mesh(self):
        raise NotImplementedError

    def all_gather_ll(self):
        raise NotImplementedError
