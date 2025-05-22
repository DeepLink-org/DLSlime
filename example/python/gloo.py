import argparse

from dlslime import _slime_c


def main(args):
    # 使用参数初始化 Gloo 上下文
    ctx = _slime_c.gloo_context(args.rank, args.size)  # 将 size 作为第二个参数
    file_store = _slime_c.gloo_file_store('/tmp')
    prefix_store = _slime_c.gloo_prefix_store(args.prefix, file_store)
    dev = _slime_c.create_device('mlx5_bond_0', 1, 3)
    ctx.connect_full_mesh(prefix_store, dev)


if __name__ == '__main__':
    # 创建解析器
    parser = argparse.ArgumentParser(description='Gloo 上下文初始化工具')

    # 添加必填参数（无默认值）
    parser.add_argument('--prefix', type=str, help="Gloo 前缀名称（如 '_slime_c'）")
    parser.add_argument('--rank', type=int, help='当前进程的 Rank 值')
    parser.add_argument('--size', type=int, help='通信组大小（必填，无默认值）')  # 新增参数
    # 解析参数
    args = parser.parse_args()
    main(args)
