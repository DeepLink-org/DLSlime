import matplotlib.pyplot as plt
import numpy as np

# 全局配置（英文风格+专业美观）
plt.rcParams['font.family'] = 'Arial'
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['figure.dpi'] = 100
plt.rcParams['savefig.dpi'] = 300

# 核心数据整理（消息大小、jring组、无jring组）
# Message sizes (bytes) -> converted to MB for readability
message_sizes_bytes = [2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288,
                       1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864, 134217728]
message_sizes_mb = [size / (1024 * 1024) for size in message_sizes_bytes]

# 上组：w/ jring (Num Concurrency=4)
latency_jring = [0.020, 0.017, 0.020, 0.016, 0.015, 0.015, 0.016, 0.018, 0.025,
                 0.039, 0.063, 0.107, 0.201, 0.389, 0.749, 1.442, 2.820]
bandwidth_jring = [104, 237, 411, 1005, 2219, 4352, 8424, 14311, 20892,
                   27026, 33272, 39099, 41671, 43076, 44804, 46542, 47602]

# 下组：w/o jring (Num Concurrency=4)
latency_no_jring = [0.020, 0.016, 0.018, 0.016, 0.018, 0.017, 0.019, 0.020, 0.029,
                    0.042, 0.067, 0.112, 0.209, 0.395, 0.753, 1.449, 2.819]
bandwidth_no_jring = [102, 253, 462, 1046, 1826, 3956, 7063, 12983, 18064,
                      25138, 31412, 37436, 40098, 42474, 44558, 46323, 47611]

# 创建2x1子图（纵向布局，更适合论文排版）
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 12))
fig.suptitle('dlslime Transfer Engine (Num Concurrency=4): w/ jring vs w/o jring',
             fontsize=18, fontweight='bold', y=0.95)

# 专业配色方案（高对比度+打印友好）
color_jring = '#2E86AB'  # 深蓝色（jring组）
color_no_jring = '#E63946'  # 深红色（无jring组）
line_width = 3.0
marker_size = 8
marker_edge_color = 'white'
marker_edge_width = 2.0

# 子图1：平均延迟对比（Avg Latency）
ax1.plot(message_sizes_mb, latency_jring, color=color_jring, linewidth=line_width,
         marker='o', markersize=marker_size, markeredgecolor=marker_edge_color,
         markeredgewidth=marker_edge_width, label='w/ jring', alpha=0.9)
ax1.plot(message_sizes_mb, latency_no_jring, color=color_no_jring, linewidth=line_width,
         marker='s', markersize=marker_size, markeredgecolor=marker_edge_color,
         markeredgewidth=marker_edge_width, label='w/o jring', alpha=0.9)

ax1.set_xlabel('Message Size (MB)', fontsize=14, fontweight='medium')
ax1.set_ylabel('Average Latency (ms)', fontsize=14, fontweight='medium')
ax1.set_title('Average Latency Comparison', fontsize=16, fontweight='bold', pad=20)
ax1.legend(fontsize=12, frameon=True, fancybox=True, shadow=True, loc='upper left')
ax1.grid(True, alpha=0.3, linestyle='--', linewidth=1.0)
ax1.set_xscale('log')  # 对数坐标适配宽范围消息大小
ax1.tick_params(axis='both', which='major', labelsize=11, width=1.2)
# 美化边框（隐藏上、右边框）
ax1.spines['top'].set_visible(False)
ax1.spines['right'].set_visible(False)
ax1.spines['left'].set_linewidth(1.5)
ax1.spines['bottom'].set_linewidth(1.5)

# 子图2：带宽对比（Bandwidth）
ax2.plot(message_sizes_mb, bandwidth_jring, color=color_jring, linewidth=line_width,
         marker='o', markersize=marker_size, markeredgecolor=marker_edge_color,
         markeredgewidth=marker_edge_width, label='w/ jring', alpha=0.9)
ax2.plot(message_sizes_mb, bandwidth_no_jring, color=color_no_jring, linewidth=line_width,
         marker='s', markersize=marker_size, markeredgecolor=marker_edge_color,
         markeredgewidth=marker_edge_width, label='w/o jring', alpha=0.9)

ax2.set_xlabel('Message Size (MB)', fontsize=14, fontweight='medium')
ax2.set_ylabel('Bandwidth (MB/s)', fontsize=14, fontweight='medium')
ax2.set_title('Bandwidth Comparison', fontsize=16, fontweight='bold', pad=20)
ax2.legend(fontsize=12, frameon=True, fancybox=True, shadow=True, loc='upper left')
ax2.grid(True, alpha=0.3, linestyle='--', linewidth=1.0)
ax2.set_xscale('log')
ax2.tick_params(axis='both', which='major', labelsize=11, width=1.2)
# 美化边框
ax2.spines['top'].set_visible(False)
ax2.spines['right'].set_visible(False)
ax2.spines['left'].set_linewidth(1.5)
ax2.spines['bottom'].set_linewidth(1.5)

# 调整布局，避免标题和标签重叠
plt.tight_layout()
plt.subplots_adjust(top=0.92)

# 保存图片（白色背景+高清，适配论文插入）
plt.savefig('dlslime_4concurrency_jring_vs_nojring.png', bbox_inches='tight',
            facecolor='white', edgecolor='none')
# plt.show()