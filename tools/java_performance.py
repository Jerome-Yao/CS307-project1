import matplotlib.pyplot as plt
import numpy as np

# Java专属数据
data = {
    "Mode": ["Single", "Multi"],
    "Ubuntu": [13.037, 5.231],
    "Windows": [15.2, 6.9],
    "macOS": [10.8, 4.5]
}

# 可视化配置
plt.style.use('ggplot')
colors = ['#FF6F00', '#1976D2', '#4CAF50']

fig, ax = plt.subplots(figsize=(8,6))
x = np.arange(len(data["Mode"]))
width = 0.3

# 绘制柱状图
for i, (sys, color) in enumerate(zip(['Ubuntu', 'Windows', 'macOS'], colors)):
    ax.bar(x + i*width, data[sys], width, color=color, label=sys)

# 优化显示
ax.set_title('Java', pad=20, fontweight='bold')
ax.set_xticks(x + width)
ax.set_xticklabels(data["Mode"])
ax.set_ylabel("time (s)", labelpad=15)

# 样式调整
ax.spines[:].set_visible(False)
ax.grid(axis='y', linestyle=':', alpha=0.7)

# 精确数值标注
for bars in ax.containers:
    ax.bar_label(bars, fmt='%.2f', padding=2, fontsize=10)

plt.legend(frameon=False, ncol=3, bbox_to_anchor=(0.5, 1.12), loc='upper center')
plt.tight_layout()
plt.axis('off')  # 关闭全部坐标轴及边框
plt.savefig('java_performance.png', dpi=300, bbox_inches='tight')
plt.show()