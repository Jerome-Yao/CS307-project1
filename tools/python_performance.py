import matplotlib.pyplot as plt
import numpy as np

# Python专属数据
data = {
    "Mode": ["Original", "Single", "Multi"],
    "Ubuntu": [126.0, 26.63, 22.97],
    "Windows": [138.0, 28.5, 29.8],
    "macOS": [115.0, 24.2, 23.5]
}

# 可视化配置
plt.style.use('ggplot')
colors = ['#FF6F00', '#1976D2', '#4CAF50']

fig, ax = plt.subplots(figsize=(10,6))
x = np.arange(len(data["Mode"]))
width = 0.25

# 绘制柱状图
for i, (sys, color) in enumerate(zip(['Ubuntu', 'Windows', 'macOS'], colors)):
    ax.bar(x + i*width, data[sys], width, color=color, label=sys)

# 装饰元素
ax.set_title('Python', pad=20, fontweight='bold')
ax.set_xticks(x + width)
ax.set_xticklabels(data["Mode"], rotation=0)
ax.set_ylabel("time (s)", labelpad=15)

# 样式优化
ax.spines[:].set_visible(False)
ax.grid(axis='y', linestyle=':', alpha=0.7)
ax.set_ylim(0, 150)

# 添加数值标签
for bars in ax.containers:
    ax.bar_label(bars, fmt='%.1f', padding=3)

plt.legend(frameon=False, ncol=3, bbox_to_anchor=(0.5, 1.15), loc='upper center')
plt.tight_layout()
# plt.axis('off')  # 关闭全部坐标轴及边框
plt.savefig('python_performance.png', dpi=300, bbox_inches='tight')
plt.show()