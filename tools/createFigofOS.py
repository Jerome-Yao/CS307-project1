import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import FormatStrFormatter

# 数据准备
data = {
    "Language": ["Java", "Java", "Python", "Python", "Python", "C++", "C++", "C++"],
    "Mode": ["Single", "Multi", "Original", "Single", "Multi", "Original", "Single", "Multi"],
    "Ubuntu": [13.037, 5.231, 126.0, 26.63, 22.97, 86.1, 12.87, 10.24],
    "Windows": [15.2, 6.9, 138.0, 28.5, 29.8, 78.3, 11.5, 9.2],
    "macOS": [10.8, 4.5, 115.0, 24.2, 23.5, 62.4, 8.9, 6.3]
}
df = pd.DataFrame(data)

# 可视化配置
plt.style.use('seaborn')
COLORS = {'Ubuntu':'#FF6F00', 'Windows':'#1976D2', 'macOS':'#4CAF50'}  # Material Design配色方案[6](@ref)
plt.rcParams.update({
    'font.size': 10,
    'axes.titlesize': 14,
    'axes.labelpad': 12,
    'font.family': 'DejaVu Sans'
})

def create_comparison_chart(df):
    fig, axs = plt.subplots(1, 3, figsize=(18, 8), dpi=120)
    
    # 按语言分组绘制
    for idx, (lang, group) in enumerate(df.groupby('Language')):
        ax = axs[idx]
        modes = group['Mode'].tolist()
        x = np.arange(len(modes))  # 执行模式坐标
        width = 0.25  # 柱宽
        
        # 绘制各系统柱状图
        for i, (sys, color) in enumerate(COLORS.items()):
            values = group[sys].values
            rects = ax.bar(x + i*width, values, width, 
                          color=color, alpha=0.85, label=sys)
            
            # 添加数值标签
            ax.bar_label(rects, padding=3, fmt='%.1f', 
                        fontsize=8, color='#424242')
        
        # 装饰元素
        ax.set_title(f'{lang} 执行效率', pad=15, fontweight='semibold')
        ax.set_xticks(x + width)
        ax.set_xticklabels(modes, rotation=45, ha='right')
        ax.yaxis.set_major_formatter(FormatStrFormatter('%.0fs'))
        ax.grid(axis='y', linestyle=':', alpha=0.6)
        
        # 坐标轴优化
        ax.spines[['top','right']].set_visible(False)
        ax.set_ylim(0, df[['Ubuntu','Windows','macOS']].max().max()*1.15)
        
        # 仅左侧显示Y轴
        if idx > 0:
            ax.set_ylabel('')
            ax.tick_params(left=False)
        else:
            ax.set_ylabel("执行时间 (秒)", labelpad=15)

    # 全局图例与标题
    handles, labels = axs[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='upper center', 
              ncol=3, frameon=False, bbox_to_anchor=(0.5, 1.02))
    plt.suptitle('跨平台程序执行效率对比分析', y=1.05, 
                fontsize=16, fontweight='bold')
    
    plt.tight_layout()
    return fig

# 生成图表
fig = create_comparison_chart(df)
plt.savefig('performance.png', bbox_inches='tight', transparent=True)
plt.show()