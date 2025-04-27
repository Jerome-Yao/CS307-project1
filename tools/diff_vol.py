import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 定义数据
data = {
    'data_volume': [0.25, 0.3, 0.5, 0.75, 1.0],
    'single_thread': [10.24, 10.55, 10.99, 11.74, 12.49],
    'multithreading': [4.82, 5.03, 5.11, 5.40, 5.70]
}

# 创建 DataFrame
df = pd.DataFrame(data)

# 转换数据格式，以便使用 seaborn 绘制回归线
melted_df = df.melt(id_vars='data_volume', 
                    var_name='mode',
                    value_name='time')

# 设置图片清晰度
plt.rcParams['figure.dpi'] = 300

# 设置绘图风格
plt.style.use('seaborn-darkgrid')

# 创建画布
plt.figure(figsize=(10, 6))

# 绘制单线程回归线
sns.regplot(x='data_volume', y='time', data=melted_df[melted_df['mode'] == 'single_thread'],
            scatter_kws={'s': 80, 'alpha': 0.8},
            line_kws={'linestyle': '--', 'linewidth': 2},
            label='single thread', color='orange')

# 绘制多线程回归线
sns.regplot(x='data_volume', y='time', data=melted_df[melted_df['mode'] == 'multithreading'],
            scatter_kws={'s': 80, 'alpha': 0.8},
            line_kws={'linestyle': '-', 'linewidth': 2},
            label='multithreading', color='blue')

# 添加标题和坐标轴标签
plt.title('single_thread vs multithreading')
plt.xlabel('data volume')
plt.xticks([0.25, 0.3, 0.5, 0.75, 1.0], ['25%', '30%', '50%', '75%', '100%'])
plt.ylabel('execute time (s)')

# 添加图例
plt.legend()

# 显示图形
plt.show()
