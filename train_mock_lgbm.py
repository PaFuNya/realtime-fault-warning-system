import lightgbm as lgb
import numpy as np

# 1. 模拟生成训练数据
# 假设我们有 1000 条设备历史数据
n_samples = 1000

# 特征 1：平均温度 (通常在 40 到 100 度之间)
temp = np.random.uniform(40, 100, n_samples)
# 特征 2：平均振动 (通常在 0.5 到 5.0 之间)
vib = np.random.uniform(0.5, 5.0, n_samples)

# 组合特征 X
X = np.column_stack((temp, vib))

# 目标 Y：RUL (剩余寿命)
# 模拟逻辑：温度越高、振动越大，剩余寿命越短
# 假设基准寿命是 300 小时
y = 300 - (temp * 2) - (vib * 10) + np.random.normal(0, 10, n_samples)
# 寿命不能小于 0
y = np.maximum(y, 5.0)

# 2. 构建 LightGBM 数据集
train_data = lgb.Dataset(X, label=y, feature_name=['avg_temp', 'avg_vib'])

# 3. 设置训练参数 (这是一个简单的回归任务)
params = {
    'objective': 'regression',
    'metric': 'rmse',
    'num_leaves': 31,
    'learning_rate': 0.05,
    'feature_fraction': 0.9,
    'verbose': -1
}

# 4. 训练模型
print("开始训练模拟的 LightGBM 模型...")
bst = lgb.train(params, train_data, num_boost_round=100)

# 5. 保存模型为 txt 文件，供 Flink / Java 加载
model_path = 'model.txt'
bst.save_model(model_path)

print(f"模型训练完成！已保存到: {model_path}")
print("模型特征要求顺序: 1. avg_temp, 2. avg_vib")
