# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 10:07:59 2026

@author: Coogie
"""

import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import getpass
from iFinDPy import *

# 设置 matplotlib 中文字体
plt.rcParams['font.sans-serif'] = ['SimHei']      # 指定默认字体为黑体
plt.rcParams['axes.unicode_minus'] = False        # 解决负号显示问题
# ==================== 参数设置 ====================
DATA_PATH = r'D:\办公\data.xlsx'
START_DATE = '2025-12-01'          # 基期
INIT_CAPITAL = 10_000_000            # 初始资金 1000万
TRADE_UNIT = 100                     # A 股交易单位（股）

# ==================== 读取交易记录 ====================
print("正在读取交易记录...")
df_trade = pd.read_excel(DATA_PATH, sheet_name='Sheet1')
df_trade['日期'] = pd.to_datetime(df_trade['日期']).dt.date
df_trade = df_trade.sort_values('日期').reset_index(drop=True)

# 获取所有原始股票代码（如 SH688270）
orig_codes = df_trade['股票代码'].unique().tolist()

# 转换为 iFinD 标准格式（如 688270.SH）
def convert_code(code):
    if code.startswith('SH'):
        return code[2:] + '.SH'
    elif code.startswith('SZ'):
        return code[2:] + '.SZ'
    else:
        return code

conv_codes = [convert_code(c) for c in orig_codes]
rev_code_map = dict(zip(conv_codes, orig_codes))

print(f"共涉及 {len(orig_codes)} 只股票")
print("转换后的代码：", conv_codes)

# ==================== iFinD 登录 ====================
print("\n请登录 iFinD...")
username = 'dtzqsh002'
password = 'UWx7xDet'
login_res = THS_iFinDLogin(username, password)
if login_res == 0:
    print("登录成功")
elif login_res == -201:
    print("检测到重复登录（可能已在其他位置登录），将使用当前会话继续")
else:
    print(f"登录失败，错误码：{login_res}")
    sys.exit()

# ==================== 获取沪深300日线数据 ====================
hs300_code = '000300.SH'
print("\n正在获取沪深300指数数据...")
end_date_str = datetime.now().strftime('%Y-%m-%d')
hs300_res = THS_HQ(hs300_code, 'close', '', START_DATE, end_date_str)

# 调试输出
print("hs300_res 类型：", type(hs300_res))
print("hs300_res.data 类型：", type(hs300_res.data))

# 检查数据是否为空（兼容 DataFrame 和列表）
if not hasattr(hs300_res, 'data') or len(hs300_res.data) == 0:
    print("返回数据为空！")
    THS_iFinDLogout()
    sys.exit()

# 打印第一行样例
if isinstance(hs300_res.data, pd.DataFrame):
    print("第一行数据样例：", hs300_res.data.iloc[0].to_dict())
else:
    print("第一行数据样例：", hs300_res.data[0])

if hs300_res.errorcode != 0:
    print(f"获取沪深300数据失败：{hs300_res.errmsg}")
    THS_iFinDLogout()
    sys.exit()

# 转为 DataFrame（若已是 DataFrame 则直接使用）
df_hs300 = pd.DataFrame(hs300_res.data)
print("沪深300数据列名：", df_hs300.columns.tolist())

# 确定时间列和价格列
time_col = 'time' if 'time' in df_hs300.columns else df_hs300.columns[0]
price_col = 'close' if 'close' in df_hs300.columns else df_hs300.columns[1]

df_hs300['date'] = pd.to_datetime(df_hs300[time_col]).dt.date
df_hs300 = df_hs300.sort_values('date').drop_duplicates('date')

trade_dates = df_hs300['date'].tolist()
print(f"获取到 {len(trade_dates)} 个交易日")

base_date = pd.to_datetime(START_DATE).date()
if base_date not in trade_dates:
    new_base = trade_dates[0]
    print(f"警告：基期 {START_DATE} 不是交易日，自动使用第一个交易日 {new_base} 作为新基期")
    START_DATE = new_base.strftime('%Y-%m-%d')
    base_date = new_base

base_hs300 = df_hs300.loc[df_hs300['date'] == base_date, price_col].iloc[0]
df_hs300['hs300_nav'] = df_hs300[price_col] / base_hs300

# ==================== 获取所有股票的日线收盘价 ====================
print("\n正在获取个股日线收盘价...")
codes_str = ','.join(conv_codes)
stock_res = THS_HQ(codes_str, 'close', '', START_DATE, end_date_str)
if stock_res.errorcode != 0:
    print(f"获取个股数据失败：{stock_res.errmsg}")
    THS_iFinDLogout()
    sys.exit()

if not hasattr(stock_res, 'data') or len(stock_res.data) == 0:
    print("个股返回数据为空！")
    THS_iFinDLogout()
    sys.exit()

df_stock = pd.DataFrame(stock_res.data)
print("个股数据列名：", df_stock.columns.tolist())

# 确定列名
time_col_stock = 'time' if 'time' in df_stock.columns else df_stock.columns[0]
code_col_stock = 'thscode' if 'thscode' in df_stock.columns else df_stock.columns[1]
price_col_stock = 'close' if 'close' in df_stock.columns else df_stock.columns[2]

df_stock['date'] = pd.to_datetime(df_stock[time_col_stock]).dt.date
df_stock['股票代码'] = df_stock[code_col_stock].map(rev_code_map)

# 处理映射失败的代码
if df_stock['股票代码'].isna().any():
    missing = df_stock.loc[df_stock['股票代码'].isna(), code_col_stock].unique()
    print(f"警告：以下代码映射失败，将使用原始返回代码：{missing}")
    df_stock['股票代码'].fillna(df_stock[code_col_stock], inplace=True)

# 透视得到收盘价矩阵
df_close = df_stock.pivot(index='date', columns='股票代码', values=price_col_stock)
df_close = df_close.reindex(trade_dates).ffill().bfill()
print("个股价格数据形状：", df_close.shape)

# ==================== 模拟投资组合交易 ====================
cash = INIT_CAPITAL
positions = {}
nav_list = []
trade_by_date = {date: group for date, group in df_trade.groupby('日期')}

print("开始逐日计算净值...")
for date in trade_dates:
    if date in trade_by_date:
        day_trades = trade_by_date[date]
        for _, row in day_trades.iterrows():
            code = row['股票代码']
            action = row['买入/卖出']
            ratio = row['仓位']
            price = row['成交价']
            target_amount = INIT_CAPITAL * ratio

            if code not in df_close.columns:
                print(f"  警告：{date} 股票 {code} 无价格数据，跳过交易")
                continue

            if action == '买入':
                shares = int(target_amount / price / TRADE_UNIT) * TRADE_UNIT
                if shares == 0:
                    print(f"  警告：{date} {code} 买入股数为0，跳过")
                    continue
                cost = shares * price
                if cash < cost:
                    shares = int(cash / price / TRADE_UNIT) * TRADE_UNIT
                    if shares == 0:
                        continue
                    cost = shares * price
                cash -= cost
                positions[code] = positions.get(code, 0) + shares

            elif action == '卖出':
                if code not in positions or positions[code] == 0:
                    print(f"  警告：{date} {code} 持仓为0，无法卖出")
                    continue
                shares = int(target_amount / price / TRADE_UNIT) * TRADE_UNIT
                shares = min(shares, positions[code])
                if shares == 0:
                    print(f"  警告：{date} {code} 卖出股数为0，跳过")
                    continue
                revenue = shares * price
                cash += revenue
                positions[code] -= shares
                if positions[code] == 0:
                    del positions[code]
            else:
                print(f"  未知操作：{action}")

    market_value = 0.0
    for code, shares in positions.items():
        if code not in df_close.columns:
            print(f"  警告：{date} 股票 {code} 无价格数据，按0计算市值")
            continue
        close_price = df_close.loc[date, code]
        market_value += shares * close_price
    total_asset = cash + market_value
    nav = total_asset / INIT_CAPITAL

    hs300_nav = df_hs300.loc[df_hs300['date'] == date, 'hs300_nav'].iloc[0]
    nav_list.append({'日期': date, '组合净值': nav, '沪深300净值': hs300_nav})

df_nav = pd.DataFrame(nav_list)
df_nav['日期'] = pd.to_datetime(df_nav['日期'])
df_nav_to_excel = df_nav.copy()
df_nav_to_excel['日期'] = df_nav_to_excel['日期'].dt.strftime('%Y-%m-%d')
# ==================== 写入 Excel ====================
print("\n正在保存净值数据到 Excel...")
try:
    with pd.ExcelWriter(DATA_PATH, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        df_nav.to_excel(writer, sheet_name='净值', index=False)
except FileNotFoundError:
    with pd.ExcelWriter(DATA_PATH, engine='openpyxl') as writer:
        df_nav.to_excel(writer, sheet_name='净值', index=False)
print("已更新工作表“净值”")

# ==================== 绘制净值图 ====================
# 将日期转换为字符串，用于等距显示（剔除周末/节假日间隔）
dates_str = df_nav['日期'].dt.strftime('%Y-%m-%d')
x = range(len(dates_str))

plt.figure(figsize=(12, 6))
plt.plot(x, df_nav['组合净值'], color='red', linewidth=2, label='投资组合净值')
plt.plot(x, df_nav['沪深300净值'], color='blue', linewidth=2, label='沪深300净值')
plt.xlabel('日期')
plt.ylabel('净值')
plt.title(f'短线王 vs 沪深300 净值走势（基期：{START_DATE}）')
plt.legend()
plt.grid(True, linestyle='--', alpha=0.6)

# 设置x轴刻度：每隔n个交易日显示一个标签，避免拥挤
n = max(1, len(dates_str) // 15)  # 大约显示15个标签
plt.xticks(x[::n], dates_str[::n], rotation=45)

# ====== 标注投资组合净值的显著峰值点 ======
nav_values = df_nav['组合净值'].values
# 寻找局部最大值（比前后点都大）
peak_indices = []
for i in range(1, len(nav_values)-1):
    if nav_values[i] > nav_values[i-1] and nav_values[i] > nav_values[i+1]:
        peak_indices.append(i)

# 计算y轴范围，用于偏移量
y_min, y_max = plt.ylim()
y_range = y_max - y_min

for idx in peak_indices:
    x_peak = x[idx]
    y_peak = nav_values[idx]
    plt.annotate(f'{y_peak:.3f}', 
                 xy=(x_peak, y_peak), 
                 xytext=(x_peak, y_peak + 0.02 * y_range),  # 向上偏移2%的图幅高度
                 ha='center',
                 fontsize=8,
                 arrowprops=dict(arrowstyle='-', color='gray', lw=0.5, alpha=0.7)
                 )

last_idx = len(x) - 1
last_x = x[last_idx]
last_y = nav_values[last_idx]
plt.annotate(f'{last_y:.3f}',
             xy=(last_x, last_y),
             xytext=(last_x, last_y + 0.02 * y_range),  # 同样向上偏移
             ha='center',
             fontsize=8,
             color='black',  # 黑色字体
             bbox=dict(boxstyle="round,pad=0.3", edgecolor='black', facecolor='white'),  # 黑色边框，白色背景
             arrowprops=dict(arrowstyle='-', color='gray', lw=0.5, alpha=0.7)
             )
# ==============================================

plt.tight_layout()
img_path = r'D:\办公\净值图.png'
plt.savefig(img_path, dpi=300)
plt.show()
print(f"净值图已保存至：{img_path}")

# ==================== 登出 ====================
THS_iFinDLogout()
print("程序运行完毕。")