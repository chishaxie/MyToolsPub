### 标准处理流程

1. 下载A股清单、交易日历、日数据 `python ./down_a.py`
2. 下载港股清单、日数据 `python ./down_hk.py` (同1并发)
3. 更新港股交易日历 `python ./trading_day_hk` (单独调用)
	1. 下载不依赖
