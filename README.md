# Polymarket BTC 5m 漲跌盤量化系統 (Chainlink-Oracle High-Prob Sniper)

專門針對 Polymarket **`btc-updown-5m`**（每 5 分鐘結算一次的二元數位選擇權盤口）設計的高勝率、正期望值（+EV）量化數據採集、定價與模擬回測框架。

---

## 核心架構：100% 對齊 Chainlink 預言機 + CEX 領先雷達

1. **獨立四層級 Chainlink `BTC/USD` 預言機引擎 (`src/chainlink_oracle.py`)**
   開盤基準價（Strike $S_0$）、盤中安全距離（$Z$-score）與收盤結算價（$S_T$）**嚴格採用 Chainlink 報價，絕不拿幣安/Coinbase 現貨價冒充預言機**。系統支援由高到低 4 層自動切換（預設免 API Key 即可直接運作）：
   - **Tier 1 — Chainlink Data Streams 官方原生直連** (`wss://ws.dataengine.chain.link/api/v1/ws`, Feed ID `0x00039d9e...`)：內建 HMAC-SHA256 簽章與純 Python `ReportV3` 二進位解碼器（設定 `CHAINLINK_STREAMS_API_KEY` 與 `CHAINLINK_STREAMS_API_SECRET` 環境變數即自動啟用）。
   - **Tier 2 — Polymarket PolyBolt V2 參考價與 60s TWAP 流** (`wss://ws-live-v2.polymarket.com/ws`)：同步訂閱 `price.crypto` 與 `price.crypto.twap`（設定 `POLY_API_KEY`, `POLY_API_SECRET`, `POLY_PASSPHRASE` 自動啟用）。
   - **Tier 3 — Polymarket RTDS 公開 Chainlink 流** (`wss://ws-live-data.polymarket.com` -> `crypto_prices_chainlink`)：**免任何金鑰開箱即用**。
   - **Tier 4 — Polygon 鏈上 Chainlink 合約直讀備援** (`0xc907E116054Ad103354f2D350FD2514433D57F6f` `latestRoundData()`)：直接透過 Polygon JSON-RPC `eth_call` 解碼鏈上最新區塊報價，並對齊 Gamma API 官方公佈的 `eventMetadata.priceToBeat`。

2. **「預言機價差（Oracle Gap）」防反殺與領先確認**
   - 雖然結算裁判是 Chainlink，但價格發現（Price Discovery）是 Binance 與 Coinbase 先動、Chainlink 數秒後跟上。
   - 系統即時計算 `oracle_gap_bps = (CEX - Chainlink) / Chainlink * 10000`：
     - 若 Chainlink 表面領先起點價，但 CEX 已經閃崩跌破 Chainlink 超過 `6 bps`（代表接下來幾秒 Chainlink 即將被往下拉），**自動攔截假訊號不進場**。
     - 若 CEX 領先方向與 Chainlink 順向一致，則確認安全墊更穩固。

3. **厚尾二元期權定價 + 動態 Taker Fee + 伺服器 UTC 校時與 VWAP 深度過濾**
   - 內建自動伺服器 UTC 時間校準（消除本機系統時鐘誤差導致的 Strike $S_0$ 偏移）與 25 U 實盤掛單深度 VWAP 計算（過濾 0.5 股碎單假報價）。
   - 避開前 110 秒五五波高費率震盪區（$p=0.50$ 時 Taker Fee 高達本金 3.5%）。
   - 在 `110s ~ 255s` 區間使用 Student-$t$ 厚尾分佈評估真實勝率 $P_{\text{true}}$，要求扣除動態手續費 $0.07 \times p(1-p)$ 後 **Net EV $\ge +3.5\%$**、點差 $\le \$0.06$ 且買價 $\le \$0.84$；出場採用期望值智慧停利（$\ge \$0.99$ 或盤口淨出價 $\ge P_{\text{true}}$ 才提前鎖利，避免無腦在 $\$0.96$ 讓利）。

---

## 快速啟動

```powershell
cd C:\Users\padwu\.gemini\antigravity\scratch\polymarket-btc-5m
pip install -r requirements.txt

# 1. 執行單元測試（含 Chainlink ABI 解碼與 Oracle Gap 防護驗證）
python -m unittest discover -s tests -v

# 2. 執行 2,000 盤 Chainlink 結算與滯後價差模擬基準測試
python backtest.py --demo --windows 2000

# 3. 啟動即時 Chainlink + Polymarket 盤口錄製與模擬交易
python run_monitor.py
```

*(選配進階環境變數)*：若日後申請了 Polymarket L2 API Key 或 Chainlink Data Streams Key，只需在執行前設定環境變數，系統會自動無縫升級至 Tier 1 / Tier 2 毫秒級通道。
