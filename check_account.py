"""
Read-only Polymarket CLOB V2 Account & Allowance Verification Script (`pUSD` / `py-clob-client-v2`).
Does NOT place any orders. Safe to run repeatedly before enabling live trading.
"""
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from py_clob_client_v2 import AssetType, BalanceAllowanceParams, ClobClient
import requests

CLOB_HOST = "https://clob.polymarket.com"
POLYGON_CHAIN_ID = 137
POLYGON_RPC_URL = "https://polygon-bor-rpc.publicnode.com"
PUSD_CONTRACT = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
USDC_E_CONTRACT = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
USDC_NATIVE_CONTRACT = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"


def _erc20_balance(token_addr: str, wallet_addr: str) -> float | None:
    if not wallet_addr or not wallet_addr.startswith("0x") or len(wallet_addr) != 42:
        return None
    data = "0x70a08231" + wallet_addr[2:].lower().rjust(64, "0")
    try:
        r = requests.post(
            POLYGON_RPC_URL,
            json={"jsonrpc": "2.0", "id": 1, "method": "eth_call", "params": [{"to": token_addr, "data": data}, "latest"]},
            timeout=5,
        )
        res = r.json().get("result")
        if res and res.startswith("0x"):
            return int(res, 16) / 1e6
    except Exception:
        pass
    return None


def main() -> int:
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        print(f"❌ 找不到設定檔: {env_path}")
        print("   請先在專案資料夾建立 .env 檔案（注意檔名不要變成 .env.txt）。")
        return 1

    load_dotenv(dotenv_path=env_path)

    private_key = (os.getenv("POLY_PRIVATE_KEY") or "").strip()
    funder = (os.getenv("POLY_FUNDER_ADDRESS") or "").strip()
    sig_type_raw = (os.getenv("POLY_SIGNATURE_TYPE") or "2").strip()

    if not private_key or "貼上" in private_key:
        print("❌ .env 內的 POLY_PRIVATE_KEY 尚未填寫。")
        return 1
    if not private_key.startswith("0x"):
        private_key = "0x" + private_key

    try:
        sig_type = int(sig_type_raw)
    except ValueError:
        print(f"❌ POLY_SIGNATURE_TYPE 格式錯誤: {sig_type_raw} (應為 0, 1, 2 或 3)")
        return 1

    if sig_type in (1, 2, 3) and (not funder or "貼上" in funder):
        print("❌ 使用 Proxy 模式 (POLY_SIGNATURE_TYPE=1/2/3) 時，必須填寫 POLY_FUNDER_ADDRESS！")
        return 1

    print("=" * 72)
    print(" Polymarket CLOB V2 實盤帳戶連線檢查（pUSD 唯讀測試，不會下單）")
    print("=" * 72)
    print(f" 簽章模式 (signature_type) : {sig_type}")
    print(f" 代理錢包 (funder)         : {funder or '(EOA 模式，使用私鑰對應地址)'}")

    try:
        client = ClobClient(
            host=CLOB_HOST,
            chain_id=POLYGON_CHAIN_ID,
            key=private_key,
            signature_type=sig_type,
            funder=funder or None,
        )
        print(f" 簽章地址 (signer)         : {client.get_address()}")

        # 1. Check CLOB server connectivity
        server_ok = client.get_ok()
        print(f" 伺服器連線狀態            : {server_ok}")

        # 2. Derive V2 L2 API Credentials
        creds = client.create_or_derive_api_key()
        client.set_api_creds(creds)
        masked_key = f"{creds.api_key[:6]}...{creds.api_key[-4:]}" if len(creds.api_key) > 10 else "***"
        print(f" ✅ V2 API 憑證衍生成功     : api_key={masked_key}")

        # 3. Sync CLOB cache & query pUSD collateral balance / allowances
        bal_params = BalanceAllowanceParams(
            asset_type=AssetType.COLLATERAL,
            signature_type=sig_type,
        )
        try:
            client.update_balance_allowance(params=bal_params)
        except Exception:
            pass
        bal_info = client.get_balance_allowance(params=bal_params)
        raw_bal = float(bal_info.get("balance", 0))
        pusd_bal = raw_bal / 1e6
        allowances = bal_info.get("allowances", {})
        approved_cnt = sum(1 for v in allowances.values() if int(v) > 0) if isinstance(allowances, dict) else 0

        print(f" 💰 可用 pUSD 餘額         : ${pusd_bal:,.2f} pUSD")
        print(f" 🔓 V2 合約授權 (Allowances): {approved_cnt}/{len(allowances) if isinstance(allowances, dict) else 0} 個交換合約已授權")

        # 4. Verify L2 authenticated open orders
        open_orders = client.get_open_orders()
        print(f" 📋 目前掛單數 (Open)      : {len(open_orders) if isinstance(open_orders, list) else 0} 筆")
        print("=" * 72)

        if pusd_bal <= 0:
            print("⚠️  提醒：CLOB V2 回報餘額為 0，正在核對 Polygon 鏈上實際餘額...")
            signer_addr = client.get_address()
            for label, addr in [("funder (代理錢包)", funder), ("signer (簽章地址)", signer_addr)]:
                if not addr:
                    continue
                b_p = _erc20_balance(PUSD_CONTRACT, addr)
                b_e = _erc20_balance(USDC_E_CONTRACT, addr)
                b_n = _erc20_balance(USDC_NATIVE_CONTRACT, addr)
                print(
                    f"    - {label} {addr}: "
                    f"pUSD=${b_p if b_p is not None else '?'} | "
                    f"USDC.e=${b_e if b_e is not None else '?'} | "
                    f"原生USDC=${b_n if b_n is not None else '?'}"
                )
            print("   👉 若鏈上 pUSD 有錢但 V2 API 仍顯示 0，請嘗試將 .env 的 POLY_SIGNATURE_TYPE 改為 1 或 3。")
        elif approved_cnt == 0:
            print("⚠️  提醒：尚未偵測到 pUSD 合約授權，請在官網手動買賣過一筆 $1 即可免費自動開啟。")
        else:
            print("🎉 恭喜！V2 帳戶 pUSD 餘額、API 憑證與授權全部就緒，可以開始跑實盤策略！")
        return 0

    except Exception as exc:
        print(f"\n❌ 連線或驗證失敗: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
