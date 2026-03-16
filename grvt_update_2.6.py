#!/usr/bin/env python3
"""
GRVT Market-Making Bot
======================
Strategy:
  1. Place limit BUY near best_bid and limit SELL near best_ask simultaneously
  2. Capture the spread when either side fills
  3. When one side fills → manage position by placing the opposite order to return neutral
  4. Continuously refresh quotes when price moves away from current orders
  5. Emergency market close if position risk exceeds hard limits

Signing mirrors the SDK grvt_raw_signing.py exactly.
"""

import os, sys, time, json, math, random, signal, logging, argparse
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

import requests
from eth_account import Account
from eth_account.messages import encode_typed_data

# ─── Logging ──────────────────────────────────────────────────────────────────
def _safe_stream_handler():
    try:
        import io
        if sys.stdout.encoding and "utf" not in sys.stdout.encoding.lower():
            return logging.StreamHandler(io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace"))
    except Exception:
        pass
    return logging.StreamHandler(sys.stdout)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[_safe_stream_handler(), logging.FileHandler("grvt_bot.log", encoding="utf-8")],
)
log = logging.getLogger("grvt_mm")

trade_log = logging.getLogger("grvt_trades")
_th = logging.FileHandler("grvt_trades.log", encoding="utf-8")
_th.setFormatter(logging.Formatter("%(asctime)s  %(message)s"))
trade_log.addHandler(_th)
trade_log.setLevel(logging.INFO)

# ─── Constants ────────────────────────────────────────────────────────────────
PRICE_MULTIPLIER = 1_000_000_000
CHAIN_IDS = {"testnet": 326, "prod": 325}

# Hardcoded fallback min sizes — updated from live exchange rejection errors.
# Bot prefers API-provided values when available; these are the safety net.
MIN_ORDER_SIZE: Dict[str, float] = {
    "BTC_USDT_Perp":  0.001,
    "ETH_USDT_Perp":  0.002,
    "SOL_USDT_Perp":  0.01,
    "ARB_USDT_Perp":  1.0,
    "BNB_USDT_Perp":  0.01,
    "DOGE_USDT_Perp": 10.0,
}
MIN_ORDER_SIZE_FALLBACK = 0.002

# Minimum notional value (size × price) required by exchange — error code 2066
MIN_NOTIONAL: Dict[str, float] = {
    "BTC_USDT_Perp":  100.0,
    "ETH_USDT_Perp":  10.0,
    "SOL_USDT_Perp":  10.0,
    "ARB_USDT_Perp":  10.0,
    "BNB_USDT_Perp":  10.0,
    "DOGE_USDT_Perp": 10.0,
}
MIN_NOTIONAL_FALLBACK = 100.0

class SignTimeInForce(Enum):
    GOOD_TILL_TIME      = 1
    ALL_OR_NONE         = 2
    IMMEDIATE_OR_CANCEL = 3
    FILL_OR_KILL        = 4

TIME_IN_FORCE_TO_SIGN = {
    "GOOD_TILL_TIME":      SignTimeInForce.GOOD_TILL_TIME.value,
    "ALL_OR_NONE":         SignTimeInForce.ALL_OR_NONE.value,
    "IMMEDIATE_OR_CANCEL": SignTimeInForce.IMMEDIATE_OR_CANCEL.value,
    "FILL_OR_KILL":        SignTimeInForce.FILL_OR_KILL.value,
}

EIP712_ORDER_MESSAGE_TYPE = {
    "Order": [
        {"name": "subAccountID",  "type": "uint64"},
        {"name": "isMarket",      "type": "bool"},
        {"name": "timeInForce",   "type": "uint8"},
        {"name": "postOnly",      "type": "bool"},
        {"name": "reduceOnly",    "type": "bool"},
        {"name": "legs",          "type": "OrderLeg[]"},
        {"name": "nonce",         "type": "uint32"},
        {"name": "expiration",    "type": "int64"},
    ],
    "OrderLeg": [
        {"name": "assetID",           "type": "uint256"},
        {"name": "contractSize",      "type": "uint64"},
        {"name": "limitPrice",        "type": "uint64"},
        {"name": "isBuyingContract",  "type": "bool"},
    ],
}

def get_EIP712_domain_data(env: str) -> dict:
    return {"name": "GRVT Exchange", "version": "0", "chainId": CHAIN_IDS[env]}

def norm_size(x) -> str:
    return str(Decimal(str(x)).quantize(Decimal("0.000000001"), rounding=ROUND_DOWN))

def norm_price(x) -> str:
    return str(Decimal(str(x)).quantize(Decimal("0.000000001"), rounding=ROUND_DOWN))

# ─── Instrument Cache ──────────────────────────────────────────────────────────
class InstrumentCache:
    def __init__(self, env: str = "testnet"):
        b = "testnet.grvt.io" if env == "testnet" else "grvt.io"
        self._url = f"https://market-data.{b}/full/v1/all_instruments"
        self._cache: Dict[str, dict] = {}

    def load(self, sess: requests.Session = None):
        caller = sess if sess else requests
        r = caller.post(self._url, json={"kind": ["PERPETUAL"], "base": [], "quote": []}, timeout=10)
        r.raise_for_status()
        data = r.json()
        instruments = data.get("result", [])
        print(instruments)
        if isinstance(instruments, dict):
            instruments = instruments.get("instruments", [])
        for instr in instruments:
            name = instr.get("instrument") or instr.get("instrument_name") or instr.get("symbol", "")
            if name:
                self._cache[name] = instr
        log.info(f"InstrumentCache: loaded {len(self._cache)} instruments")
        if self._cache:
            k = next(iter(self._cache))
            s = self._cache[k]
            min_val = s.get("min_size") or s.get("minimum_size") or "N/A"
            log.info(f"  Sample '{k}': base_decimals={s.get('base_decimals')} min_size={min_val}")

    def get(self, name: str) -> dict:
        if not self._cache:
            raise RuntimeError("InstrumentCache.load() has not been called")
        if name not in self._cache:
            raise KeyError(f"'{name}' not in cache. Available: {list(self._cache.keys())[:5]}")
        return self._cache[name]

    def get_min_size(self, name: str, fallback: float = 0.0) -> float:
        try:
            instr = self.get(name)
            for field in ("min_size", "minimum_size", "min_contract_size", "minSize", "lot_size"):
                val = instr.get(field)
                if val is not None:
                    sz = float(val)
                    if sz > 0:
                        return sz
        except Exception:
            pass
        return fallback

# ─── sign_order ────────────────────────────────────────────────────────────────
def sign_order(account, domain_data, sub_account_id, is_market, time_in_force,
               post_only, reduce_only, nonce, expiration,
               instrument, size, limit_price, is_buying_asset, instruments) -> dict:
    instr_meta      = instruments.get(instrument)
    size_multiplier = 10 ** instr_meta["base_decimals"]
    # Use Decimal arithmetic to avoid float precision errors in contractSize
    raw_cs = int(Decimal(str(size)) * Decimal(str(size_multiplier)))
    # Round down to the nearest lot size (min_size × base_multiplier)
    min_sz = instruments.get_min_size(instrument, fallback=0.001)
    lot    = max(1, int(Decimal(str(min_sz)) * Decimal(str(size_multiplier))))
    cs     = (raw_cs // lot) * lot
    if cs <= 0:
        raise ValueError(f"contractSize={cs} after lot rounding (raw={raw_cs}, lot={lot})")
    legs = [{
        "assetID":          instr_meta["instrument_hash"],
        "contractSize":     cs,
        "limitPrice":       int(Decimal(str(limit_price)) * Decimal(str(PRICE_MULTIPLIER))),
        "isBuyingContract": is_buying_asset,
    }]
    message_data = {
        "subAccountID": sub_account_id,
        "isMarket":     is_market or False,
        "timeInForce":  TIME_IN_FORCE_TO_SIGN[time_in_force],
        "postOnly":     post_only or False,
        "reduceOnly":   reduce_only or False,
        "legs":         legs,
        "nonce":        nonce,
        "expiration":   expiration,
    }
    signature  = encode_typed_data(domain_data, EIP712_ORDER_MESSAGE_TYPE, message_data)
    signed_msg = account.sign_message(signature)
    # Compute the exact decimal size that matches contractSize — must match what we send
    signed_size = str(Decimal(cs) / Decimal(size_multiplier))
    log.info(f"  Signed: contractSize={cs} limitPrice={legs[0]['limitPrice']} nonce={nonce} size={signed_size}")
    return {
        "signer":      str(account.address),
        "r":           "0x" + hex(signed_msg.r)[2:].zfill(64),
        "s":           "0x" + hex(signed_msg.s)[2:].zfill(64),
        "v":           signed_msg.v,
        "expiration":  str(expiration),
        "nonce":       nonce,
        "chain_id":    str(domain_data["chainId"]),
        "signed_size": signed_size,
    }

# ─── Configuration ────────────────────────────────────────────────────────────
class Config:
    def __init__(self):
        self.env                = "testnet"
        self.api_key            = ""
        self.signer_private_key = ""
        self.sub_account_id     = ""
        self.instrument         = "ETH_USDT_Perp"
        self._order_size        = 0.002      # fallback size when equity is not yet available
        self._tick_size         = 0.5
        self.order_expiry_h     = 1.0        # order time-to-live in hours (orders sit open, no time-based cancel)
        self.dry_run            = False

        # Position sizing
        self.trade_size_pct     = 0.10       # 10% of equity per order (each side)
        self.max_exposure_pct   = 0.50       # stop opening new quotes above 50% equity exposure

        # Quote refresh: cancel and replace existing quotes if price drifts beyond this threshold
        self.stale_bps          = 5.0

        # Quote placement: how many ticks to offset from best bid/ask
        self.quote_offset_ticks = 0          # 0 = join the best bid/ask directly

        # Hard risk limits
        self.hard_loss_pct      = 0.005      # emergency market close if unrealized loss > 0.5% of equity
        self.max_position_notional = 500.0   # absolute position cap in USD regardless of equity

        # Timing (main loop, session check, cancel retry, emergency)
        self.cycle_sleep_sec             = 2.0
        self.session_check_sec           = 300
        self.cancel_retry_count          = 3
        self.cancel_retry_delay_sec      = 0.25
        self.emergency_close_delay_sec   = 0.5
        self.emergency_verify_sleep_sec  = 1.0
        self.startup_cancel_sleep_sec    = 0.5
        self.no_price_wait_sec           = 3
        self.after_emergency_sleep_sec   = 2

    @property
    def order_size(self): return float(self._order_size)
    @order_size.setter
    def order_size(self, v): self._order_size = float(v)

    @property
    def tick_size(self): return float(self._tick_size)
    @tick_size.setter
    def tick_size(self, v): self._tick_size = float(v)

# ─── GRVT REST Client ─────────────────────────────────────────────────────────
class GRVTClient:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        b = "testnet.grvt.io" if cfg.env == "testnet" else "grvt.io"
        self.edge   = f"https://edge.{b}"
        self.trades = f"https://trades.{b}"
        self.market = f"https://market-data.{b}"
        self.sess   = requests.Session()
        self.account_id = ""
        self._login()

    def _login(self):
        r = requests.post(
            f"{self.edge}/auth/api_key/login",
            json={"api_key": self.cfg.api_key},
            headers={"Content-Type": "application/json", "Cookie": "rm=true;"},
            timeout=15,
        )
        r.raise_for_status()
        set_cookie = r.headers.get("Set-Cookie", "")
        gravity = next((p.strip() for p in set_cookie.split(";") if p.strip().startswith("gravity=")), None)
        if not gravity:
            raise ValueError("gravity cookie missing from login response")
        self.account_id = r.headers.get("X-Grvt-Account-Id", "")
        self.sess.headers.update({
            "Content-Type":       "application/json",
            "Cookie":             gravity,
            "X-Grvt-Account-Id":  self.account_id,
            "X-Grvt-Api-Key":     self.cfg.api_key,
        })
        log.info(f"Authenticated. account_id={self.account_id}")

    def _ensure_session(self):
        """Heartbeat — re-login if session expired."""
        try:
            self.get_ticker(self.cfg.instrument)
        except requests.HTTPError as e:
            if e.response.status_code in (401, 403):
                log.warning("Session expired — re-logging in")
                self._login()

    def _post(self, base: str, path: str, data: dict) -> dict:
        url = f"{base}{path}"
        try:
            r = self.sess.post(url, json=data, timeout=10)
            r.raise_for_status()
            return r.json()
        except requests.HTTPError:
            log.error(f"POST {url} {r.status_code}: {r.text[:400]}")
            raise

    def get_ticker(self, instr: str) -> dict:
        """Fetch current best bid, best ask, mid and mark price from the ticker endpoint."""
        try:
            result = self._post(self.market, "/full/v1/ticker", {"instrument": instr}).get("result", {})
            return {
                "mark_price": float(result.get("mark_price") or 0),
                "best_bid":   float(result.get("best_bid_price") or 0),
                "best_ask":   float(result.get("best_ask_price") or 0),
                "mid_price":  float(result.get("mid_price") or 0),
            }
        except Exception as e:
            log.warning(f"get_ticker: {e}")
            return {"mark_price": 0.0, "best_bid": 0.0, "best_ask": 0.0, "mid_price": 0.0}

    def get_mark_price(self, instr: str) -> float:
        t = self.get_ticker(instr)
        return t["mark_price"] or t["mid_price"]

    def get_sub_account_summary(self, sub_id: str) -> dict:
        raw = self._post(self.trades, "/full/v1/account_summary", {"sub_account_id": str(sub_id)})
        return raw.get("result", raw)

    def get_positions(self, sub_id: str) -> List[dict]:
        raw = self._post(self.trades, "/full/v1/positions", {"sub_account_id": str(sub_id)})
        return raw.get("positions", [])

    def get_open_orders(self, sub_id: str) -> Dict[str, dict]:
        raw = self._post(self.trades, "/full/v1/open_orders", {"sub_account_id": str(sub_id)})
        if isinstance(raw, list):
            orders = raw
        else:
            result = raw.get("result", raw)
            orders = result if isinstance(result, list) else result.get("orders", [])
        # Index by client_order_id — GRVT testnet returns order_id=0x00 so we track by our own id
        return {
            str(o.get("metadata", {}).get("client_order_id", o.get("order_id", ""))): o
            for o in orders
        }

    def create_order(self, sub_id, is_buying, instr, size, price,
                     post_only, reduce_only, sig, client_oid, is_market=False) -> dict:
        payload = {"order": {
            "sub_account_id": str(sub_id),
            "is_market":      is_market,
            "time_in_force":  "IMMEDIATE_OR_CANCEL" if is_market else "GOOD_TILL_TIME",
            "post_only":      post_only,
            "reduce_only":    reduce_only,
            "legs": [{"instrument": instr, "size": size,
                      "limit_price": "0" if is_market else price,
                      "is_buying_asset": is_buying}],
            "signature": sig,
            "metadata": {"client_order_id": str(client_oid)},
        }}
        if self.cfg.dry_run:
            log.info(f"[DRY] {'MKT' if is_market else 'LMT'} {'BUY' if is_buying else 'SELL'} {size} @ {price}")
            return {"order_id": f"dry_{int(time.time()*1000)}"}
        return self._post(self.trades, "/full/v1/create_order", payload)

    def cancel_order(self, sub_id: str, order_id: str):
        if self.cfg.dry_run: return
        try:
            self._post(self.trades, "/full/v1/cancel_order",
                       {"sub_account_id": str(sub_id), "client_order_id": str(order_id)})
        except Exception as e:
            log.warning(f"Cancel {order_id}: {e}")

    def cancel_all(self, sub_id: str, instr: str = None):
        if self.cfg.dry_run:
            log.info("[DRY] cancel_all"); return
        try:
            payload = {"sub_account_id": str(sub_id)}
            if instr:
                parts = instr.split("_")
                if len(parts) == 3 and parts[2] == "Perp":
                    payload.update({"kind": ["PERPETUAL"], "base": [parts[0]], "quote": [parts[1]]})
            self._post(self.trades, "/full/v1/cancel_all_orders", payload)
        except Exception as e:
            log.warning(f"Cancel all: {e}")

# ─── Data classes ─────────────────────────────────────────────────────────────
@dataclass
class Position:
    size:      float = 0.0
    avg_entry: float = 0.0

    @property
    def is_flat(self):  return abs(self.size) < 1e-12
    @property
    def is_long(self):  return self.size > 1e-12
    @property
    def is_short(self): return self.size < -1e-12

    def pnl(self, px: float) -> float:
        return self.size * (px - self.avg_entry)

@dataclass
class MO:
    """Managed Order — tracks a live order on the book."""
    client_oid: str
    is_buy:     bool
    price:      float
    size:       float

# ─── Market Maker ─────────────────────────────────────────────────────────────
class MarketMaker:
    """
    Two-sided market maker:
      - Places limit BUY at best_bid and limit SELL at best_ask
      - When one side fills, manages the open position with a close order
      - Continuously refreshes stale quotes based on order book movement
      - Falls back to market close if position risk exceeds hard limits
    """

    def __init__(self, cfg: Config):
        self.cfg         = cfg
        self.client      = GRVTClient(cfg)
        self.account     = Account.from_key(cfg.signer_private_key)
        self.domain      = get_EIP712_domain_data(cfg.env)
        self.instruments = InstrumentCache(cfg.env)
        self.instruments.load(self.client.sess)

        self.pos              = Position()
        # Two-sided quote slots — one order per side at all times
        self.quotes: Dict[str, Optional[MO]] = {"buy": None, "sell": None}
        # Close order placed after a fill to return position to flat
        self.close_mo: Optional[MO] = None

        self.equity           = 0.0
        self.internal_sub_id  = ""
        self._running         = False
        self._last_session_check = 0.0
        self._last_quote_time    = 0.0

        # Monotonic nonce counter with random seed to avoid duplicate nonce errors
        self._nonce_counter = random.randint(1, 2**20) * 1000

        # Track entry price and time for trade P&L logging
        self._entry_px = 0.0
        self._entry_ts = 0.0

    # ── Nonce ─────────────────────────────────────────────────────────────────
    def _next_nonce(self) -> int:
        self._nonce_counter = (self._nonce_counter + 1) % (2**32)
        return self._nonce_counter

    # ── Size helpers ──────────────────────────────────────────────────────────
    def _min_size(self) -> float:
        hardcoded = MIN_ORDER_SIZE.get(self.cfg.instrument, MIN_ORDER_SIZE_FALLBACK)
        api_min   = self.instruments.get_min_size(self.cfg.instrument, fallback=0.0)
        return api_min if api_min > 0 else hardcoded

    def _min_notional(self) -> float:
        return MIN_NOTIONAL.get(self.cfg.instrument, MIN_NOTIONAL_FALLBACK)

    def _valid_size(self, size: float, price: float) -> float:
        """
        Return the smallest valid size >= the requested size that satisfies:
          1. size >= exchange min_size  (avoids error 2062)
          2. size * price >= min_notional  (avoids error 2066)
        Result is always a multiple of min_size.
        """
        min_sz  = self._min_size()
        min_not = self._min_notional()
        size = max(size, min_sz)
        if price > 0 and size * price < min_not:
            size = min_not / price
        steps = math.ceil(size / min_sz)
        return round(steps * min_sz, 9)

    def _trade_size(self, price: float) -> float:
        """Compute order size as a percentage of equity, floored to exchange minimums."""
        if self.equity > 0 and price > 0:
            raw = (self.equity * self.cfg.trade_size_pct) / price
        else:
            raw = self.cfg.order_size
        return self._valid_size(raw, price)

    # ── Submit ────────────────────────────────────────────────────────────────
    def _submit(self, is_buying: bool, price: float, size: float,
                post_only: bool = True, reduce_only: bool = False,
                is_market: bool = False) -> Optional[MO]:
        if not self.internal_sub_id:
            log.error("internal_sub_id missing"); return None

        # Final safety check before sending — prevents exchange errors 2062 and 2066
        min_sz  = self._min_size()
        min_not = self._min_notional()
        if size < min_sz:
            log.warning(f"  Skip submit: size={size:.9f} < min_size={min_sz}")
            return None
        if price > 0 and size * price < min_not:
            log.warning(f"  Skip submit: notional={size*price:.2f} < min_notional={min_not}")
            return None

        oid    = str((int(time.time() * 1000) + random.randint(0, 999)) % (2**32))
        size_s = norm_size(size)
        px_s   = norm_price(price)
        nonce  = self._next_nonce()
        expiry = time.time_ns() + int(self.cfg.order_expiry_h * 3600 * 1_000_000_000)
        tif    = "IMMEDIATE_OR_CANCEL" if is_market else "GOOD_TILL_TIME"

        log.info(f"  Submit {'BUY' if is_buying else 'SELL'} "
                 f"{'MKT' if is_market else px_s} size={size_s} "
                 f"post={post_only} reduce={reduce_only}")

        if self.cfg.dry_run:
            fake_id = f"dry_{int(time.time()*1000)}"
            return MO(client_oid=fake_id, is_buy=is_buying, price=float(px_s), size=float(size_s))

        try:
            sig = sign_order(
                account=self.account, domain_data=self.domain,
                sub_account_id=self.internal_sub_id,
                is_market=is_market, time_in_force=tif,
                post_only=post_only, reduce_only=reduce_only,
                nonce=nonce, expiration=expiry,
                instrument=self.cfg.instrument,
                size=size_s, limit_price=px_s,
                is_buying_asset=is_buying, instruments=self.instruments,
            )
        except Exception as e:
            log.error(f"Sign failed: {e}"); return None

        try:
            self.client.create_order(
                sub_id=self.internal_sub_id, is_buying=is_buying,
                instr=self.cfg.instrument,
                size=sig["signed_size"], price=px_s,
                post_only=post_only, reduce_only=reduce_only,
                sig=sig, client_oid=oid, is_market=is_market,
            )
        except Exception as e:
            log.error(f"Submit failed: {e}"); return None

        actual_size = float(sig["signed_size"])
        log.info(f"  [OK] {'BUY' if is_buying else 'SELL'} {actual_size} @ {px_s}  coid={oid}")
        return MO(client_oid=oid, is_buy=is_buying, price=float(px_s), size=actual_size)

    # ── Cancel (with confirm retry) ───────────────────────────────────────────
    def _cancel(self, mo: MO, confirm: bool = False) -> bool:
        self.client.cancel_order(self.cfg.sub_account_id, mo.client_oid)
        if not confirm:
            return True
        for _ in range(self.cfg.cancel_retry_count):
            time.sleep(self.cfg.cancel_retry_delay_sec)
            try:
                if mo.client_oid not in self.client.get_open_orders(self.cfg.sub_account_id):
                    return True
            except Exception:
                pass
        log.error(f"  Cancel unconfirmed: {mo.client_oid}")
        return False

    # ── Balance / position ────────────────────────────────────────────────────
    def _get_balances(self):
        try:
            data = self.client.get_sub_account_summary(self.cfg.sub_account_id)
            internal_id = None
            for key in ("sub_account_id", "subAccountId", "sub_id", "id"):
                val = data.get(key) or data.get("sub_account", {}).get(key)
                if val and str(val).isdigit():
                    internal_id = str(val); break
            if internal_id is None:
                for key, val in data.items():
                    if key.lower().endswith("_id") and str(val).isdigit():
                        internal_id = str(val); break
            if internal_id:
                self.internal_sub_id = internal_id
            try:
                # Prefer available_balance / free_collateral — reflects actual margin free to use.
                # total_equity includes unrealized PnL and margin locked by other positions/orders,
                # so sizing from it can cause exchange rejections when margin is already consumed.
                avail = (data.get("available_balance")
                         or data.get("free_collateral")
                         or data.get("available_margin")
                         or data.get("available_equity"))
                total = data.get("total_equity")
                if avail is not None:
                    self.equity = float(avail)
                    log.info(f"  Available={self.equity:.2f} USD (total_equity={float(total or 0):.2f}) | sub_id={self.internal_sub_id}")
                elif total is not None:
                    self.equity = float(total)
                    log.info(f"  Equity={self.equity:.2f} USD (available_balance not in API) | sub_id={self.internal_sub_id}")
                else:
                    self.equity = 0.0
                    log.warning(f"  No equity field found in API response. Keys: {list(data.keys())}")
            except (TypeError, ValueError):
                self.equity = 0.0
        except Exception as e:
            log.warning(f"_get_balances: {e}")

    def _sync_pos(self):
        try:
            instr_norm = self.cfg.instrument.lower()
            for p in self.client.get_positions(self.cfg.sub_account_id):
                if p.get("instrument", p.get("instrument_id", "")).lower() == instr_norm:
                    self.pos.size      = float(p.get("size", 0))
                    self.pos.avg_entry = float(p.get("entry_price", 0))
                    return
            self.pos.size = 0.0; self.pos.avg_entry = 0.0
        except Exception as e:
            log.warning(f"_sync_pos: {e}")

    # ── Quote price calculation ───────────────────────────────────────────────
    def _quote_prices(self, ticker: dict) -> Tuple[float, float]:
        """
        Compute bid and ask quote prices.
        Places bid at best_bid and ask at best_ask, offset by quote_offset_ticks.
        Falls back to mark_price ± 1 tick when order book data is unavailable.
        """
        tick     = self.cfg.tick_size
        offset   = self.cfg.quote_offset_ticks * tick
        best_bid = ticker["best_bid"]
        best_ask = ticker["best_ask"]
        mark     = ticker["mark_price"] or ticker["mid_price"]

        if best_bid and best_ask:
            bid_px = _round(best_bid - offset, tick, up=False)
            ask_px = _round(best_ask + offset, tick, up=True)
        elif mark:
            # No order book — use mark ± 1 tick
            bid_px = _round(mark - tick, tick, up=False)
            ask_px = _round(mark + tick, tick, up=True)
        else:
            raise ValueError("No price data available")

        return bid_px, ask_px

    def _is_stale(self, mo: MO, target_px: float) -> bool:
        """Return True if the order price has drifted more than stale_bps from the target price."""
        if not mo: return False
        threshold = mo.price * self.cfg.stale_bps / 10_000
        return abs(mo.price - target_px) > threshold

    # ── Two-sided quote management ────────────────────────────────────────────
    def _refresh_quotes(self, ticker: dict, open_api: Dict[str, dict]):
        """
        Core quoting logic: maintain a limit buy at best_bid and a limit sell at best_ask.
        Cancel and replace a quote only when the price has moved beyond stale_bps.
        Skip placing a new quote on a side if the max exposure limit is already reached.
        """
        try:
            bid_px, ask_px = self._quote_prices(ticker)
        except ValueError as e:
            log.warning(f"  _refresh_quotes: {e}"); return

        cur_px   = ticker["mark_price"] or ticker["mid_price"]
        notional = abs(self.pos.size) * cur_px
        max_usd  = (self.equity * self.cfg.max_exposure_pct
                    if self.equity > 0 else self.cfg.max_position_notional)

        for side, is_buy, px in [("buy", True, bid_px), ("sell", False, ask_px)]:
            # Do not open new quotes beyond max exposure limit
            at_limit = notional >= max_usd

            mo = self.quotes.get(side)
            if mo:
                # Detect if the order was filled or expired (no longer in open orders)
                if mo.client_oid not in open_api:
                    log.info(f"  Quote {side} gone (filled or expired)")
                    self.quotes[side] = None
                    mo = None
                elif self._is_stale(mo, px):
                    # Price has drifted beyond threshold — cancel and reprice
                    log.info(f"  Stale {side} @ {mo.price:.2f} → {px:.2f} (>{self.cfg.stale_bps}bps)")
                    self._cancel(mo)
                    self.quotes[side] = None
                    mo = None

            if mo is None and not at_limit:
                sz = self._trade_size(px)
                new_mo = self._submit(is_buying=is_buy, price=px, size=sz, post_only=True)
                if new_mo:
                    self.quotes[side] = new_mo
            elif mo is None and at_limit:
                log.info(f"  At exposure limit (${notional:.2f} >= ${max_usd:.2f}) — no new {side} quote")

    # ── Close order management ────────────────────────────────────────────────
    def _refresh_close(self, ticker: dict, open_api: Dict[str, dict]):
        """
        Manage the close order. Strategy:
          1. Place limit close at best_ask (long) or best_bid (short)
          2. Every close_sec seconds, reprice if price has moved
          3. Chase up to close_chase_ticks against us — keeps order competitive
          4. If chased more than close_max_chase_ticks total → market sell immediately
             (position is stuck, cut losses and get back to quoting)
        """
        if self.pos.is_flat:
            if self.close_mo:
                self._cancel(self.close_mo)
                self.close_mo = None
            self._close_ticks_chased = 0
            return

        best_bid  = ticker["best_bid"]
        best_ask  = ticker["best_ask"]
        mark      = ticker["mark_price"] or ticker["mid_price"]
        tick      = self.cfg.tick_size
        now       = time.time()

        if self.pos.is_long:
            target_px = _round(best_ask if best_ask else mark + tick, tick, up=True)
        else:
            target_px = _round(best_bid if best_bid else mark - tick, tick, up=False)

        if self.close_mo:
            if self.close_mo.client_oid not in open_api:
                log.info("  Close order filled/gone ✓")
                self.close_mo = None
                self._close_ticks_chased = 0
                return

            # Check if we've been chasing too long → market sell
            if self._close_ticks_chased >= self.cfg.close_max_chase_ticks:
                log.warning(f"  Close chased {self._close_ticks_chased} ticks — switching to market sell")
                self._cancel(self.close_mo, confirm=True)
                self.close_mo = None
                self._close_ticks_chased = 0
                self._emergency_close(mark or best_bid or best_ask, "max_chase_exceeded")
                return

            # Reprice every close_sec seconds
            age = now - getattr(self, "_last_close_reprice", 0)
            if age < self.cfg.close_sec:
                return

            # Calculate how many ticks price moved against us
            if self.pos.is_long:
                ticks_moved = round((self.close_mo.price - target_px) / tick)  # positive = against us
            else:
                ticks_moved = round((target_px - self.close_mo.price) / tick)

            if ticks_moved > self.cfg.close_chase_ticks:
                # Price moved too far against us in one step — market sell now
                log.warning(f"  Price moved {ticks_moved} ticks against close — market selling")
                self._cancel(self.close_mo, confirm=True)
                self.close_mo = None
                self._close_ticks_chased = 0
                self._emergency_close(mark or best_bid or best_ask, f"price_gap_{ticks_moved}ticks")
                return

            if abs(target_px - self.close_mo.price) >= tick:
                direction = "better" if (
                    (self.pos.is_long and target_px > self.close_mo.price) or
                    (self.pos.is_short and target_px < self.close_mo.price)
                ) else "worse"
                log.info(f"  Close reprice ({direction}): {self.close_mo.price:.2f} → {target_px:.2f} "
                         f"(chased={self._close_ticks_chased}/{self.cfg.close_max_chase_ticks})")
                if direction == "worse":
                    self._close_ticks_chased += abs(round((target_px - self.close_mo.price) / tick))
                self._cancel(self.close_mo, confirm=True)
                self.close_mo = None
                self._last_close_reprice = now

        if self.close_mo is None:
            qty = self._valid_size(abs(self.pos.size), target_px)
            mo  = self._submit(
                is_buying   = self.pos.is_short,
                price       = target_px,
                size        = qty,
                post_only   = True,
                reduce_only = True,
            )
            if mo:
                self.close_mo = mo
                self._last_close_reprice = now
                log.info(f"  Close order placed @ {target_px:.2f} qty={qty:.6f}")


    # ── Emergency market close ─────────────────────────────────────────────────
    def _emergency_close(self, cur_px: float, reason: str):
        log.warning(f"  EMERGENCY CLOSE reason={reason} pos={self.pos.size:+.6f} "
                    f"pnl=${self.pos.pnl(cur_px):.2f}")
        # Cancel all open orders before submitting the market close
        self.client.cancel_all(self.cfg.sub_account_id, self.cfg.instrument)
        self.quotes   = {"buy": None, "sell": None}
        self.close_mo = None
        time.sleep(self.cfg.emergency_close_delay_sec)
        # Market close
        qty = abs(self.pos.size)
        mo  = self._submit(is_buying=self.pos.is_short, price=cur_px,
                           size=qty, post_only=False, reduce_only=True, is_market=True)
        if mo:
            log.info(f"  Emergency close sent coid={mo.client_oid}")
        # Verify
        time.sleep(self.cfg.emergency_verify_sleep_sec)
        self._sync_pos()
        if not self.pos.is_flat:
            log.error(f"  Emergency close UNCONFIRMED — pos still {self.pos.size:+.6f}")

    # ── Main loop ─────────────────────────────────────────────────────────────
    def run(self):
        self._running = True

        def _stop(sig, _):
            log.info("Shutting down — cancelling all orders")
            self.client.cancel_all(self.cfg.sub_account_id, self.cfg.instrument)
            sys.exit(0)
        signal.signal(signal.SIGINT,  _stop)
        signal.signal(signal.SIGTERM, _stop)

        log.info("=" * 60)
        log.info("GRVT Market Maker — Two-sided limit quoting")
        log.info(f"  Instrument  : {self.cfg.instrument}")
        log.info(f"  Trade size  : {self.cfg.trade_size_pct*100:.0f}% equity per side")
        log.info(f"  Max exposure: {self.cfg.max_exposure_pct*100:.0f}% equity")
        log.info(f"  Stale bps   : {self.cfg.stale_bps} bps")
        log.info(f"  Hard loss   : {self.cfg.hard_loss_pct*100:.1f}% equity")
        log.info(f"  Mode        : {'DRY RUN' if self.cfg.dry_run else 'LIVE'}")
        log.info("=" * 60)

        try:
            self.client.cancel_all(self.cfg.sub_account_id, self.cfg.instrument)
            time.sleep(self.cfg.startup_cancel_sleep_sec)
        except Exception: pass

        cycle = 0
        while self._running:
            cycle += 1
            try:
                # Re-check session health every 5 minutes; re-login if expired
                now = time.time()
                if now - self._last_session_check > self.cfg.session_check_sec:
                    self.client._ensure_session()
                    self._last_session_check = now

                self._get_balances()
                self._sync_pos()

                ticker   = self.client.get_ticker(self.cfg.instrument)
                cur_px   = ticker["mark_price"] or ticker["mid_price"]
                open_api = self.client.get_open_orders(self.cfg.sub_account_id)

                if not cur_px:
                    log.warning("  No price — waiting"); time.sleep(self.cfg.no_price_wait_sec); continue

                log.info(
                    f"--- Cycle {cycle} | "
                    f"pos={self.pos.size:+.6f} @ {self.pos.avg_entry:.2f} | "
                    f"px={cur_px:.2f} | pnl=${self.pos.pnl(cur_px):.2f} | "
                    f"equity=${self.equity:.2f} | "
                    f"quotes=buy={'Y' if self.quotes['buy'] else 'N'} "
                    f"sell={'Y' if self.quotes['sell'] else 'N'} "
                    f"close={'Y' if self.close_mo else 'N'}"
                )

                # Hard risk limit: emergency market close if unrealized loss exceeds threshold
                if not self.pos.is_flat and self.equity > 0:
                    loss_pct = -self.pos.pnl(cur_px) / self.equity
                    if loss_pct > self.cfg.hard_loss_pct:
                        self._emergency_close(cur_px, f"hard_loss {loss_pct*100:.2f}%")
                        time.sleep(self.cfg.after_emergency_sleep_sec); continue

                # Maintain two-sided quotes (bid + ask) every cycle.
                # _refresh_quotes will skip a side automatically if at exposure limit.
                self._refresh_quotes(ticker, open_api)

                # When a position is open (one quote got filled), manage the close order
                # on the opposite side to return to flat.
                if not self.pos.is_flat:
                    self._refresh_close(ticker, open_api)
                    # Cancel any remaining quote in the same direction as the open position
                    # to avoid adding more exposure in the filled direction.
                    filled_side = "buy" if self.pos.is_long else "sell"
                    if self.quotes[filled_side]:
                        log.info(f"  Cancelling {filled_side} quote (already filled that side)")
                        self._cancel(self.quotes[filled_side])
                        self.quotes[filled_side] = None
                else:
                    # Position is flat — clean up any stale close order
                    if self.close_mo:
                        self._cancel(self.close_mo)
                        self.close_mo = None
                    # Log completed round-trip
                    if self._entry_px:
                        log.info("  Position returned to flat")
                        self._entry_px = 0.0
                        self._entry_ts = 0.0

                # Record entry price the first time a position appears
                if not self.pos.is_flat and not self._entry_px:
                    self._entry_px = self.pos.avg_entry
                    self._entry_ts = time.time()

            except Exception as e:
                log.exception(f"Cycle error: {e}")

            time.sleep(self.cfg.cycle_sleep_sec)

# ── Utility ────────────────────────────────────────────────────────────────────
def _round(price: float, tick: float, up: bool = False) -> float:
    if tick <= 0: return price
    return (math.ceil if up else math.floor)(price / tick) * tick

# ── CLI & Config ───────────────────────────────────────────────────────────────
def load_config(args) -> Config:
    cfg = Config()
    config_path = getattr(args, "config", None)
    if not config_path:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        for c in [os.path.join(script_dir, "grvt_config.json"),
                  os.path.join(os.getcwd(), "grvt_config.json")]:
            if os.path.isfile(c): config_path = c; break
    if config_path:
        deprecated = {
            "close_init_bps", "close_widen_bps", "close_max_bps",
            "stop_loss_bps", "builder_address", "builder_fee", "as_T", "as_gamma", "as_k",
            "order_expiry_days", "min_spread_bps_passive", "min_spread_bps_agg",
            "refresh_sec_passive", "refresh_sec_aggressive", "levels_tob_passive",
            "levels_tob_aggressive", "mode", "soft_loss_pct", "cb_timeout_sec",
            "cooldown_sec", "close_refresh_sec",
        }
        try:
            with open(config_path, encoding="utf-8") as f:
                raw = json.load(f)
            ignored = [] 
            for k, v in raw.items():
                if hasattr(cfg, k):
                    setattr(cfg, k, v)
                elif k not in deprecated:
                    ignored.append(k)
            if ignored:
                log.warning(f"Unknown config keys: {ignored}")
            log.info(f"Config loaded: {os.path.abspath(config_path)}")
        except Exception as e:
            log.error(f"Config error: {e}"); sys.exit(1)
    else:
        log.warning("grvt_config.json not found — using CLI/env vars")

    # for k, v in {
    #     "env": getattr(args, "env", None),
    #     "api_key": getattr(args, "api_key", None),
    #     "signer_private_key": getattr(args, "signer_key", None),
    #     "sub_account_id": getattr(args, "sub_account_id", None),
    #     "instrument": getattr(args, "instrument", None),
    #     "order_size": getattr(args, "size", None),
    #     "dry_run": getattr(args, "dry_run", None),
    # }.items():
    #     if v is not None: setattr(cfg, k, v)
    cfg.api_key            = cfg.api_key            or os.getenv("GRVT_API_KEY",    "")
    cfg.signer_private_key = cfg.signer_private_key or os.getenv("GRVT_SIGNER_KEY", "")
    cfg.sub_account_id     = cfg.sub_account_id     or os.getenv("GRVT_SUB_ID",     "")
    return cfg

def validate(cfg: Config):
    errs = []
    if not cfg.api_key:            errs.append("api_key missing")
    if not cfg.signer_private_key: errs.append("signer_private_key missing")
    if not cfg.sub_account_id:     errs.append("sub_account_id missing")
    if cfg.env not in CHAIN_IDS:   errs.append(f"env must be {list(CHAIN_IDS)}")
    if errs:
        for e in errs: log.error(f"Config: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="GRVT Market-Making Bot")
    parser.add_argument("--config",         type=str) 
    args = parser.parse_args()
    cfg = load_config(args)
    validate(cfg)
    log.info(f"env={cfg.env} | sub={cfg.sub_account_id} | "
             f"instr={cfg.instrument} | dry={cfg.dry_run}")
    MarketMaker(cfg).run()

if __name__ == "__main__":
    main()