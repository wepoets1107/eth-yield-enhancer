"""
Deribit API 客户端
封装 JSON-RPC 接口：认证、余额、指数价格、K线数据、现货买卖

参考：
- icefire-options-workbench (REST GET 公共数据)
- f405-options-bot (JSON-RPC 下单)
- DDH workbench (WebSocket 实时交易)
- binghuodao-options-notes (公共行情)
"""

from __future__ import annotations

import time
import json
import logging
import threading
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

# 安全浮点数转换（兼容 None / 字符串 / 各种边界）
def _f(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        v = float(value)
        if v != v:  # NaN
            return default
        return v
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# 错误分级（参考 ccxt deribit.exceptions 映射，归纳为业务类别）
# 目的：让调用方能按错误类型决定 重试 / 告警 / 暂停该侧 / 视为已处理，
# 而非把限频抖动与致命鉴权失败混在同一份日志里。
# ---------------------------------------------------------------------------
# Deribit JSON-RPC 错误码 -> 业务类别
DERIBIT_ERROR_CATEGORY: dict = {
    # 认证/授权失败（凭据/2FA/banned 等）：不可重试，需人工排查
    10000: "auth", 12000: "auth", 12003: "auth", 12004: "auth", 12005: "auth",
    12998: "auth", 12999: "auth", 13000: "auth", 13001: "auth", 13003: "auth",
    13004: "auth", 13005: "auth", 13006: "auth", 13007: "auth", 13009: "auth",
    # 限流 / 撮合队列满（DDoSProtection）：可重试（指数退避）
    10028: "rate_limit", 10047: "rate_limit", 11035: "rate_limit",
    11093: "rate_limit", 12001: "rate_limit",
    # 资金不足（InsufficientFunds）：不可重试，挂起对应方向
    10009: "insufficient_funds",
    # 订单不存在（OrderNotFound）：视为已处理
    10004: "order_not_found", 10010: "order_not_found", 10029: "order_not_found",
    # 订单参数非法（含 tick/精度错误 InvalidOrder）：不可重试，属代码缺陷
    10002: "invalid_order", 10003: "invalid_order", 10005: "invalid_order",
    10006: "invalid_order", 10007: "invalid_order", 10008: "invalid_order",
    10011: "invalid_order", 10012: "invalid_order", 10021: "invalid_order",
    10022: "invalid_order", 10023: "invalid_order", 10024: "invalid_order",
    10025: "invalid_order", 10026: "invalid_order", 10027: "invalid_order",
    10032: "invalid_order", 10034: "invalid_order", 10035: "invalid_order",
    10036: "invalid_order", 10043: "invalid_order", 10044: "invalid_order",
    10045: "invalid_order", 10046: "invalid_order", 11008: "invalid_order",
    11036: "invalid_order", 11038: "invalid_order", 11039: "invalid_order",
    11041: "invalid_order", 11044: "invalid_order", 11054: "invalid_order",
    # 参数错误（含 tick 精度 / JSON-RPC 参数 BadRequest）：不可重试
    11029: "bad_request", 11037: "bad_request", 11043: "bad_request",
    11045: "bad_request", 11046: "bad_request", 11047: "bad_request",
    11049: "bad_request", 11050: "bad_request", 13010: "bad_request",
    13011: "bad_request",
    13013: "bad_request", 13014: "bad_request", 13015: "bad_request",
    13016: "bad_request", -32602: "bad_request", -32601: "bad_request",
    -32700: "bad_request", -32000: "bad_request",
    # 权限拒绝（PermissionDenied）：不可重试
    10013: "permission_denied", 10014: "permission_denied", 10015: "permission_denied",
    10016: "permission_denied", 10017: "permission_denied", 10018: "permission_denied",
    10019: "permission_denied", 11042: "permission_denied", 13002: "permission_denied",
    13012: "permission_denied", 13021: "permission_denied",
    # 交易所不可用（ExchangeNotAvailable）：可重试
    10040: "exchange_not_available",
    # 维护中（OnMaintenance）：等待，不重试
    10041: "on_maintenance", 11051: "on_maintenance",
    # 通用交易所错误（ExchangeError）：视为瞬时，可重试
    10001: "exchange_error", 10020: "exchange_error", 10030: "exchange_error",
    10031: "exchange_error", 10048: "exchange_error", 11030: "exchange_error",
    11031: "exchange_error", 11048: "exchange_error", 11052: "exchange_error",
    11053: "exchange_error", 11094: "exchange_error", 11095: "exchange_error",
    11096: "exchange_error", 12002: "exchange_error", 12100: "exchange_error",
    13008: "exchange_error", 13017: "exchange_error", 13018: "exchange_error",
    13019: "exchange_error", 13020: "exchange_error", 13025: "exchange_error",
    # 其他明确类别（不重试）
    10033: "not_supported",
    11090: "invalid_address", 11091: "invalid_address", 11092: "invalid_address",
}

# 可重试的类别（参考 ccxt：限流/不可用属瞬时，退避后大概率恢复）
RETRYABLE_CATEGORIES = {"rate_limit", "exchange_not_available", "exchange_error"}


def _backoff(attempt: int) -> float:
    """指数退避：1, 2, 4, 8 秒封顶"""
    return min(8.0, 2.0 ** attempt)


def _classify_error(err: Any) -> tuple:
    """把 Deribit 错误归一为 (code, category, is_retryable, message)"""
    code = None
    msg = ""
    if isinstance(err, dict):
        code = err.get("code")
        msg = err.get("message", "")
    else:
        msg = str(err)
    category = DERIBIT_ERROR_CATEGORY.get(code, "unknown")
    is_retryable = category in RETRYABLE_CATEGORIES
    return code, category, is_retryable, msg


class DeribitClient:
    """Deribit JSON-RPC API 客户端（同步、线程安全）"""

    MAINNET = "https://www.deribit.com/api/v2/"
    TESTNET = "https://test.deribit.com/api/v2/"

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        testnet: bool = False,
    ) -> None:
        if not client_id or not client_secret:
            raise ValueError("client_id and client_secret are required")

        self.client_id = client_id
        self.client_secret = client_secret
        self.testnet = testnet
        self.base_url = self.TESTNET if testnet else self.MAINNET

        self._token: Optional[str] = None
        self._token_expiry: float = 0.0
        self._lock = threading.RLock()
        self._request_id = 0
        self._last_auth_error: Optional[str] = None

    # ------------------------------------------------------------------
    # 底层 JSON-RPC 请求
    # ------------------------------------------------------------------

    def _call(
        self,
        method: str,
        params: Optional[dict] = None,
        need_auth: bool = False,
        max_retries: int = 4,
    ) -> dict:
        """发送 JSON-RPC POST 请求，统一返回 {success, result/error, error_code, error_category, }。

        参考 ccxt 的设计：
        - 把 Deribit 错误码归类为业务类别，调用方可据此分流；
        - 瞬时错误（限流 / 交易所不可用 / 通用交易所错误）自动指数退避重试，
          最多 max_retries 次；认证 / 资金不足 / 订单参数等明确错误不重试。
        """
        if params is None:
            params = {}

        self._request_id += 1
        req_id = self._request_id

        payload = {
            "jsonrpc": "2.0",
            "id": req_id,
            "method": method,
            "params": params,
        }

        # 鉴权一次性获取：重试期间不再反复刷新 token，避免抖动被放大
        headers = {}
        if need_auth or method.startswith("private"):
            token = self._get_token()
            if token:
                headers["Authorization"] = f"Bearer {token}"
            else:
                logger.error("Deribit: no auth token available for [%s]", method)
                return {
                    "success": False,
                    "error": "auth_token_unavailable",
                    "error_code": None,
                    "error_category": "auth",
                    "is_retryable": False,
                }

        last_result: dict = {"success": False, "error": "unknown"}
        for attempt in range(max_retries):
            try:
                resp = requests.post(
                    self.base_url,
                    json=payload,
                    headers=headers,
                    timeout=20,
                )
                if resp.status_code != 200:
                    # 网关 / 5xx：视为瞬时，退避后重试
                    logger.warning(
                        "Deribit HTTP %d [%s] attempt %d: %s",
                        resp.status_code, method, attempt + 1, resp.text[:200],
                    )
                    last_result = {"success": False, "error": f"HTTP {resp.status_code}"}
                    if attempt < max_retries - 1:
                        time.sleep(_backoff(attempt))
                    continue

                data = resp.json()

                # JSON-RPC error（Deribit 正常响应但业务错误）
                if "error" in data and data["error"] is not None:
                    err = data["error"]
                    code, category, is_retryable, msg = _classify_error(err)
                    logger.error(
                        "Deribit API error [%s] code=%s cat=%s: %s",
                        method, code, category, msg,
                    )
                    last_result = {
                        "success": False,
                        "error": err,
                        "error_code": code,
                        "error_category": category,
                        "is_retryable": is_retryable,
                    }
                    if is_retryable and attempt < max_retries - 1:
                        logger.warning(
                            "Retryable error (%s), backoff %.1fs",
                            category, _backoff(attempt),
                        )
                        time.sleep(_backoff(attempt))
                        continue
                    return last_result

                return {"success": True, "result": data.get("result")}

            except (requests.exceptions.RequestException, ValueError) as e:
                # 网络异常 / 响应非 JSON（如 502 网关）：视为瞬时，退避重试
                logger.warning(
                    "Deribit request failed [%s] attempt %d: %s",
                    method, attempt + 1, e,
                )
                last_result = {"success": False, "error": str(e)}
                if attempt < max_retries - 1:
                    time.sleep(_backoff(attempt))
                continue

        return last_result

    # ------------------------------------------------------------------
    # 认证（带自动续期 + 线程安全）
    # ------------------------------------------------------------------

    def _authenticate(self) -> bool:
        """获取新的 access_token（client_credentials 模式）"""
        result = self._call("public/auth", {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        })
        if result["success"]:
            r = result["result"]
            self._token = r["access_token"]
            expires_in = _f(r.get("expires_in"), 3600)
            # 提前 120 秒续期，避免临界区过期
            self._token_expiry = time.time() + expires_in - 120
            self._last_auth_error = None
            logger.info(
                "Deribit authenticated (%s), token expires in %d s",
                "testnet" if self.testnet else "mainnet",
                expires_in,
            )
            return True

        err = result.get("error", "unknown_error")
        if isinstance(err, dict):
            err_msg = err.get("message", str(err))
        else:
            err_msg = str(err)
        self._last_auth_error = err_msg
        logger.error(
            "Deribit auth failed (%s): %s",
            "testnet" if self.testnet else "mainnet",
            err_msg,
        )
        return False

    def _get_token(self) -> Optional[str]:
        """获取有效 token，过期自动续期（double-checked locking）"""
        if self._token is None or time.time() >= self._token_expiry:
            with self._lock:
                if self._token is None or time.time() >= self._token_expiry:
                    if not self._authenticate():
                        return None
        return self._token

    def check_connection(self) -> dict:
        """检查 API 连接，返回详细状态字典"""
        token = self._get_token()
        result: dict = {
            "connected": False,
            "testnet": self.testnet,
            "auth_error": self._last_auth_error,
        }
        if not token:
            return result
        r = self._call("public/get_time")
        if r["success"]:
            result["connected"] = True
        else:
            result["error"] = str(r.get("error", "unknown"))
        return result

    # ------------------------------------------------------------------
    # 公共接口（无需认证）
    # ------------------------------------------------------------------

    def get_index_price(self, index_name: str = "btc_usdc") -> Optional[float]:
        """获取指数价格"""
        result = self._call("public/get_index_price", {
            "index_name": index_name,
        })
        if result["success"]:
            return _f(result["result"].get("index_price"))
        return None

    def get_instruments(
        self, currency: str = "BTC", kind: str = "spot"
    ) -> list[dict]:
        """获取交易品种列表"""
        result = self._call("public/get_instruments", {
            "currency": currency,
            "kind": kind,
            "expired": False,
        })
        if result["success"]:
            return result["result"]
        return []

    def get_ticker(self, instrument_name: str) -> Optional[dict]:
        """获取单个品种的完整 ticker（含 Greeks）"""
        result = self._call("public/ticker", {
            "instrument_name": instrument_name,
        })
        if result["success"]:
            return result["result"]
        return None

    def get_tradingview_chart_data(
        self,
        instrument_name: str,
        start_timestamp: int,
        end_timestamp: int,
        resolution: str = "1D",
    ) -> Optional[dict]:
        """获取 K 线数据（波动率计算用）"""
        result = self._call("public/get_tradingview_chart_data", {
            "instrument_name": instrument_name,
            "start_timestamp": int(start_timestamp),
            "end_timestamp": int(end_timestamp),
            "resolution": resolution,
        })
        if result["success"]:
            return result["result"]
        return None

    def get_book_summary_by_currency(
        self, currency: str = "BTC", kind: str = "option"
    ) -> list[dict]:
        """获取期权/期货盘口摘要"""
        result = self._call("public/get_book_summary_by_currency", {
            "currency": currency,
            "kind": kind,
        })
        if result["success"]:
            return result["result"]
        return []

    # ------------------------------------------------------------------
    # 私有接口（账户 & 交易）
    # ------------------------------------------------------------------

    def get_account_summary(
        self, currency: str = "USDC", extended: bool = True
    ) -> Optional[dict]:
        """获取账户摘要（余额、保证金等）"""
        result = self._call("private/get_account_summary", {
            "currency": currency,
            "extended": extended,
        }, need_auth=True)
        if result["success"]:
            return result["result"]
        return None

    def get_positions(
        self, currency: str = "BTC", kind: str = "any"
    ) -> list[dict]:
        """获取持仓"""
        result = self._call("private/get_positions", {
            "currency": currency,
            "kind": kind,
        }, need_auth=True)
        if result["success"]:
            return result["result"]
        return []

    def buy(
        self,
        instrument_name: str,
        amount: float,
        order_type: str = "market",
        label: Optional[str] = None,
        price: Optional[float] = None,
        reduce_only: bool = False,
        post_only: bool = False,
    ) -> dict:
        """买入

        Args:
            instrument_name: 交易对名称 (e.g. BTC_USDC)
            amount: 数量（BTC 或合约张数）
            order_type: market / limit / stop_limit / stop_market
            label: 订单标签（便于识别）
            price: 限价单价格
            reduce_only: 仅减仓
            post_only: 仅挂单（不吃单）
        """
        params = {
            "instrument_name": instrument_name,
            "amount": amount,
            "type": order_type,
        }
        if label:
            params["label"] = label
        if price and order_type in ("limit", "stop_limit"):
            params["price"] = price
        if reduce_only:
            params["reduce_only"] = True
        if post_only:
            params["post_only"] = True
        return self._call("private/buy", params, need_auth=True)

    def sell(
        self,
        instrument_name: str,
        amount: float,
        order_type: str = "market",
        label: Optional[str] = None,
        price: Optional[float] = None,
        reduce_only: bool = False,
        post_only: bool = False,
    ) -> dict:
        """卖出"""
        params = {
            "instrument_name": instrument_name,
            "amount": amount,
            "type": order_type,
        }
        if label:
            params["label"] = label
        if price and order_type in ("limit", "stop_limit"):
            params["price"] = price
        if reduce_only:
            params["reduce_only"] = True
        if post_only:
            params["post_only"] = True
        return self._call("private/sell", params, need_auth=True)

    def get_order_state(self, order_id: str) -> dict:
        """查询订单状态"""
        return self._call("private/get_order_state", {
            "order_id": order_id,
        }, need_auth=True)

    def cancel_order(self, order_id: str) -> dict:
        """取消单笔订单"""
        return self._call("private/cancel", {
            "order_id": order_id,
        }, need_auth=True)

    def cancel_all(self, instrument_name: str = "") -> dict:
        """取消指定币种的所有订单（不传参则取消全部）"""
        params = {}
        if instrument_name:
            params["instrument_name"] = instrument_name
        return self._call("private/cancel_all", params, need_auth=True)

    def cancel_all_by_instrument(self, instrument_name: str) -> dict:
        """只取消指定 instrument 的订单（不碰其他币种/期权/期货）"""
        return self._call("private/cancel_all_by_instrument", {
            "instrument_name": instrument_name,
        }, need_auth=True)

    def get_open_orders(
        self, instrument_name: Optional[str] = None
    ) -> list[dict]:
        """获取未成交订单"""
        params = {}
        if instrument_name:
            params["instrument_name"] = instrument_name
        result = self._call("private/get_open_orders", params, need_auth=True)
        if result["success"]:
            return result["result"]
        return []

    def get_order_book(
        self, instrument_name: str, depth: int = 5
    ) -> Optional[dict]:
        """获取订单薄"""
        result = self._call("public/get_order_book", {
            "instrument_name": instrument_name,
            "depth": depth,
        })
        if result["success"]:
            return result["result"]
        return None

    # ------------------------------------------------------------------
    # 辅助：解析订单响应中的成交明细
    # ------------------------------------------------------------------

    @staticmethod
    def parse_order_result(result_data: dict) -> dict:
        """从 buy/sell 响应中提取可读的订单摘要"""
        order = result_data.get("order", result_data)
        trades = result_data.get("trades", [])

        filled_amount = _f(order.get("filled_amount"))
        avg_price = _f(order.get("average_price"))
        order_id = order.get("order_id", "")
        state = order.get("order_state", "")

        # 成交详情
        fills = []
        for t in trades:
            fills.append({
                "trade_id": t.get("trade_id"),
                "amount": _f(t.get("amount")),
                "price": _f(t.get("price")),
                "fee": _f(t.get("fee")),
                "fee_currency": t.get("fee_currency"),
                "timestamp": t.get("timestamp"),
            })

        return {
            "order_id": order_id,
            "state": state,
            "filled_amount": filled_amount,
            "average_price": avg_price,
            "total_cost": round(filled_amount * avg_price, 2),
            "label": order.get("label", ""),
            "direction": order.get("direction", ""),
            "instrument_name": order.get("instrument_name", ""),
            "fills": fills,
            "raw_order": order,
        }
