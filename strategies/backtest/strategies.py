"""5つのデイトレ戦略を実装する。"""

import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Protocol


@dataclass
class Signal:
    timestamp: pd.Timestamp
    action: str  # "BUY" or "SELL"
    price: float
    reason: str


class Strategy(Protocol):
    name: str

    def generate_signals(self, df_day: pd.DataFrame) -> list[Signal]:
        ...


# ── 指標計算ヘルパー ──

def _ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _vwap(df: pd.DataFrame) -> pd.Series:
    typical = (df["high"] + df["low"] + df["close"]) / 3
    cum_tp_vol = (typical * df["volume"]).cumsum()
    cum_vol = df["volume"].cumsum()
    return cum_tp_vol / cum_vol.replace(0, np.nan)


def _atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift(1)).abs(),
        (df["low"] - df["close"].shift(1)).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(window=min(period, len(tr)), min_periods=1).mean()


def _bollinger(series: pd.Series, period: int = 20, std_dev: float = 2.0):
    mid = series.rolling(period, min_periods=1).mean()
    std = series.rolling(period, min_periods=1).std()
    return mid, mid + std_dev * std, mid - std_dev * std


# ── 戦略1: VWAP Mean Reversion ──

class VWAPMeanReversion:
    name = "VWAP Mean Reversion"

    def generate_signals(self, df_day: pd.DataFrame) -> list[Signal]:
        signals = []
        vwap = _vwap(df_day)
        atr = _atr(df_day)
        position = False

        for i in range(20, len(df_day)):
            ts = df_day.index[i]
            price = df_day["close"].iloc[i]
            v = vwap.iloc[i]
            a = atr.iloc[i]

            if pd.isna(v) or pd.isna(a) or a == 0:
                continue

            if not position:
                prev_price = df_day["close"].iloc[i - 1]
                if prev_price < v - a and price > v - a:
                    signals.append(Signal(ts, "BUY", price, "VWAP回帰ロング"))
                    position = True
            else:
                if price >= v + 0.5 * a or price <= v - 1.5 * a:
                    signals.append(Signal(ts, "SELL", price, "VWAP利確/損切"))
                    position = False

        return signals


# ── 戦略2: Opening Range Breakout ──

class OpeningRangeBreakout:
    name = "Opening Range Breakout"

    def generate_signals(self, df_day: pd.DataFrame) -> list[Signal]:
        signals = []
        # 最初30分 = 6本 (5分足)
        if len(df_day) < 10:
            return signals

        opening = df_day.iloc[:6]
        range_high = opening["high"].max()
        range_low = opening["low"].min()
        atr = _atr(df_day)
        position = False

        for i in range(6, len(df_day)):
            ts = df_day.index[i]
            price = df_day["close"].iloc[i]
            a = atr.iloc[i]

            if pd.isna(a) or a == 0:
                continue

            if not position:
                if price > range_high and df_day["volume"].iloc[i] > df_day["volume"].iloc[:i].mean():
                    signals.append(Signal(ts, "BUY", price, "ORBブレイクアウト"))
                    position = True
            else:
                entry_price = signals[-1].price if signals else price
                if price >= entry_price + 1.5 * a or price <= entry_price - a:
                    signals.append(Signal(ts, "SELL", price, "ORB利確/損切"))
                    position = False

        return signals


# ── 戦略3: EMA Crossover ──

class EMACrossover:
    name = "EMA Crossover (9/21)"

    def generate_signals(self, df_day: pd.DataFrame) -> list[Signal]:
        signals = []
        ema9 = _ema(df_day["close"], 9)
        ema21 = _ema(df_day["close"], 21)
        atr = _atr(df_day)
        position = False

        for i in range(22, len(df_day)):
            ts = df_day.index[i]
            price = df_day["close"].iloc[i]
            a = atr.iloc[i]

            if pd.isna(a) or a == 0:
                continue

            if not position:
                if ema9.iloc[i - 1] <= ema21.iloc[i - 1] and ema9.iloc[i] > ema21.iloc[i]:
                    signals.append(Signal(ts, "BUY", price, "EMAゴールデンクロス"))
                    position = True
            else:
                entry_price = signals[-1].price if signals else price
                if price >= entry_price + 1.5 * a or price <= entry_price - a:
                    signals.append(Signal(ts, "SELL", price, "EMA利確/損切"))
                    position = False

        return signals


# ── 戦略4: Bollinger Band Reversion ──

class BollingerBandReversion:
    name = "Bollinger Band Reversion"

    def generate_signals(self, df_day: pd.DataFrame) -> list[Signal]:
        signals = []
        mid, upper, lower = _bollinger(df_day["close"])
        atr = _atr(df_day)
        position = False

        for i in range(20, len(df_day)):
            ts = df_day.index[i]
            price = df_day["close"].iloc[i]
            a = atr.iloc[i]

            if pd.isna(lower.iloc[i]) or pd.isna(a) or a == 0:
                continue

            if not position:
                if price <= lower.iloc[i]:
                    signals.append(Signal(ts, "BUY", price, "BB下限タッチ"))
                    position = True
            else:
                if price >= mid.iloc[i] or price <= lower.iloc[i] - a:
                    signals.append(Signal(ts, "SELL", price, "BB中央帯/損切"))
                    position = False

        return signals


# ── 戦略5: RSI Mean Reversion ──

class RSIMeanReversion:
    name = "RSI Mean Reversion"

    def generate_signals(self, df_day: pd.DataFrame) -> list[Signal]:
        signals = []
        rsi = _rsi(df_day["close"], 14)
        atr = _atr(df_day)
        position = False

        for i in range(20, len(df_day)):
            ts = df_day.index[i]
            price = df_day["close"].iloc[i]
            r = rsi.iloc[i]
            a = atr.iloc[i]

            if pd.isna(r) or pd.isna(a) or a == 0:
                continue

            if not position:
                if r < 30:
                    signals.append(Signal(ts, "BUY", price, "RSI<30 oversold"))
                    position = True
            else:
                entry_price = signals[-1].price if signals else price
                if r > 70 or price >= entry_price + 1.5 * a or price <= entry_price - a:
                    signals.append(Signal(ts, "SELL", price, "RSI>70/利確/損切"))
                    position = False

        return signals


ALL_STRATEGIES = [
    VWAPMeanReversion(),
    OpeningRangeBreakout(),
    EMACrossover(),
    BollingerBandReversion(),
    RSIMeanReversion(),
]


# ── Swing / Position 戦略 (日足用) ──


@dataclass
class SwingSignal:
    date: pd.Timestamp
    action: str  # "BUY", "SELL", "REBALANCE"
    symbol: str
    price: float
    weight: float  # ポートフォリオ内の目標ウェイト (0.0〜1.0)
    reason: str


class SwingStrategy(Protocol):
    name: str

    def generate_signals(self, daily_data: dict[str, pd.DataFrame]) -> list[SwingSignal]:
        ...


# ── 戦略6: Dual Momentum (セクターETFローテーション) ──

class DualMomentum:
    """
    絶対モメンタム + 相対モメンタムでセクターETFを月次リバランス。
    - 相対: 過去 lookback 日リターン上位 top_n を選定
    - 絶対: リターンが負ならBND(債券)に退避
    """

    def __init__(self, lookback: int = 40, top_n: int = 3, bond: str = "BND"):
        self.lookback = lookback
        self.top_n = top_n
        self.bond = bond
        self.name = f"DualMomentum(lb={lookback},top={top_n})"

    def generate_signals(self, daily_data: dict[str, pd.DataFrame]) -> list[SwingSignal]:
        signals: list[SwingSignal] = []
        # 全銘柄の終値を結合
        closes = {}
        for sym, df in daily_data.items():
            closes[sym] = df["close"]
        price_df = pd.DataFrame(closes).dropna()

        if len(price_df) < self.lookback + 5:
            return signals

        # 月初の営業日にリバランス
        price_df["month"] = price_df.index.to_period("M")
        rebalance_dates = price_df.groupby("month").apply(lambda g: g.index[0]).values

        etf_symbols = [s for s in price_df.columns if s != self.bond and s != "month"]

        for date in rebalance_dates:
            idx = price_df.index.get_loc(date)
            if idx < self.lookback:
                continue

            # 各ETFの lookback 日リターン
            returns = {}
            for sym in etf_symbols:
                past = price_df[sym].iloc[idx - self.lookback]
                curr = price_df[sym].iloc[idx]
                if past > 0:
                    returns[sym] = (curr - past) / past
                else:
                    returns[sym] = 0.0

            # 相対モメンタム: 上位 top_n
            ranked = sorted(returns.items(), key=lambda x: x[1], reverse=True)
            selected = []
            for sym, ret in ranked[:self.top_n]:
                if ret > 0:  # 絶対モメンタム: 正リターンのみ
                    selected.append(sym)

            # シグナル生成
            if selected:
                w = 1.0 / len(selected)
                for sym in selected:
                    price = price_df[sym].iloc[idx]
                    signals.append(SwingSignal(
                        date=price_df.index[idx], action="REBALANCE", symbol=sym,
                        price=price, weight=w,
                        reason=f"相対MOM top{self.top_n}, ret={returns[sym]:+.1%}",
                    ))
            else:
                # 全部負 → 債券退避
                if self.bond in price_df.columns:
                    price = price_df[self.bond].iloc[idx]
                    signals.append(SwingSignal(
                        date=price_df.index[idx], action="REBALANCE", symbol=self.bond,
                        price=price, weight=1.0, reason="絶対MOM: 全セクター負→債券退避",
                    ))

        return signals


# ── 戦略7: Breakout Swing (20日高値ブレイクアウト) ──

class BreakoutSwing:
    """
    20日高値ブレイク + 出来高 + RSI フィルターのスイング戦略。
    - エントリー: 終値 > 20日高値 & 出来高 > 平均×vol_mult & RSI(14) 50-70
    - エグジット: ATR(14) ベースのトレーリングストップ (trail_atr × ATR)
    - 最大同時保有: max_positions 銘柄
    """

    def __init__(self, breakout_period: int = 20, vol_mult: float = 1.5,
                 rsi_low: float = 50, rsi_high: float = 70,
                 trail_atr: float = 2.0, max_positions: int = 5):
        self.breakout_period = breakout_period
        self.vol_mult = vol_mult
        self.rsi_low = rsi_low
        self.rsi_high = rsi_high
        self.trail_atr = trail_atr
        self.max_positions = max_positions
        self.name = f"BreakoutSwing(bp={breakout_period},vol={vol_mult},trail={trail_atr})"

    def generate_signals(self, daily_data: dict[str, pd.DataFrame]) -> list[SwingSignal]:
        signals: list[SwingSignal] = []
        positions: dict[str, float] = {}  # sym -> trailing stop price
        highest: dict[str, float] = {}  # sym -> highest since entry

        # 全銘柄の日付を統一
        all_dates = set()
        for df in daily_data.values():
            all_dates.update(df.index)
        all_dates = sorted(all_dates)

        for date in all_dates:
            # エグジット判定（先に処理）
            for sym in list(positions.keys()):
                df = daily_data[sym]
                if date not in df.index:
                    continue
                idx = df.index.get_loc(date)
                price = df["close"].iloc[idx]
                atr_val = self._atr(df, idx)

                if price > highest[sym]:
                    highest[sym] = price
                    positions[sym] = highest[sym] - self.trail_atr * atr_val

                if price <= positions[sym]:
                    signals.append(SwingSignal(
                        date=date, action="SELL", symbol=sym, price=price,
                        weight=0.0, reason=f"トレーリングストップ (stop={positions[sym]:.2f})",
                    ))
                    del positions[sym]
                    del highest[sym]

            # エントリー判定
            if len(positions) >= self.max_positions:
                continue

            for sym, df in daily_data.items():
                if sym in positions:
                    continue
                if date not in df.index:
                    continue
                idx = df.index.get_loc(date)
                if idx < self.breakout_period + 1:
                    continue

                price = df["close"].iloc[idx]
                high_n = df["high"].iloc[idx - self.breakout_period:idx].max()
                avg_vol = df["volume"].iloc[idx - self.breakout_period:idx].mean()
                cur_vol = df["volume"].iloc[idx]
                rsi_val = self._rsi(df["close"], idx)
                atr_val = self._atr(df, idx)

                if (price > high_n
                        and cur_vol > avg_vol * self.vol_mult
                        and self.rsi_low <= rsi_val <= self.rsi_high
                        and len(positions) < self.max_positions):
                    w = 1.0 / self.max_positions
                    signals.append(SwingSignal(
                        date=date, action="BUY", symbol=sym, price=price,
                        weight=w,
                        reason=f"20日高値ブレイク (RSI={rsi_val:.0f}, vol={cur_vol/avg_vol:.1f}x)",
                    ))
                    positions[sym] = price - self.trail_atr * atr_val
                    highest[sym] = price

        return signals

    @staticmethod
    def _rsi(series: pd.Series, idx: int, period: int = 14) -> float:
        if idx < period + 1:
            return 50.0
        changes = series.iloc[idx - period:idx + 1].diff().dropna()
        gains = changes.where(changes > 0, 0.0).mean()
        losses = (-changes.where(changes < 0, 0.0)).mean()
        if losses == 0:
            return 100.0
        rs = gains / losses
        return float(100 - (100 / (1 + rs)))

    @staticmethod
    def _atr(df: pd.DataFrame, idx: int, period: int = 14) -> float:
        start = max(0, idx - period)
        slc = df.iloc[start:idx + 1]
        tr = pd.concat([
            slc["high"] - slc["low"],
            (slc["high"] - slc["close"].shift(1)).abs(),
            (slc["low"] - slc["close"].shift(1)).abs(),
        ], axis=1).max(axis=1)
        val = tr.mean()
        return float(val) if pd.notna(val) and val > 0 else 0.01



# ── 戦略8: Lead-Lag PCA (日米業種リードラグ) ──

# セクター cyclical/defensive 分類
_US_CYCLICAL = {"XLB", "XLE", "XLF", "XLI", "XLK", "XLC", "XLY"}
_US_DEFENSIVE = {"XLP", "XLU", "XLV", "XLRE"}
_JP_CYCLICAL = {
    "1618.T", "1619.T", "1620.T", "1622.T", "1623.T", "1624.T",
    "1625.T", "1629.T", "1630.T", "1631.T", "1632.T",
}
_JP_DEFENSIVE = {"1617.T", "1621.T", "1626.T", "1627.T", "1628.T", "1633.T"}


def _gram_schmidt(V: np.ndarray) -> np.ndarray:
    """Gram-Schmidt 直交化。V: (N, K)"""
    Q = np.zeros_like(V, dtype=float)
    for i in range(V.shape[1]):
        q = V[:, i].astype(float)
        for j in range(i):
            q = q - np.dot(Q[:, j], q) * Q[:, j]
        norm = np.linalg.norm(q)
        if norm < 1e-12:
            Q[:, i] = 0.0
        else:
            Q[:, i] = q / norm
    return Q


class LeadLagPCA:
    """
    部分空間正則化付きPCAによる日米業種リードラグ投資戦略。

    米国セクターETFの当日close-to-closeリターンから翌営業日の
    日本セクターETFのopen-to-closeリターンを予測し、
    ロングショートポートフォリオを構築する。
    """

    def __init__(
        self,
        us_symbols: list[str] | None = None,
        jp_symbols: list[str] | None = None,
        window_length: int = 60,
        reg_lambda: float = 0.9,
        n_components: int = 3,
        quantile_threshold: float = 0.3,
    ):
        from .data import SECTOR_ETFS, JP_SECTOR_ETFS
        self.us_symbols = us_symbols or SECTOR_ETFS
        self.jp_symbols = jp_symbols or JP_SECTOR_ETFS
        self.window_length = window_length
        self.reg_lambda = reg_lambda
        self.n_components = n_components
        self.quantile_threshold = quantile_threshold
        self.name = f"LeadLagPCA(L={window_length},\u03bb={reg_lambda},K={n_components},q={quantile_threshold})"

    def _build_prior_vectors(self, us_syms: list[str], jp_syms: list[str]) -> np.ndarray:
        """事前部分空間ベクトル V0 (N x 3) を構築。"""
        n_us = len(us_syms)
        n_jp = len(jp_syms)
        n = n_us + n_jp

        # v1: グローバルファクター (全銘柄均等)
        v1 = np.ones(n)

        # v2: 国スプレッドファクター (US正, JP負)
        v2 = np.concatenate([np.ones(n_us), -np.ones(n_jp)])

        # v3: シクリカル/ディフェンシブファクター
        v3 = np.zeros(n)
        for i, sym in enumerate(us_syms):
            v3[i] = 1.0 if sym in _US_CYCLICAL else -1.0
        for i, sym in enumerate(jp_syms):
            v3[n_us + i] = 1.0 if sym in _JP_CYCLICAL else -1.0

        V0 = np.column_stack([v1, v2, v3])
        return _gram_schmidt(V0)

    def _compute_prior_correlation(self, V0: np.ndarray, C_full: np.ndarray) -> np.ndarray:
        """事前エクスポージャー行列 C0 を計算。"""
        D0 = np.diag(np.diag(V0.T @ C_full @ V0))
        C0_raw = V0 @ D0 @ V0.T

        # 対角正規化
        delta = np.diag(C0_raw)
        delta = np.where(delta > 1e-12, delta, 1e-12)
        delta_inv_sqrt = np.diag(1.0 / np.sqrt(delta))
        C0 = delta_inv_sqrt @ C0_raw @ delta_inv_sqrt

        # 対角を1に
        np.fill_diagonal(C0, 1.0)
        return C0

    def generate_signals(self, daily_data: dict[str, pd.DataFrame]) -> list[SwingSignal]:
        signals: list[SwingSignal] = []

        # US/JP シンボルのうち daily_data に存在するもの
        us_syms = [s for s in self.us_symbols if s in daily_data]
        jp_syms = [s for s in self.jp_symbols if s in daily_data]
        if len(us_syms) < 3 or len(jp_syms) < 3:
            return signals

        n_us = len(us_syms)
        n_jp = len(jp_syms)
        n = n_us + n_jp
        K = min(self.n_components, n)
        L = self.window_length

        # US close-to-close リターン
        us_close = pd.DataFrame({s: daily_data[s]["close"] for s in us_syms})
        us_ret = us_close.pct_change().dropna()

        # JP open-to-close リターン
        jp_open = pd.DataFrame({s: daily_data[s]["open"] for s in jp_syms})
        jp_close = pd.DataFrame({s: daily_data[s]["close"] for s in jp_syms})
        jp_ret = ((jp_close - jp_open) / jp_open.replace(0, np.nan)).dropna()

        # 日付アラインメント: JP t日に対して直前のUS営業日リターンを使う
        us_ret_aligned = us_ret.reindex(jp_ret.index, method="ffill").shift(1)

        # 共通日付 (両方とも値があるもの)
        common = us_ret_aligned.dropna().index.intersection(jp_ret.dropna().index)
        if len(common) < L + 10:
            return signals

        us_ret_aligned = us_ret_aligned.loc[common]
        jp_ret_aligned = jp_ret.loc[common]

        # 結合リターン行列 (US | JP)
        joint_ret = pd.concat([us_ret_aligned, jp_ret_aligned], axis=1)

        # 全期間の相関行列 C_full (prior の D0 推定用)
        joint_std = (joint_ret - joint_ret.mean()) / joint_ret.std()
        joint_std = joint_std.dropna()
        if len(joint_std) < L:
            return signals
        C_full = joint_std.T.values @ joint_std.values / len(joint_std)
        np.fill_diagonal(C_full, 1.0)

        # 事前ベクトルと事前相関行列
        V0 = self._build_prior_vectors(us_syms, jp_syms)
        C0 = self._compute_prior_correlation(V0, C_full)

        # ローリングウィンドウでシグナル生成
        dates = joint_ret.index
        for t_idx in range(L, len(dates)):
            t = dates[t_idx]
            window = joint_ret.iloc[t_idx - L:t_idx]

            # ウィンドウ内の平均・標準偏差
            mu = window.mean()
            sigma = window.std()
            sigma = sigma.replace(0, np.nan)
            if sigma.isna().any():
                continue

            # 標準化
            z_window = (window - mu) / sigma

            # サンプル相関行列
            C_t = z_window.T.values @ z_window.values / L
            np.fill_diagonal(C_t, 1.0)

            # 正則化相関行列
            lam = self.reg_lambda
            C_reg = (1 - lam) * C_t + lam * C0

            # 固有値分解 (上位K個)
            eigenvalues, eigenvectors = np.linalg.eigh(C_reg)
            idx_sorted = np.argsort(eigenvalues)[::-1]
            V_top = eigenvectors[:, idx_sorted[:K]]

            # US / JP ブロックに分割
            V_U = V_top[:n_us, :]
            V_J = V_top[n_us:, :]

            # 当日のUS標準化リターン
            us_today = us_ret_aligned.loc[t].values
            z_u = (us_today - mu.values[:n_us]) / sigma.values[:n_us]
            if np.any(np.isnan(z_u)):
                continue

            # シグナル: z_hat = V_J @ V_U^T @ z_u
            f_t = V_U.T @ z_u
            z_hat = V_J @ f_t

            # ランキング → long/short
            n_long = max(1, int(n_jp * self.quantile_threshold))
            n_short = max(1, int(n_jp * self.quantile_threshold))
            ranked_idx = np.argsort(z_hat)
            long_idx = ranked_idx[-n_long:]
            short_idx = ranked_idx[:n_short]

            w_long = 0.5 / n_long
            w_short = -0.5 / n_short

            for i in long_idx:
                sym = jp_syms[i]
                price = daily_data[sym]["open"].asof(t)
                if pd.isna(price) or price <= 0:
                    continue
                signals.append(SwingSignal(
                    date=t, action="REBALANCE", symbol=sym,
                    price=float(price), weight=w_long,
                    reason=f"LeadLag long (z_hat={z_hat[i]:+.3f})",
                ))
            for i in short_idx:
                sym = jp_syms[i]
                price = daily_data[sym]["open"].asof(t)
                if pd.isna(price) or price <= 0:
                    continue
                signals.append(SwingSignal(
                    date=t, action="REBALANCE", symbol=sym,
                    price=float(price), weight=w_short,
                    reason=f"LeadLag short (z_hat={z_hat[i]:+.3f})",
                ))

        return signals


SWING_STRATEGIES = [
    DualMomentum(lookback=40, top_n=3),
    BreakoutSwing(breakout_period=20, vol_mult=1.5, trail_atr=2.0),
]
