"""Backend-compatible technical snapshot calculation for stock candles.

The backend uses technicalindicators 3.1.0.  The small indicator primitives in
this module intentionally mirror that library's seed and output alignment
rules instead of using pandas rolling operations, whose warmup behavior is not
the same.  ``build_technical_snapshot`` is pure apart from its computedAt
clock value (which can be supplied by tests or callers).
"""

from datetime import datetime, timedelta, timezone
import logging
import math


logger = logging.getLogger(__name__)
MIN_52_WEEK_TRADING_SESSIONS = 250


def round_to(value, decimals=4):
    """Match JS ``Math.round(value * 10 ** decimals) / ...`` for finite values."""
    if value is None or isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    value = float(value)
    if not math.isfinite(value):
        return None
    factor = 10 ** decimals
    return math.floor(value * factor + 0.5) / factor


def _finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))


def _last(values):
    return values[-1] if values else None


def _previous(values, offset=1):
    return values[-1 - offset] if len(values) > offset else None


def _align(values, length):
    return [None] * max(0, length - len(values)) + list(values or [])


def _sma(values, period):
    if len(values) < period:
        return []
    return [sum(values[index - period + 1:index + 1]) / period for index in range(period - 1, len(values))]


def _ema(values, period):
    if len(values) < period:
        return []
    result = []
    previous = sum(values[:period]) / period
    result.append(previous)
    exponent = 2 / (period + 1)
    for value in values[period:]:
        previous = ((value - previous) * exponent) + previous
        result.append(previous)
    return result


def _wema(values, period):
    if len(values) < period:
        return []
    result = []
    previous = sum(values[:period]) / period
    result.append(previous)
    for value in values[period:]:
        previous = ((value - previous) / period) + previous
        result.append(previous)
    return result


def _wilder(values, period):
    """The technicalindicators WilderSmoothing primitive used by ADX."""
    if len(values) < period:
        return []
    result = [sum(values[:period])]
    previous = result[0]
    for value in values[period:]:
        previous = previous - (previous / period) + value
        result.append(previous)
    return result


def _wma(values, period):
    if len(values) < period:
        return []
    denominator = period * (period + 1) / 2
    return [
        sum(value * weight for value, weight in zip(values[index - period + 1:index + 1], range(1, period + 1))) / denominator
        for index in range(period - 1, len(values))
    ]


def _rsi(values, period):
    if len(values) <= period:
        return []
    gains = []
    losses = []
    for index in range(1, len(values)):
        change = values[index] - values[index - 1]
        gains.append(change if change > 0 else 0)
        losses.append(-change if change < 0 else 0)
    average_gain = sum(gains[:period]) / period
    average_loss = sum(losses[:period]) / period
    result = []

    def value():
        if average_loss == 0:
            return 100
        if average_gain == 0:
            return 0
        return round_to(100 - (100 / (1 + (average_gain / average_loss))), 2)

    result.append(value())
    for index in range(period, len(gains)):
        average_gain = ((average_gain * (period - 1)) + gains[index]) / period
        average_loss = ((average_loss * (period - 1)) + losses[index]) / period
        result.append(value())
    return result


def _macd(values, fast_period, slow_period, signal_period):
    # technicalindicators emits its first MACD from the moving-average state
    # after the first slowPeriod inputs, then consumes the next input.
    fast = _align(_ema(values, fast_period), len(values))
    slow = _align(_ema(values, slow_period), len(values))
    raw = []
    for index in range(slow_period - 1, len(values)):
        if fast[index] is not None and slow[index] is not None:
            raw.append(fast[index] - slow[index])
    signal = _ema(raw, signal_period)
    result = []
    for index, macd in enumerate(raw):
        signal_value = signal[index - signal_period + 1] if index >= signal_period - 1 else None
        result.append({
            "MACD": macd,
            "signal": signal_value,
            "histogram": macd - signal_value if signal_value is not None else None,
        })
    return result


def _true_ranges(rows):
    return [
        max(row["high"] - row["low"], abs(row["high"] - rows[index - 1]["close"]), abs(row["low"] - rows[index - 1]["close"]))
        for index, row in enumerate(rows)
        if index > 0
    ]


def _stochastic(highs, lows, closes, period, signal_period):
    result = []
    ks = []
    for index in range(period - 1, len(closes)):
        period_high = max(highs[index - period + 1:index + 1])
        period_low = min(lows[index - period + 1:index + 1])
        denominator = period_high - period_low
        k = ((closes[index] - period_low) / denominator * 100) if denominator else 0
        ks.append(k)
        d = sum(ks[-signal_period:]) / signal_period if len(ks) >= signal_period else None
        result.append({"k": k, "d": d})
    return result


def _adx(rows, period):
    if len(rows) <= 2 * period - 1:
        return []
    trs = []
    plus = []
    minus = []
    for index in range(1, len(rows)):
        current = rows[index]
        previous = rows[index - 1]
        up_move = current["high"] - previous["high"]
        down_move = previous["low"] - current["low"]
        trs.append(max(current["high"] - current["low"], abs(current["high"] - previous["close"]), abs(current["low"] - previous["close"])))
        plus.append(up_move if up_move > down_move and up_move > 0 else 0)
        minus.append(down_move if down_move > up_move and down_move > 0 else 0)

    smoothed_tr = _wilder(trs, period)
    smoothed_plus = _wilder(plus, period)
    smoothed_minus = _wilder(minus, period)
    dx_values = []
    pdi_values = []
    mdi_values = []
    for index in range(len(smoothed_tr)):
        pdi = smoothed_plus[index] * 100 / smoothed_tr[index]
        mdi = smoothed_minus[index] * 100 / smoothed_tr[index]
        total = pdi + mdi
        dx = abs(pdi - mdi) / total * 100 if total else float("nan")
        pdi_values.append(pdi)
        mdi_values.append(mdi)
        dx_values.append(dx)

    adx_values = _wema(dx_values, period)
    result = []
    first_adx = period - 1
    for index, adx in enumerate(adx_values):
        source_index = first_adx + index
        result.append({"adx": adx, "pdi": pdi_values[source_index], "mdi": mdi_values[source_index]})
    return result


def _cci(rows, period):
    typical = [(row["high"] + row["low"] + row["close"]) / 3 for row in rows]
    result = []
    for index in range(period - 1, len(rows)):
        window = typical[index - period + 1:index + 1]
        average = sum(window) / period
        deviation = sum(abs(value - average) for value in window) / period
        result.append((typical[index] - average) / (0.015 * deviation) if deviation else float("nan"))
    return result


def _bollinger(values, period, standard_deviation):
    result = []
    for index in range(period - 1, len(values)):
        window = values[index - period + 1:index + 1]
        middle = sum(window) / period
        deviation = math.sqrt(sum((value - middle) ** 2 for value in window) / period)
        upper = middle + deviation * standard_deviation
        lower = middle - deviation * standard_deviation
        result.append({
            "upper": upper,
            "middle": middle,
            "lower": lower,
            "percentB": (values[index] - lower) / (upper - lower) if upper != lower else float("nan"),
        })
    return result


def _stochastic_rsi(values):
    rsi = _rsi(values, 14)
    stochastic_k = []
    stochastic_d = []
    result = []
    for index in range(13, len(rsi)):
        window = rsi[index - 13:index + 1]
        high = max(window)
        low = min(window)
        k = ((rsi[index] - low) / (high - low) * 100) if high != low else 0
        stochastic_k.append(k)
        d = sum(stochastic_k[-3:]) / 3 if len(stochastic_k) >= 3 else None
        if d is not None:
            stochastic_d.append(d)
            final_d = sum(stochastic_d[-3:]) / 3 if len(stochastic_d) >= 3 else None
            if final_d is not None:
                result.append({"k": d, "d": final_d})
    return result


def _ichimoku(rows):
    result = []
    for index in range(51, len(rows)):
        conversion_window = rows[index - 8:index + 1]
        base_window = rows[index - 25:index + 1]
        span_window = rows[index - 51:index + 1]
        conversion = (max(row["high"] for row in conversion_window) + min(row["low"] for row in conversion_window)) / 2
        base = (max(row["high"] for row in base_window) + min(row["low"] for row in base_window)) / 2
        span_b = (max(row["high"] for row in span_window) + min(row["low"] for row in span_window)) / 2
        result.append({"conversion": conversion, "base": base, "spanA": (conversion + base) / 2, "spanB": span_b})
    return result


def _percent_distance(base, current):
    return round_to((current - base) / base * 100) if base is not None and current is not None and base != 0 else None


def _percent_of_price(value, price):
    return round_to(value / price * 100) if value is not None and price is not None and price != 0 else None


def _range_position(low, high, current):
    return round_to((current - low) / (high - low) * 100) if low is not None and high is not None and current is not None and high != low else None


def _percentile_rank_below(values, current, minimum):
    valid = [value for value in values if value is not None and _finite(value)]
    if current is None or not _finite(current) or len(valid) < minimum:
        return None
    return round_to(sum(value < current for value in valid) / len(valid) * 100, 2)


def _rolling(rows, count, field, exclude_latest=False, minimum=False):
    eligible = rows[:-1] if exclude_latest else rows
    if len(eligible) < count:
        return None
    operation = min if minimum else max
    return operation(row[field] for row in eligible[-count:])


def _year_before(value):
    try:
        return value.replace(year=value.year - 1)
    except ValueError:
        return value.replace(year=value.year - 1, day=28)


def _pivot(previous, kind):
    if previous is None:
        return {"asOf": None, "pivot": None, "support": [None, None, None], "resistance": [None, None, None]}
    required_fields = ("open", "high", "low", "close") if kind == "demark" else ("high", "low", "close")
    if not all(_finite(previous.get(field)) for field in required_fields):
        return {"asOf": previous.get("date"), "pivot": None, "support": [None, None, None], "resistance": [None, None, None]}
    high, low, close = previous["high"], previous["low"], previous["close"]
    pivot = (high + low + close) / 3
    if kind == "classic":
        support = [(2 * pivot) - high, pivot - (high - low), low - (2 * (high - pivot))]
        resistance = [(2 * pivot) - low, pivot + high - low, high + (2 * (pivot - low))]
    elif kind == "fibonacci":
        support = [pivot - 0.382 * (high - low), pivot - 0.618 * (high - low), pivot - high + low]
        resistance = [pivot + 0.382 * (high - low), pivot + 0.618 * (high - low), pivot + high - low]
    else:
        open_price = previous["open"]
        x = high + 2 * low + close if close < open_price else (2 * high) + low + close if close > open_price else high + low + 2 * close
        return {"asOf": previous.get("date"), "pivot": round_to(x / 4), "support": [round_to(x / 2 - high), None, None], "resistance": [round_to(x / 2 - low), None, None]}
    return {"asOf": previous.get("date"), "pivot": round_to(pivot), "support": [round_to(value) for value in support], "resistance": [round_to(value) for value in resistance]}


def _obv(rows):
    if len(rows) < 2:
        return []
    result = [0]
    for index in range(1, len(rows)):
        value = result[-1]
        if rows[index]["close"] > rows[index - 1]["close"]:
            value += rows[index]["volume"]
        elif rows[index]["close"] < rows[index - 1]["close"]:
            value -= rows[index]["volume"]
        result.append(value)
    return result


def _cmf(rows, count):
    if len(rows) < count:
        return None
    flow = 0
    volume = 0
    for row in rows[-count:]:
        price_range = row["high"] - row["low"]
        multiplier = 0 if price_range == 0 else ((row["close"] - row["low"]) - (row["high"] - row["close"])) / price_range
        flow += multiplier * row["volume"]
        volume += row["volume"]
    return round_to(flow / volume) if volume > 0 else None


def _historical_volatility(closes, period):
    if len(closes) < period + 1:
        return None
    window = closes[-(period + 1):]
    if any(value is None or value <= 0 for value in window):
        return None
    returns = [math.log(window[index] / window[index - 1]) for index in range(1, len(window))]
    average = sum(returns) / len(returns)
    variance = sum((value - average) ** 2 for value in returns) / len(returns)
    return round_to(math.sqrt(variance) * math.sqrt(252) * 100)


def _rating_label(score):
    if score is None:
        return None
    if score < -0.5:
        return "Strong Sell"
    if score < -0.1:
        return "Sell"
    if score <= 0.1:
        return "Neutral"
    if score <= 0.5:
        return "Buy"
    return "Strong Buy"


def _rating_summary(scores):
    if not scores or any(score is None for score in scores):
        return {"score": None, "label": None, "buyCount": None, "sellCount": None, "neutralCount": None}
    score = round_to(sum(scores) / len(scores))
    return {"score": score, "label": _rating_label(score), "buyCount": scores.count(1), "sellCount": scores.count(-1), "neutralCount": scores.count(0)}


def _compare(indicator, price):
    if indicator is None or price is None:
        return None
    return 1 if indicator < price else -1 if indicator > price else 0


def _safe(call, default):
    try:
        return call()
    except Exception as error:  # parity with the backend's safeIndicator wrapper
        logger.warning("Technical indicator calculation failed: %s", error)
        return default


def build_technical_snapshot(symbol, rows, computed_at=None):
    """Build the persisted nested snapshot body from ascending OHLCV rows."""
    if not rows:
        raise ValueError("rows must not be empty")
    closes = [row["close"] for row in rows]
    highs = [row["high"] for row in rows]
    lows = [row["low"] for row in rows]
    volumes = [row["volume"] for row in rows]
    last = rows[-1]
    length = len(rows)

    ema = {period: _align(_safe(lambda period=period: _ema(closes, period), []), length) for period in (10, 20, 30, 50, 100, 200)}
    sma = {period: _align(_safe(lambda period=period: _sma(closes, period), []), length) for period in (10, 20, 30, 50, 100, 200)}
    hma_values = []
    if length >= 9:
        half = _align(_wma(closes, 4), length)
        full = _align(_wma(closes, 9), length)
        differences = [(2 * half[index]) - full[index] for index in range(length) if half[index] is not None and full[index] is not None]
        hma_values = _align(_wma(differences, 3), length)
    vwma = [None] * length
    for index in range(19, length):
        window_volume = sum(volumes[index - 19:index + 1])
        if window_volume > 0:
            vwma[index] = sum(closes[item] * volumes[item] for item in range(index - 19, index + 1)) / window_volume

    rsi_series = _align(_safe(lambda: _rsi(closes, 14), []), length)
    momentum = [None] * length
    for index in range(10, length):
        momentum[index] = closes[index] - closes[index - 10]
    roc_series = _align(_safe(lambda: [((closes[index] - closes[index - 10]) / closes[index - 10] * 100) for index in range(10, length)], []), length)
    macd_series = _align(_safe(lambda: _macd(closes, 12, 26, 9), []), length)
    bollinger_series = _align(_safe(lambda: _bollinger(closes, 20, 2), []), length)
    atr_series = {period: _align(_safe(lambda period=period: _wema(_true_ranges(rows), period), []), length) for period in (7, 14, 21)}
    stochastic_series = _align(_safe(lambda: _stochastic(highs, lows, closes, 14, 3), []), length)
    williams_series = _align(_safe(lambda: [((max(highs[index - 13:index + 1]) - closes[index]) / (max(highs[index - 13:index + 1]) - min(lows[index - 13:index + 1])) * -100) for index in range(13, length)], []), length)
    adx_series = _align(_safe(lambda: _adx(rows, 14), []), length)
    cci_series = _align(_safe(lambda: _cci(rows, 20), []), length)
    ao_series = _align(_safe(lambda: [sum((highs[item] + lows[item]) / 2 for item in range(index - 4, index + 1)) / 5 - sum((highs[item] + lows[item]) / 2 for item in range(index - 33, index + 1)) / 34 for index in range(33, length)], []), length)
    stoch_rsi_series = _align(_safe(lambda: _stochastic_rsi(closes), []), length)
    ichimoku_series = _align(_safe(lambda: _ichimoku(rows), []), length)

    last_bollinger = _last(bollinger_series)
    previous_bollinger = _previous(bollinger_series)

    def bandwidth(entry):
        return ((entry["upper"] - entry["lower"]) / entry["middle"] * 100) if entry and entry.get("middle") else None

    current_bandwidth = bandwidth(last_bollinger)
    previous_bandwidth = bandwidth(previous_bollinger)
    bandwidth_percentile = _percentile_rank_below([bandwidth(entry) for entry in bollinger_series[-120:]], current_bandwidth, 60)
    bollinger = {
        "upper": round_to(last_bollinger.get("upper") if last_bollinger else None),
        "middle": round_to(last_bollinger.get("middle") if last_bollinger else None),
        "lower": round_to(last_bollinger.get("lower") if last_bollinger else None),
        "bandwidthPct": round_to(current_bandwidth),
        "bandwidthChangePct": _percent_distance(previous_bandwidth, current_bandwidth),
        "bandwidthPercentile120D": bandwidth_percentile,
        "percentB": round_to(last_bollinger.get("percentB") * 100) if last_bollinger and _finite(last_bollinger.get("percentB")) else None,
    }

    moving_averages = {f"ema{period}": round_to(_last(ema[period])) for period in (10, 20, 30, 50, 100, 200)}
    moving_averages.update({f"sma{period}": round_to(_last(sma[period])) for period in (10, 20, 30, 50, 100, 200)})
    moving_averages.update({"hma9": round_to(_last(hma_values)), "vwma20": round_to(_last(vwma))})
    last_macd = _last(macd_series) or {}
    last_stochastic = _last(stochastic_series) or {}
    last_adx = _last(adx_series) or {}
    last_stoch_rsi = _last(stoch_rsi_series) or {}
    last_ichimoku = _last(ichimoku_series) or {}
    ichimoku = {"conversionLine9": round_to(last_ichimoku.get("conversion")), "baseLine26": round_to(last_ichimoku.get("base")), "leadingSpanA": round_to(last_ichimoku.get("spanA")), "leadingSpanB": round_to(last_ichimoku.get("spanB"))}
    atr = {period: round_to(_last(atr_series[period])) for period in (7, 14, 21)}
    previous_atr14 = round_to(_previous(atr_series[14]))

    year_before = _year_before(last["date"])
    year_rows = [row for row in rows if row["date"] >= year_before]
    high52 = round_to(max(row["high"] for row in year_rows)) if len(year_rows) >= MIN_52_WEEK_TRADING_SESSIONS else None
    low52 = round_to(min(row["low"] for row in year_rows)) if len(year_rows) >= MIN_52_WEEK_TRADING_SESSIONS else None
    highs20 = round_to(_rolling(rows, 20, "high")) if length >= 20 else None
    highs30 = round_to(_rolling(rows, 30, "high")) if length >= 30 else None
    highs50 = round_to(_rolling(rows, 50, "high")) if length >= 50 else None
    lows20 = round_to(_rolling(rows, 20, "low", minimum=True)) if length >= 20 else None
    lows30 = round_to(_rolling(rows, 30, "low", minimum=True)) if length >= 30 else None
    lows50 = round_to(_rolling(rows, 50, "low", minimum=True)) if length >= 50 else None
    previous_row = rows[-2] if length >= 2 else None

    obv = _obv(rows)
    latest_obv = _last(obv)
    obv_ago = obv[-21] if len(obv) > 20 else None
    obv_slope = round_to((latest_obv - obv_ago) / 20) if latest_obv is not None and obv_ago is not None else None
    latest_rsi, previous_rsi = _last(rsi_series), _previous(rsi_series)
    latest_cci, previous_cci = _last(cci_series), _previous(cci_series)
    latest_momentum, previous_momentum = _last(momentum), _previous(momentum)
    latest_williams, previous_williams = _last(williams_series), _previous(williams_series)
    latest_ao, previous_ao, before_ao = _last(ao_series), _previous(ao_series), _previous(ao_series, 2)

    ichimoku_rating = None
    if all(value is not None for value in ichimoku.values()):
        if ichimoku["leadingSpanA"] > ichimoku["leadingSpanB"] and ichimoku["baseLine26"] > ichimoku["leadingSpanA"] and ichimoku["conversionLine9"] > ichimoku["baseLine26"] and last["close"] > ichimoku["conversionLine9"]:
            ichimoku_rating = 1
        elif ichimoku["leadingSpanA"] < ichimoku["leadingSpanB"] and ichimoku["baseLine26"] < ichimoku["leadingSpanA"] and ichimoku["conversionLine9"] < ichimoku["baseLine26"] and last["close"] < ichimoku["conversionLine9"]:
            ichimoku_rating = -1
        else:
            ichimoku_rating = 0

    ao_rating = None
    if latest_ao is not None and previous_ao is not None and before_ao is not None:
        if (latest_ao > 0 and previous_ao > 0 and latest_ao > previous_ao and previous_ao < before_ao) or (previous_ao <= 0 < latest_ao):
            ao_rating = 1
        elif (latest_ao < 0 and previous_ao < 0 and latest_ao < previous_ao and previous_ao > before_ao) or (previous_ao >= 0 > latest_ao):
            ao_rating = -1
        else:
            ao_rating = 0

    bull = [row["high"] - value if value is not None else None for row, value in zip(rows, ema[50])]
    bear = [row["low"] - value if value is not None else None for row, value in zip(rows, ema[50])]
    bull_rating = None
    bull_latest, bull_previous, bear_latest, bear_previous = _last(bull), _previous(bull), _last(bear), _previous(bear)
    if all(value is not None for value in (bull_latest, bull_previous, bear_latest, bear_previous, last["close"], moving_averages["ema50"])):
        bull_rating = 1 if last["close"] > moving_averages["ema50"] and bear_latest < 0 and bear_latest > bear_previous else -1 if last["close"] < moving_averages["ema50"] and bull_latest > 0 and bull_latest < bull_previous else 0

    ma_scores = [_compare(moving_averages[key], last["close"]) for key in ("sma10", "ema10", "sma20", "ema20", "sma30", "ema30", "sma50", "ema50", "sma100", "ema100", "sma200", "ema200", "hma9", "vwma20")] + [ichimoku_rating]
    stochastic_rsi_rating = None
    if last_stoch_rsi.get("k") is not None and last_stoch_rsi.get("d") is not None and moving_averages["ema50"] is not None:
        stochastic_rsi_rating = 1 if last["close"] < moving_averages["ema50"] and last_stoch_rsi["k"] < 20 and last_stoch_rsi["d"] < 20 and last_stoch_rsi["k"] > last_stoch_rsi["d"] else -1 if last["close"] > moving_averages["ema50"] and last_stoch_rsi["k"] > 80 and last_stoch_rsi["d"] > 80 and last_stoch_rsi["k"] < last_stoch_rsi["d"] else 0
    adx_previous = _previous(adx_series) or {}
    oscillator_scores = [
        1 if latest_rsi is not None and previous_rsi is not None and latest_rsi < 30 and latest_rsi > previous_rsi else -1 if latest_rsi is not None and previous_rsi is not None and latest_rsi > 70 and latest_rsi < previous_rsi else 0 if latest_rsi is not None and previous_rsi is not None else None,
        1 if last_stochastic.get("k") is not None and last_stochastic.get("d") is not None and last_stochastic["k"] < 20 and last_stochastic["d"] < 20 and last_stochastic["k"] > last_stochastic["d"] else -1 if last_stochastic.get("k") is not None and last_stochastic.get("d") is not None and last_stochastic["k"] > 80 and last_stochastic["d"] > 80 and last_stochastic["k"] < last_stochastic["d"] else 0 if last_stochastic.get("k") is not None and last_stochastic.get("d") is not None else None,
        1 if latest_cci is not None and previous_cci is not None and latest_cci < -100 and latest_cci > previous_cci else -1 if latest_cci is not None and previous_cci is not None and latest_cci > 100 and latest_cci < previous_cci else 0 if latest_cci is not None and previous_cci is not None else None,
        1 if last_adx.get("adx") is not None and adx_previous.get("adx") is not None and last_adx.get("pdi") is not None and last_adx.get("mdi") is not None and last_adx["adx"] > 20 and last_adx["adx"] > adx_previous["adx"] and last_adx["pdi"] > last_adx["mdi"] else -1 if last_adx.get("adx") is not None and adx_previous.get("adx") is not None and last_adx.get("pdi") is not None and last_adx.get("mdi") is not None and last_adx["adx"] > 20 and last_adx["adx"] > adx_previous["adx"] and last_adx["pdi"] < last_adx["mdi"] else 0 if last_adx.get("adx") is not None and adx_previous.get("adx") is not None and last_adx.get("pdi") is not None and last_adx.get("mdi") is not None else None,
        ao_rating,
        1 if latest_momentum is not None and previous_momentum is not None and latest_momentum > previous_momentum else -1 if latest_momentum is not None and previous_momentum is not None and latest_momentum < previous_momentum else 0 if latest_momentum is not None and previous_momentum is not None else None,
        1 if last_macd.get("MACD") is not None and last_macd.get("signal") is not None and last_macd["MACD"] > last_macd["signal"] else -1 if last_macd.get("MACD") is not None and last_macd.get("signal") is not None else None,
        stochastic_rsi_rating,
        1 if latest_williams is not None and previous_williams is not None and latest_williams < -80 and latest_williams > previous_williams else -1 if latest_williams is not None and previous_williams is not None and latest_williams > -20 and latest_williams < previous_williams else 0 if latest_williams is not None and previous_williams is not None else None,
        bull_rating,
        1 if _last(_ultimate(rows)) is not None and _last(_ultimate(rows)) > 70 else -1 if _last(_ultimate(rows)) is not None and _last(_ultimate(rows)) < 30 else 0 if _last(_ultimate(rows)) is not None else None,
    ]
    ma_ratings = _rating_summary(ma_scores)
    oscillator_ratings = _rating_summary(oscillator_scores)
    if ma_ratings["score"] is None or oscillator_ratings["score"] is None:
        overall = _rating_summary([])
    else:
        all_scores = ma_scores + oscillator_scores
        overall_score = round_to((ma_ratings["score"] + oscillator_ratings["score"]) / 2)
        overall = {"score": overall_score, "label": _rating_label(overall_score), "buyCount": all_scores.count(1), "sellCount": all_scores.count(-1), "neutralCount": all_scores.count(0)}

    ultimate = _ultimate(rows)
    return {
        "symbol": symbol,
        "asOf": last["date"],
        "computedAt": computed_at or datetime.now(timezone.utc),
        "snapshotClose": round_to(last["close"]),
        "snapshotVolume": last["volume"] if _finite(last["volume"]) else None,
        "movingAverages": moving_averages,
        "clouds": {"ichimoku": ichimoku},
        "momentum": {"rsi14": round_to(_last(rsi_series)), "macd": {"value": round_to(last_macd.get("MACD")), "signal": round_to(last_macd.get("signal")), "histogram": round_to(last_macd.get("histogram"))}, "cci20": round_to(latest_cci), "momentum10": round_to(latest_momentum), "roc10": round_to(_last(roc_series)), "awesomeOscillator": round_to(latest_ao), "stochastic": {"k14": round_to(last_stochastic.get("k")), "d14": round_to(last_stochastic.get("d"))}, "stochasticRsi": {"k14": round_to(last_stoch_rsi.get("k")), "d14": round_to(last_stoch_rsi.get("d"))}, "williamsR14": round_to(latest_williams), "ultimateOscillator": round_to(_last(ultimate)), "bullPower50": round_to(bull_latest), "bearPower50": round_to(bear_latest)},
        "bands": {"bollinger": bollinger},
        "volatility": {"atr7": atr[7], "atr7Pct": _percent_of_price(atr[7], last["close"]), "atr14": atr[14], "atr14Pct": _percent_of_price(atr[14], last["close"]), "atr14ChangePct": _percent_distance(previous_atr14, atr[14]), "atr21": atr[21], "atr21Pct": _percent_of_price(atr[21], last["close"]), "dailyRangePct": _percent_of_price(last["high"] - last["low"], last["close"]), "historicalVolatility20D": _historical_volatility(closes, 20), "historicalVolatility50D": _historical_volatility(closes, 50)},
        "priceStructure": {"high20D": highs20, "high30D": highs30, "high50D": highs50, "high52W": high52, "low20D": lows20, "low30D": lows30, "low50D": lows50, "low52W": low52, "distanceFromHigh20DPct": _percent_distance(highs20, last["close"]), "distanceFromHigh30DPct": _percent_distance(highs30, last["close"]), "distanceFromHigh50DPct": _percent_distance(highs50, last["close"]), "distanceFromHigh52WPct": _percent_distance(high52, last["close"]), "distanceFromLow20DPct": _percent_distance(lows20, last["close"]), "distanceFromLow30DPct": _percent_distance(lows30, last["close"]), "distanceFromLow50DPct": _percent_distance(lows50, last["close"]), "distanceFromLow52WPct": _percent_distance(low52, last["close"]), "rangePosition20DPct": _range_position(lows20, highs20, last["close"]), "rangePosition30DPct": _range_position(lows30, highs30, last["close"]), "rangePosition50DPct": _range_position(lows50, highs50, last["close"]), "rangePosition52WPct": _range_position(low52, high52, last["close"])},
        "volume": {"avgVolume10D": round_to(sum(volumes[-10:]) / 10, 0) if length >= 10 else None, "avgVolume20D": round_to(sum(volumes[-20:]) / 20, 0) if length >= 20 else None, "avgVolume50D": round_to(sum(volumes[-50:]) / 50, 0) if length >= 50 else None, "avgVolume10DVs50D": round_to((sum(volumes[-10:]) / 10) / (sum(volumes[-50:]) / 50)) if length >= 50 and sum(volumes[-50:]) > 0 else None, "relativeVolume20D": round_to(last["volume"] / (sum(volumes[-20:]) / 20)) if length >= 20 and sum(volumes[-20:]) > 0 else None, "relativeVolume50D": round_to(last["volume"] / (sum(volumes[-50:]) / 50)) if length >= 50 and sum(volumes[-50:]) > 0 else None},
        "volumeFlow": {"obv": round_to(latest_obv, 0) if latest_obv is not None else None, "obvSlope20D": obv_slope, "cmf20D": _cmf(rows, 20)},
        "trendStrength": {"adx14": round_to(last_adx.get("adx")), "plusDI14": round_to(last_adx.get("pdi")), "minusDI14": round_to(last_adx.get("mdi"))},
        "pivots": {"classic": _pivot(previous_row, "classic"), "fibonacci": _pivot(previous_row, "fibonacci"), "demark": _pivot(previous_row, "demark")},
        "ratings": {"overall": overall, "movingAverages": ma_ratings, "oscillators": oscillator_ratings},
        "derived": {"distanceFromEma20Pct": _percent_distance(moving_averages["ema20"], last["close"]), "distanceFromEma50Pct": _percent_distance(moving_averages["ema50"], last["close"]), "distanceFromEma200Pct": _percent_distance(moving_averages["ema200"], last["close"]), "distanceFromSma20Pct": _percent_distance(moving_averages["sma20"], last["close"]), "distanceFromSma50Pct": _percent_distance(moving_averages["sma50"], last["close"]), "distanceFromSma200Pct": _percent_distance(moving_averages["sma200"], last["close"]), "sma50Slope20DPct": _percent_distance(_previous(sma[50], 20), _last(sma[50])), "sma200Slope20DPct": _percent_distance(_previous(sma[200], 20), _last(sma[200])), "ema200Slope20DPct": _percent_distance(_previous(ema[200], 20), _last(ema[200]))},
        "signals": {"isAboveEma50": moving_averages["ema50"] is not None and last["close"] > moving_averages["ema50"] if moving_averages["ema50"] is not None else None, "isAboveEma200": moving_averages["ema200"] is not None and last["close"] > moving_averages["ema200"] if moving_averages["ema200"] is not None else None, "isAboveAllMas": last["close"] > moving_averages["ema20"] and last["close"] > moving_averages["ema50"] and last["close"] > moving_averages["ema200"] if all(moving_averages[key] is not None for key in ("ema20", "ema50", "ema200")) else None, "isGoldenCross": moving_averages["ema50"] > moving_averages["ema200"] if moving_averages["ema50"] is not None and moving_averages["ema200"] is not None else None, "isMacdBullish": last_macd.get("histogram") > 0 if last_macd.get("histogram") is not None else None, "isRsiOverbought": _last(rsi_series) > 70 if _last(rsi_series) is not None else None, "isRsiOversold": _last(rsi_series) < 30 if _last(rsi_series) is not None else None, "isVolumeSpike20D": round_to(last["volume"] / (sum(volumes[-20:]) / 20)) > 2 if length >= 20 and sum(volumes[-20:]) > 0 else None, "isCloseAbovePrior20DHigh": last["close"] > _rolling(rows, 20, "high", True) if _rolling(rows, 20, "high", True) is not None else None, "isCloseAbovePrior50DHigh": last["close"] > _rolling(rows, 50, "high", True) if _rolling(rows, 50, "high", True) is not None else None, "isCloseBelowPrior20DLow": last["close"] < _rolling(rows, 20, "low", True, True) if _rolling(rows, 20, "low", True, True) is not None else None, "isCloseBelowPrior50DLow": last["close"] < _rolling(rows, 50, "low", True, True) if _rolling(rows, 50, "low", True, True) is not None else None, "isStochasticOverbought": last_stochastic.get("k") > 80 if last_stochastic.get("k") is not None else None, "isStochasticOversold": last_stochastic.get("k") < 20 if last_stochastic.get("k") is not None else None, "isAdxTrendStrong": last_adx.get("adx") > 25 if last_adx.get("adx") is not None else None, "isPlusDiAboveMinusDi": last_adx.get("pdi") > last_adx.get("mdi") if last_adx.get("pdi") is not None and last_adx.get("mdi") is not None else None, "isObvRising20D": obv_slope > 0 if obv_slope is not None else None, "isCmfPositive": _cmf(rows, 20) > 0 if _cmf(rows, 20) is not None else None, "isSma200Rising": _percent_distance(_previous(sma[200], 20), _last(sma[200])) > 0 if _percent_distance(_previous(sma[200], 20), _last(sma[200])) is not None else None},
    }


def _ultimate(rows, fast=7, middle=14, slow=28):
    if len(rows) <= slow:
        return []
    pressure = [None] * len(rows)
    true_range = [None] * len(rows)
    result = [None] * len(rows)
    for index in range(1, len(rows)):
        pressure[index] = rows[index]["close"] - min(rows[index]["low"], rows[index - 1]["close"])
        true_range[index] = max(rows[index]["high"], rows[index - 1]["close"]) - min(rows[index]["low"], rows[index - 1]["close"])
        if index >= slow:
            fast_tr = sum(true_range[index - fast + 1:index + 1])
            middle_tr = sum(true_range[index - middle + 1:index + 1])
            slow_tr = sum(true_range[index - slow + 1:index + 1])
            if fast_tr and middle_tr and slow_tr:
                result[index] = 100 * ((4 * sum(pressure[index - fast + 1:index + 1]) / fast_tr + 2 * sum(pressure[index - middle + 1:index + 1]) / middle_tr + sum(pressure[index - slow + 1:index + 1]) / slow_tr) / 7)
    return result


# Compatibility alias used by callers that mirror the backend function name.
buildTechnicalSnapshot = build_technical_snapshot
