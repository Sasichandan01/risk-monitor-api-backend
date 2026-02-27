from datetime import datetime, date
from services.db import db
from config.ssm import config
import logging
import time

logger = logging.getLogger(__name__)

_spot_cache = {'spot': None, 'ts': 0}

# query.py - Add better error handling

def get_tracked_symbols() -> list:
    global _spot_cache

    if time.time() - _spot_cache['ts'] < 30 and _spot_cache['spot']:
        spot = _spot_cache['spot']
    else:
        try:
            spot_value = config.NIFTY_SPOT
            if not spot_value:
                logger.error("NIFTY_SPOT is None in SSM - using fallback 24000")
                spot = 24000.0  # Fallback
            else:
                spot = float(spot_value)
            _spot_cache = {'spot': spot, 'ts': time.time()}
            logger.info("Refreshed spot cache: %.2f", spot)
        except (TypeError, ValueError) as e:
            logger.error("Could not fetch nifty spot: %s - using fallback", e)
            spot = 24000.0  # Fallback
            _spot_cache = {'spot': spot, 'ts': time.time()}

    atm = round(spot / 50) * 50
    strikes = [atm + (i * 50) for i in range(-15, 16)]
    ce_symbols = [f"NIFTY{s}CE" for s in strikes]
    pe_symbols = [f"NIFTY{s}PE" for s in strikes]

    logger.info("ATM=%s spot=%.2f | tracking %d strikes", atm, spot, len(strikes))
    return ce_symbols + pe_symbols

def get_latest_snapshot():
    call_conn = None
    put_conn = None
    try:
        symbols = get_tracked_symbols()
        call_conn = db.get_call_conn()
        put_conn = db.get_put_conn()

        result = {}

        for conn, option_type in [(call_conn, 'CE'), (put_conn, 'PE')]:
            cursor = conn.cursor()

            if symbols:
                cursor.execute("""
                    SELECT DISTINCT ON (symbol, strike, expiry)
                        symbol, strike, expiry, option_type,
                        ltp, overall_risk_score, recommendation
                    FROM option_greeks
                    WHERE symbol = ANY(%s)
                    ORDER BY symbol, strike, expiry, time DESC;
                """, (symbols,))
            else:
                logger.warning("No tracked symbols — fetching all")
                cursor.execute("""
                    SELECT DISTINCT ON (symbol, strike, expiry)
                        symbol, strike, expiry, option_type,
                        ltp, overall_risk_score, recommendation
                    FROM option_greeks
                    ORDER BY symbol, strike, expiry, time DESC;
                """)

            rows = cursor.fetchall()
            logger.info("%s returned %d rows", option_type, len(rows))
            cursor.close()

            for row in rows:
                symbol, strike, expiry, opt_type, ltp, risk_score, recommendation = row
                expiry_str = expiry.strftime('%Y-%m-%d') if isinstance(expiry, date) else str(expiry)

                if expiry_str not in result:
                    result[expiry_str] = []

                result[expiry_str].append({
                    "symbol": symbol,
                    "strike": strike,
                    "type": opt_type,
                    "price": ltp,
                    "risk_score": risk_score,
                    "recommendation": recommendation
                })

        for expiry_str in result:
            result[expiry_str].sort(key=lambda x: (x['strike'], x['type']))

        return {
            "timestamp": datetime.now().strftime("%H:%M:%S"),
            "expiries": result
        }

    except (ValueError, KeyError) as e:
        logger.error("Snapshot query error: %s", str(e))
        return {"timestamp": datetime.now().strftime("%H:%M:%S"), "expiries": {}}
    finally:
        if call_conn:
            db.return_call_conn(call_conn)
        if put_conn:
            db.return_put_conn(put_conn)

def get_option_history(symbol: str, expiry: str, history_range: str):
    range_config = {
        '1D': ("time AT TIME ZONE 'Asia/Kolkata'", "CURRENT_DATE"),
        '1W': ("(date_trunc('minute', time AT TIME ZONE 'Asia/Kolkata') - ((EXTRACT(MINUTE FROM time AT TIME ZONE 'Asia/Kolkata')::int %% 5) * INTERVAL '1 minute'))", "NOW() - INTERVAL '1 week'"),
        '1M': ("(date_trunc('minute', time AT TIME ZONE 'Asia/Kolkata') - ((EXTRACT(MINUTE FROM time AT TIME ZONE 'Asia/Kolkata')::int %% 15) * INTERVAL '1 minute'))", "NOW() - INTERVAL '1 month'"),
        'MAX': ("date_trunc('hour', time AT TIME ZONE 'Asia/Kolkata')", "'-infinity'"),
    }

    if history_range not in range_config:
        return []

    bucket_expr, time_filter = range_config[history_range]
    is_call = symbol.endswith('CE')
    conn = db.get_call_conn() if is_call else db.get_put_conn()

    try:
        cursor = conn.cursor()
        
        # Build query with SQL keywords directly, not as parameters
        query = f"""
            SELECT
                {bucket_expr} AS bucket,
                avg(ltp)    AS ltp,
                avg(delta)  AS delta,
                avg(gamma)  AS gamma,
                avg(theta)  AS theta,
                avg(vega)   AS vega,
                avg(iv)     AS iv,
                max(oi)     AS oi,
                max(volume) AS volume,
                (array_agg(overall_risk_score ORDER BY time DESC))[1] AS overall_risk_score,
                (array_agg(recommendation ORDER BY time DESC))[1]     AS recommendation
            FROM option_greeks
            WHERE symbol = %s
            AND expiry = %s::date
            AND time >= {time_filter}
            GROUP BY bucket
            ORDER BY bucket ASC;
        """
        
        cursor.execute(query, (symbol, expiry))

        rows = cursor.fetchall()
        logger.info("History query for %s %s %s — %d rows", symbol, expiry, history_range, len(rows))
        cursor.close()

        return [
            {
                "time": row[0].strftime("%Y-%m-%dT%H:%M:%S"),
                "ltp": row[1],
                "delta": row[2],
                "gamma": row[3],
                "theta": row[4],
                "vega": row[5],
                "iv": row[6],
                "oi": row[7],
                "volume": row[8],
                "risk_score": row[9],
                "recommendation": row[10]
            }
            for row in rows
        ]

    except (ValueError, KeyError) as e:
        logger.error("History query error: %s", e)
        return []
    finally:
        if is_call:
            db.return_call_conn(conn)
        else:
            db.return_put_conn(conn)
def get_option_latest(symbol: str, expiry: str):
    is_call = symbol.endswith('CE')
    conn = db.get_call_conn() if is_call else db.get_put_conn()

    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT ON (g.symbol, g.strike, g.expiry)
                (g.time AT TIME ZONE 'Asia/Kolkata') AS time,
                g.symbol, g.strike, g.expiry, g.option_type,
                g.ltp, g.delta, g.gamma, g.theta, g.vega, g.iv, g.oi, g.volume,
                r.var_1day, r.risk_pct, r.time_risk, r.theta_burn_pct,
                r.moneyness, r.liquidity_score, r.overall_risk_score,
                r.recommendation, r.dte, r.expected_move
            FROM option_greeks g
            JOIN option_risk_metrics r USING (time, symbol, strike, expiry)
            WHERE g.symbol = %s AND g.expiry = %s
            ORDER BY g.symbol, g.strike, g.expiry, g.time DESC;
        """, (symbol, expiry))

        row = cursor.fetchone()
        cursor.close()

        if not row:
            return None

        return {
            "time":             row[0].strftime("%Y-%m-%dT%H:%M:%S"),
            "symbol":           row[1],
            "strike":           row[2],
            "expiry":           row[3].strftime("%Y-%m-%d"),
            "option_type":      row[4],
            "ltp":              row[5],
            "delta":            row[6],
            "gamma":            row[7],
            "theta":            row[8],
            "vega":             row[9],
            "iv":               row[10],
            "oi":               row[11],
            "volume":           row[12],
            "var_1day":         row[13],
            "risk_pct":         row[14],
            "time_risk":        row[15],
            "theta_burn_pct":   row[16],
            "moneyness":        row[17],
            "liquidity_score":  row[18],
            "risk_score":       row[19],
            "recommendation":   row[20],
            "dte":              row[21],
            "expected_move":    row[22]
        }

    except (ValueError, KeyError) as e:
        logger.error("Latest query error: %s", str(e))
        return None
    finally:
        if is_call:
            db.return_call_conn(conn)
        else:
            db.return_put_conn(conn)
