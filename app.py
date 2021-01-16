import os, asyncio, csv, time
import ccxt
import logging
import itertools
from collections import namedtuple
# common constants

msec = 1000
minute = 60 * msec
hold = 30
import ccxt.async_support as ccxta

logging.basicConfig(level=logging.INFO)


def sync_client(exchange_id):
    markets = None
    exchange = getattr(ccxt, exchange_id)({'enableRateLimit': True})
    try:
        markets = exchange.load_markets()
    except Exception as e:
        print(type(e).__name__, str(e))
    return { 'exchange': exchange.id, 'markets': markets }

async def async_client(exchange_id):
    markets = None
    exchange = getattr(ccxta, exchange_id)({'enableRateLimit': True})
    try:
        markets = await exchange.load_markets()
    except Exception as e:
        print(type(e).__name__, str(e))
    await exchange.close()
    dict_i_want = { your_key: markets[your_key] for your_key in filter(lambda x: 'BTC' in x, markets)}
    return { 'exchange': exchange.id, 'markets': dict_i_want }

async def multi_markets(exchanges):
    input_coroutines = [async_client(exchange) for exchange in exchanges]
    markets = await asyncio.gather(*input_coroutines, return_exceptions=True)
    return markets

def single_ohclv(exchange_id, symbol, since, timeframe):
    data = []
    limit = 100
    exchange = getattr(ccxt, exchange_id)({'enableRateLimit': True})
    timeframe_duration_in_seconds = exchange.parse_timeframe(timeframe)
    timeframe_duration_in_ms = timeframe_duration_in_seconds * 1000
    from_timestamp = exchange.parse8601(since)
    now = exchange.milliseconds()
    while from_timestamp <= now:
        try:
            ohlcvs = exchange.fetch_ohlcv(symbol, timeframe, from_timestamp, limit=limit)
            if len(ohlcvs) > 0:
                first = ohlcvs[0][0]
                last = ohlcvs[-1][0]
                print('First candle epoch', first, exchange.iso8601(first))
                print('Last candle epoch', last, exchange.iso8601(last))
                from_timestamp = last + timeframe_duration_in_ms
                data += ohlcvs
        except (ccxt.ExchangeError, ccxt.AuthenticationError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout) as error:
            print('Got an error', type(error).__name__, error.args, ', retrying in', hold, 'seconds...')
            time.sleep(hold)

def asingle_ohclv(exchange_id, symbol, since, timeframe):
    data = []
    limit = 100
    exchange = getattr(ccxt, exchange_id)({'enableRateLimit': True})
    print(exchange.id)
    timeframe_duration_in_seconds = exchange.parse_timeframe(timeframe)
    timeframe_duration_in_ms = timeframe_duration_in_seconds * 1000
    from_timestamp = exchange.parse8601(since)
    now = exchange.milliseconds()
    while from_timestamp <= now:
        try:
            ohlcvs = exchange.fetch_ohlcv(symbol, timeframe, from_timestamp, limit=limit)
            if len(ohlcvs) > 0:
                first = ohlcvs[0][0]
                last = ohlcvs[-1][0]
                print('First candle epoch', first, exchange.iso8601(first))
                print('Last candle epoch', last, exchange.iso8601(last))
                from_timestamp = last + timeframe_duration_in_ms
                data += ohlcvs
        except (ccxt.ExchangeError, ccxt.AuthenticationError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout) as error:
            print('Got an error', type(error).__name__, error.args, ', retrying in', hold, 'seconds...')
            time.sleep(hold)

async def multi_ohlcv(list_exchange_symbol, since, timeframe):
    input_coroutines = [asingle_ohclv(k['id'], k['symbol'], since, timeframe) for k in list_exchange_symbol]
    markets = await asyncio.gather(*input_coroutines, return_exceptions=True)
    return markets

if __name__ == '__main__':
    base = 'BTC'
    timeframe = '1d'
    since='2020-01-01T00:00:00Z'

    exchanges = ["kucoin", "bittrex", "bitfinex", "poloniex", "huobipro", "binance", "coinbase"]

    tic = time.time()
    a = asyncio.get_event_loop().run_until_complete(multi_markets(exchanges))

    results = []
    while True:
        list_base = []
        list_exchange_symbol = []
        if len(list_exchange_symbol) >= len(exchanges):
            multi_ohlcv(list_exchange_symbol, since, timeframe)
            print('fetched 6 coins already')
            list_exchange_symbol = []
            results += list_base
            list_base = []
            print(results)
            print("async call spend:", time.time() - tic)
            tic = time.time()
        for ex in a:
            exchange_id = ex['exchange']
            for coin, coindata in ex['markets'].items():
                baseId = coindata['baseId'].upper()
                quoteId = coindata['quoteId'].upper()
                if quoteId == 'BTC' and baseId != 'BTC' and baseId not in results and baseId not in list_base:
                    list_exchange_symbol.append({'id': exchange_id, 'symbol': coindata['id']})
                    list_base.append(baseId)
                    ex['markets'].pop(coin)
                    print(f'{exchange_id}: popped {baseId}/{quoteId}')
                    break

