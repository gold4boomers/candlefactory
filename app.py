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

def single_ohclv(exchange_id, symbol, quoteid, baseid, since, timeframe):
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

async def asingle_ohclv(ex):
    symbol = ex['symbol']
    quoteId = ex['quoteId']
    baseId = ex['baseId']
    since = ex['since']
    timeframe = ex['timeframe']
    exchange_id = ex['id']

    data = []
    limit = 100
    filename = f'{quoteId}_{baseId}-{timeframe}-{exchange_id}.csv'
    exchange = getattr(ccxta, exchange_id)({'enableRateLimit': True})

    timeframe_duration_in_seconds = exchange.parse_timeframe(timeframe)
    timeframe_duration_in_ms = timeframe_duration_in_seconds * 1000
    from_timestamp = exchange.parse8601(since)
    now = exchange.milliseconds()
    await exchange.close()
    while from_timestamp <= now:
        exchange = getattr(ccxta, exchange_id)({'enableRateLimit': True})
        try:
            ohlcvs = await exchange.fetch_ohlcv(symbol, timeframe, from_timestamp, limit)
            if len(ohlcvs) > 0:
                first = ohlcvs[0][0]
                last = ohlcvs[-1][0]
                # print('First candle epoch', first, exchange.iso8601(first))
                # print('Last candle epoch', last, exchange.iso8601(last))
                from_timestamp = last + timeframe_duration_in_ms
                data += ohlcvs
        except (ccxt.ExchangeError, ccxt.AuthenticationError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout) as error:
            print('Got an error', type(error).__name__, error.args, ', retrying in', hold, 'seconds...')
            return
        finally:
            time.sleep(1)
            await exchange.close()
            break
    if data:
        return {'filename': filename, 'data': data, 'base': baseId }


async def multi_ohlcv(list_exchange_symbol):
    input_coroutines = [asingle_ohclv(k) for k in list_exchange_symbol]
    markets = await asyncio.gather(*input_coroutines, return_exceptions=True)
    return markets

if __name__ == '__main__':
    base = 'BTC'
    timeframe = '1d'
    since='2020-01-01T00:00:00Z'

    exchanges = ["binance", "kucoin", "poloniex", "hitbtc", "bittrex", "coinbase", "coinbaseprime", "coinbasepro", "huobipro", "okex"]

    tic = time.time()
    a = asyncio.get_event_loop().run_until_complete(multi_markets(exchanges))


    results = []
    i = 0
    while True:
        i+=1
        list_base = []
        list_exchange_symbol = []

        tic = time.time()
        for ex in a:
            try:
                exchange_id = ex['exchange']
            except:
                print(ex)
                continue
            for coin, coindata in ex['markets'].items():
                symbol = coindata['symbol']
                baseId = coindata['baseId'].upper()
                quoteId = coindata['quoteId'].upper()
                if quoteId == 'BTC' and baseId != 'BTC' and baseId not in results and baseId not in list_base:
                    list_exchange_symbol.append({'id': exchange_id, 
                    'symbol': symbol, 
                    'quoteId': quoteId, 
                    'baseId': baseId, 
                    'since': since, 
                    'timeframe': timeframe})
                    ex['markets'].pop(coin)
                    break
        
        b = asyncio.get_event_loop().run_until_complete(multi_ohlcv(list_exchange_symbol))
        for res in b:
            if res == None:
                continue
            if not 'filename' in res.keys():
                print('no fname')
                continue
            if not 'data' in res.keys():
                print('no data')
                continue
            filename = res['filename']
            data = res['data']
            with open(f'out/{res["filename"]}', mode='w') as output_file:
                csv_writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                csv_writer.writerows(res["data"])
            list_base.append(res['base'])
        results += list_base
    print("async call spend:", time.time() - tic)
