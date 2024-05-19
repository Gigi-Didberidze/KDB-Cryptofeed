from cryptofeed import FeedHandler
from cryptofeed.exchanges import (
    Binance, Bitfinex, Gateio, Huobi, OKX, BinanceFutures, GateioFutures, 
    HuobiSwap, Kraken, KuCoin, Bitstamp, Bitmex, HitBTC, OKCoin
)
from cryptofeed.exchanges.cryptodotcom import CryptoDotCom
from cryptofeed.exchanges.dydx import dYdX
from cryptofeed.exchanges.delta import Delta
from cryptofeed.exchanges.deribit import Deribit

from cryptofeed.defines import TRADES, L2_BOOK, BID, ASK
import asyncio
import pykx as kx
from decimal import Decimal
import logging


class DataHandler:
    def __init__(self, host='localhost', port=5000, batch_size=2, order_size=Decimal('1.0')):
        self.conn = None
        self.host = host
        self.port = port
        self.batch_size = batch_size
        self.order_size = order_size
        self.trades = []
        self.books = []
        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(self.connect())

    async def connect(self):
        if not self.conn or self.conn.closed:
            self.conn = await kx.AsyncQConnection(host=self.host, port=self.port)

    async def handle_trade(self, t, receipt_timestamp):
        if t.amount == 0:
            return

        trade_tuple = (
            str(receipt_timestamp),
            t.symbol,
            str(t.timestamp),
            t.exchange,
            t.side,
            str(t.amount),
            str(t.price)
        )
        self.trades.append(trade_tuple)
        
        if len(self.trades) >= self.batch_size:
            await self.publish_trades()

    async def publish_trades(self):
        try:
            timestamps, symbols, timestamps_exchange, exchanges, sides, amounts, prices = zip(*self.trades)
            timestamps_str = " ".join(timestamps)
            timestamps_exchange_str = " ".join(te if te != 'None' else t for te, t in zip(timestamps_exchange, timestamps))
            symbols_q_format = ";".join(f"`$\"{s}\"" for s in symbols)
            symbols_str = f"({symbols_q_format})"
            exchanges_str = "`" + "`".join(exchanges)
            sides_str = "`" + "`".join(sides)
            amounts_str = f"`float$({' '.join(amounts)})"
            prices_str = f"`float$({' '.join(prices)})"
            
            batch_str = f"({timestamps_str}; {symbols_str}; {timestamps_exchange_str}; {exchanges_str}; {sides_str}; {amounts_str}; {prices_str})"
            await self.conn(f".u.upd[`trade; {batch_str}]")
            self.trades.clear()
        except Exception as e:
            logging.error(f"Error publishing trades: {e}")

    def calculate_price_with_slippage(self, price_volume_list, order_size):
        accumulated_volume = Decimal(0)
        total_cost = Decimal(0)
        
        for price, volume in price_volume_list:
            if accumulated_volume + volume >= order_size:
                total_cost += (order_size - accumulated_volume) * price
                break
            else:
                accumulated_volume += volume
                total_cost += volume * price
        
        if accumulated_volume == 0:
            return None

        return total_cost / order_size

    async def handle_book(self, book, receipt_timestamp):
        bid_list = book.book[BID].to_list()
        ask_list = book.book[ASK].to_list()

        midprice_initial = (bid_list[0][0] + ask_list[0][0]) / 2
        bid_threshold = Decimal('0.99') * midprice_initial
        ask_threshold = Decimal('1.01') * midprice_initial

        filtered_bids = [(price, volume) for price, volume in bid_list if price >= bid_threshold]
        filtered_asks = [(price, volume) for price, volume in ask_list if price <= ask_threshold]

        if not filtered_bids or not filtered_asks:
            return

        best_bid_price, best_bid_volume = filtered_bids[0]
        best_ask_price, best_ask_volume = filtered_asks[0]

        midprice = (best_bid_price + best_ask_price) / 2
        bid_ask_spread = best_ask_price - best_bid_price
        market_depth_bids = sum(volume for price, volume in filtered_bids)
        market_depth_asks = sum(volume for price, volume in filtered_asks)
        order_book_imbalance = (market_depth_bids - market_depth_asks) / (market_depth_bids + market_depth_asks)
        bid_vwap = sum(price * volume for price, volume in filtered_bids) / sum(volume for price, volume in filtered_bids)
        ask_vwap = sum(price * volume for price, volume in filtered_asks) / sum(volume for price, volume in filtered_asks)
        vwap = (bid_vwap + ask_vwap) / 2
        order_book_ratio = market_depth_bids / market_depth_asks if market_depth_asks != 0 else None

        bid_slippage_price = self.calculate_price_with_slippage(filtered_bids, self.order_size)
        ask_slippage_price = self.calculate_price_with_slippage(filtered_asks, self.order_size)

        bid_slippage_price_str = str(bid_slippage_price) if bid_slippage_price else '0.0000'
        ask_slippage_price_str = str(ask_slippage_price) if ask_slippage_price else '0.0000'

        book_tuple = (
            str(receipt_timestamp),
            book.symbol,
            str(book.timestamp),
            book.exchange,
            str(best_bid_price),
            str(best_bid_volume),
            str(best_ask_price),
            str(best_ask_volume),
            str(midprice),
            str(bid_ask_spread),
            str(market_depth_bids),
            str(market_depth_asks),
            str(order_book_imbalance),
            str(vwap),
            str(order_book_ratio) if order_book_ratio is not None else 'None',
            bid_slippage_price_str,
            ask_slippage_price_str
        )
        self.books.append(book_tuple)
        
        if len(self.books) >= self.batch_size:
            await self.publish_books()

    async def publish_books(self):
        try:
            (
                timestamps, symbols, timestamps_exchange, exchanges, bid_prices, bid_sizes,
                ask_prices, ask_sizes, midprices, bid_ask_spreads, market_depth_bids,
                market_depth_asks, order_book_imbalances, vwaps, order_book_ratios,
                bid_slippage_prices, ask_slippage_prices
            ) = zip(*self.books)

            timestamps_str = " ".join(timestamps)
            timestamps_exchange_str = " ".join(te if te != 'None' else t for te, t in zip(timestamps_exchange, timestamps))
            symbols_q_format = ";".join(f"`$\"{s}\"" for s in symbols)
            symbols_str = f"({symbols_q_format})"
            exchanges_str = "`" + "`".join(exchanges)
            bid_prices_str = f"`float$({' '.join(bid_prices)})"
            bid_sizes_str = f"`float$({' '.join(bid_sizes)})"
            ask_prices_str = f"`float$({' '.join(ask_prices)})"
            ask_sizes_str = f"`float$({' '.join(ask_sizes)})"
            midprices_str = f"`float$({' '.join(midprices)})"
            bid_ask_spreads_str = f"`float$({' '.join(bid_ask_spreads)})"
            market_depth_bids_str = f"`float$({' '.join(market_depth_bids)})"
            market_depth_asks_str = f"`float$({' '.join(market_depth_asks)})"
            order_book_imbalances_str = f"`float$({' '.join(order_book_imbalances)})"
            vwaps_str = f"`float$({' '.join(vwaps)})"
            order_book_ratios_str = f"`float$({' '.join(or_ if or_ != 'None' else '0' for or_ in order_book_ratios)})"
            bid_slippage_prices_str = f"`float$({' '.join(sp if sp != 'None' else '0' for sp in bid_slippage_prices)})"
            ask_slippage_prices_str = f"`float$({' '.join(sp if sp != 'None' else '0' for sp in ask_slippage_prices)})"

            batch_str = (
                f"({timestamps_str}; {symbols_str}; {timestamps_exchange_str}; {exchanges_str}; {bid_prices_str}; "
                f"{bid_sizes_str}; {ask_prices_str}; {ask_sizes_str}; {midprices_str}; {bid_ask_spreads_str}; "
                f"{market_depth_bids_str}; {market_depth_asks_str}; {order_book_imbalances_str}; {vwaps_str}; "
                f"{order_book_ratios_str}; {bid_slippage_prices_str}; {ask_slippage_prices_str})"
            )
            await self.conn(f".u.upd[`quote; {batch_str}]")
            self.books.clear()
        except Exception as e:
            logging.error(f"Error publishing books: {e}")

def main():
    data_handler = DataHandler(batch_size=50)

    config = {'log': {'filename': 'crypto_feed.log', 'level': 'DEBUG', 'disabled': False}}
    f = FeedHandler(config=config)

    common_cfg = {
        'symbols': ['BTC-USDT'],
        'channels': [TRADES, L2_BOOK],
        'callbacks': {
            TRADES: data_handler.handle_trade,
            L2_BOOK: data_handler.handle_book
        }
    }

    common_cfg_futures = {
        'checksum_validation': True,
        'symbols': ['BTC-USDT-PERP'],
        'channels': [TRADES, L2_BOOK],
        'callbacks': {
            L2_BOOK: data_handler.handle_book,
            TRADES: data_handler.handle_trade
        }
    }

    f.add_feed(Binance(**common_cfg))
    f.add_feed(Bitfinex(**common_cfg))
    f.add_feed(Gateio(**common_cfg))
    f.add_feed(OKX(**common_cfg))
    f.add_feed(Delta(**common_cfg))
    f.add_feed(Huobi(**common_cfg))
    f.add_feed(Kraken(**common_cfg))
    f.add_feed(Bitstamp(**common_cfg))
    f.add_feed(HitBTC(**common_cfg))

    f.add_feed(BinanceFutures(**common_cfg_futures))
    f.add_feed(Bitfinex(**common_cfg_futures))
    f.add_feed(GateioFutures(**common_cfg_futures))
    f.add_feed(OKX(**common_cfg_futures))
    f.add_feed(Delta(**common_cfg_futures))
    f.add_feed(HuobiSwap(**common_cfg_futures))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(f.run())

if __name__ == '__main__':
    main()
