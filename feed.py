from cryptofeed import FeedHandler
from cryptofeed.exchanges import (Binance, Bitfinex, Gateio, Huobi, OKX, BinanceFutures, GateioFutures, HuobiSwap, Kraken, KuCoin, Bitstamp, Bitmex, HitBTC, OKCoin)
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
    def __init__(self, host='localhost', port=5000, batch_size=2):
        self.conn = None
        self.host = host
        self.port = port
        self.batch_size = batch_size
        self.trades = []
        self.books = []
        self.loop = asyncio.get_event_loop()

    async def connect(self):
        if not self.conn or self.conn.closed:
            self.conn = await kx.AsyncQConnection(host=self.host, port=self.port)

    async def handle_trade(self, t, receipt_timestamp):
        # Prepare a trade tuple for the batch
        trade_tuple = (
            format(receipt_timestamp),
            t.symbol,
            format(t.timestamp),
            t.exchange,
            t.side,
            format(t.amount, '.4f'),
            format(t.price, '.4f')
        )
        self.trades.append(trade_tuple)
        
        # Check if batch size is met
        if len(self.trades) >= self.batch_size:
            await self.publish_trades()

    async def publish_trades(self):
        try:
            await self.connect()
            # Unpack and prepare the batch data
            timestamps, symbols, timestamps_exchange, exchanges, sides, amounts, prices = zip(*self.trades)
            # Format lists properly for Q
            timestamps_str = " ".join(str(t) for t in timestamps)
            timestamps_exchange_str = " ".join([str(te) if te != 'None' else t for te, t in zip(timestamps_exchange, timestamps)])
            symbols_q_format = ";".join(f"`$\"{s}\"" for s in symbols)
            symbols_str = f"({symbols_q_format})"
            exchanges_str = "`" + "`".join(exchanges)
            sides_str = "`" + "`".join(sides)
            amounts_str = " ".join(str(a) for a in amounts)
            prices_str = " ".join(str(p) for p in prices)
            # Construct the full Q update string
            batch_str = f"({timestamps_str}; {symbols_str}; {timestamps_exchange_str}; {exchanges_str}; {sides_str}; {amounts_str}; {prices_str})"
            await self.conn(f".u.upd[`trade; {batch_str}]")
            self.trades = []  # Clear the list after sending
        except Exception as e:
            logging.error(f"Error publishing trades: {e}")

    def calculate_price_with_slippage(self, order_book, order_size, side):
        accumulated_volume = Decimal(0)
        total_cost = Decimal(0)
        
        if side == BID:
            for price, volume in order_book.book[BID].to_list():
                if accumulated_volume + volume >= order_size:
                    total_cost += (order_size - accumulated_volume) * price
                    break
                else:
                    accumulated_volume += volume
                    total_cost += volume * price
        else:  # ASK
            for price, volume in order_book.book[ASK].to_list():
                if accumulated_volume + volume >= order_size:
                    total_cost += (order_size - accumulated_volume) * price
                    break
                else:
                    accumulated_volume += volume
                    total_cost += volume * price
        
        if accumulated_volume == 0:
            return None  # Prevent division by zero

        return total_cost / order_size

    async def handle_book(self, book, receipt_timestamp):
        bid_list = book.book[BID].to_list()
        ask_list = book.book[ASK].to_list()
        
        best_bid_price, best_bid_volume = bid_list[0]
        best_ask_price, best_ask_volume = ask_list[0]

        # Calculate metrics
        bid_ask_spread = best_ask_price - best_bid_price
        market_depth_bids = sum(volume for price, volume in bid_list)
        market_depth_asks = sum(volume for price, volume in ask_list)
        order_book_imbalance = (market_depth_bids - market_depth_asks) / (market_depth_bids + market_depth_asks)
        bid_vwap = sum(price * volume for price, volume in bid_list) / sum(volume for price, volume in bid_list)
        ask_vwap = sum(price * volume for price, volume in ask_list) / sum(volume for price, volume in ask_list)
        vwap = (bid_vwap + ask_vwap) / 2
        order_book_ratio = market_depth_bids / market_depth_asks

        # Calculate price with slippage for a hypothetical order size
        hypothetical_order_size = Decimal('1.0')  # Adjust the order size as needed
        bid_slippage_price = self.calculate_price_with_slippage(book, hypothetical_order_size, BID)
        ask_slippage_price = self.calculate_price_with_slippage(book, hypothetical_order_size, ASK)

        # Prepare a book tuple for the batch
        book_tuple = (
            format(receipt_timestamp),
            book.symbol,
            format(book.timestamp),
            book.exchange,
            format(best_bid_price, '.4f'),
            format(best_bid_volume, '.4f'),
            format(best_ask_price, '.4f'),
            format(best_ask_volume, '.4f'),
            format(bid_ask_spread, '.4f'),
            format(market_depth_bids, '.4f'),
            format(market_depth_asks, '.4f'),
            format(order_book_imbalance, '.4f'),
            format(vwap, '.4f'),
            format(order_book_ratio, '.4f'),
            format(bid_slippage_price, '.4f') if bid_slippage_price else 'None',
            format(ask_slippage_price, '.4f') if ask_slippage_price else 'None'
        )
        self.books.append(book_tuple)
        
        # Check if batch size is met
        if len(self.books) >= self.batch_size:
            await self.publish_books()

    async def publish_books(self):
        try:
            await self.connect()
            # Unpack and prepare the batch data
            timestamps, symbols, timestamps_exchange, exchanges, bid_prices, bid_sizes, ask_prices, ask_sizes, bid_ask_spreads, market_depth_bids, market_depth_asks, order_book_imbalances, vwaps, order_book_ratios, bid_slippage_prices, ask_slippage_prices = zip(*self.books)
            # Format lists properly for Q
            timestamps_str = " ".join(str(t) for t in timestamps)
            timestamps_exchange_str = " ".join([str(te) if te != 'None' else t for te, t in zip(timestamps_exchange, timestamps)])
            symbols_q_format = ";".join(f"`$\"{s}\"" for s in symbols)
            symbols_str = f"({symbols_q_format})"
            exchanges_str = "`" + "`".join(exchanges)
            bid_prices_str = " ".join(str(b) for b in bid_prices)
            bid_sizes_str = " ".join(str(bs) for bs in bid_sizes)
            ask_prices_str = " ".join(str(ap) for ap in ask_prices)
            ask_sizes_str = " ".join(str(as_) for as_ in ask_sizes)
            bid_ask_spreads_str = " ".join(str(sp) for sp in bid_ask_spreads)
            market_depth_bids_str = " ".join(str(md) for md in market_depth_bids)
            market_depth_asks_str = " ".join(str(md) for md in market_depth_asks)
            order_book_imbalances_str = " ".join(str(oi) for oi in order_book_imbalances)
            vwaps_str = " ".join(str(vw) for vw in vwaps)
            order_book_ratios_str = " ".join(str(or_) for or_ in order_book_ratios)
            bid_slippage_prices_str = " ".join(str(sp) if sp != 'None' else '0' for sp in bid_slippage_prices)
            ask_slippage_prices_str = " ".join(str(sp) if sp != 'None' else '0' for sp in ask_slippage_prices)
            # Construct the full Q update string
            batch_str = f"({timestamps_str}; {symbols_str}; {timestamps_exchange_str}; {exchanges_str}; {bid_prices_str}; {bid_sizes_str}; {ask_prices_str}; {ask_sizes_str}; {bid_ask_spreads_str}; {market_depth_bids_str}; {market_depth_asks_str}; {order_book_imbalances_str}; {vwaps_str}; {order_book_ratios_str}; {bid_slippage_prices_str}; {ask_slippage_prices_str})"
            await self.conn(f".u.upd[`quote; {batch_str}]")
            self.books = []  # Clear the list after sending
        except Exception as e:
            logging.error(f"Error publishing books: {e}")

def main():
    # Create an instance of the data handler
    data_handler = DataHandler(batch_size=50)  # Adjust these parameters as needed

    # Setup configuration for the feed handler
    config = {'log': {'filename': 'crypto_feed.log', 'level': 'DEBUG', 'disabled': False}}
    #config = 'config.yaml'
    f = FeedHandler(config=config)

    # Define the common configuration for the feeds
    common_cfg = {
        'symbols': ['BTC-USDT'],
        'channels': [TRADES, L2_BOOK],
        'callbacks': {
            TRADES: data_handler.handle_trade,  # Use the batch handler for trades
            L2_BOOK: data_handler.handle_book   # Use the batch handler for books
        }
    }

    common_cfg_futures = {
        'checksum_validation': True,
        'symbols': ['BTC-USDT-PERP'],
        'channels': [TRADES, L2_BOOK],
        'callbacks':{
            L2_BOOK: data_handler.handle_book,
            TRADES: data_handler.handle_trade
        }
    }

    # Add multiple feeds to the handler with the common configuration
    f.add_feed(Binance(**common_cfg))
    f.add_feed(Bitfinex(**common_cfg))
    f.add_feed(Gateio(**common_cfg))
    f.add_feed(OKX(**common_cfg))
    f.add_feed(Delta(**common_cfg))
    f.add_feed(Huobi(**common_cfg))
    # Kraken, KuCoin, Bitstamp, Bitmex, HitBTC, OKCoin, CryptoDotCom, dYdX
    f.add_feed(Kraken(**common_cfg))
    f.add_feed(Bitstamp(**common_cfg))
    f.add_feed(HitBTC(**common_cfg))
    #f.add_feed(Deribit(**common_cfg))


    # ## Futures 

    f.add_feed(BinanceFutures(**common_cfg_futures))
    f.add_feed(Bitfinex(**common_cfg_futures))
    f.add_feed(GateioFutures(**common_cfg_futures))
    f.add_feed(OKX(**common_cfg_futures))
    f.add_feed(Delta(**common_cfg_futures))
    f.add_feed(HuobiSwap(**common_cfg_futures))

    # Run the feed handler in an async loop
    loop = asyncio.get_event_loop()
    loop.run_until_complete(f.run())

if __name__ == '__main__':
    main()
