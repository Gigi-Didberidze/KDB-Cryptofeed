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
        self.loop.run_until_complete(self.connect())

    async def connect(self):
        if not self.conn or self.conn.closed:
            self.conn = await kx.AsyncQConnection(host=self.host, port=self.port)

    async def handle_trade(self, t, receipt_timestamp):
        # Validate and filter out trades with zero amount
        if t.amount == 0:
            #print(f"Invalid trade with zero amount: {t}")
            return

        # Prepare a trade tuple for the batch
        trade_tuple = (
            format(receipt_timestamp),
            t.symbol,
            format(t.timestamp),
            t.exchange,
            t.side,
            format(float(t.amount)),
            format(float(t.price))
        )
        self.trades.append(trade_tuple)
        
        # Check if batch size is met
        if len(self.trades) >= self.batch_size:
            await self.publish_trades()

    async def publish_trades(self):
        try:
            # Unpack and prepare the batch data
            timestamps, symbols, timestamps_exchange, exchanges, sides, amounts, prices = zip(*self.trades)
            # Format lists properly for Q
            timestamps_str = " ".join(str(t) for t in timestamps)
            timestamps_exchange_str = " ".join([str(te) if te != 'None' else t for te, t in zip(timestamps_exchange, timestamps)])
            symbols_q_format = ";".join(f"`$\"{s}\"" for s in symbols)
            symbols_str = f"({symbols_q_format})"
            exchanges_str = "`" + "`".join(exchanges)
            sides_str = "`" + "`".join(sides)
            amounts_str_q = " ".join(str(a) for a in amounts)
            amounts_str = f"`float$({amounts_str_q})"
            prices_str_q = " ".join(str(p) for p in prices)
            prices_str = f"`float$({prices_str_q})"
            # Construct the full Q update string
            batch_str = f"({timestamps_str}; {symbols_str}; {timestamps_exchange_str}; {exchanges_str}; {sides_str}; {amounts_str}; {prices_str})"
            await self.conn(f".u.upd[`trade; {batch_str}]")
            self.trades = []  # Clear the list after sending
        except Exception as e:
            logging.error(f"Error publishing trades: {e}")


    async def handle_book(self, book, receipt_timestamp):

        best_bid_price, best_bid_volume = book.book.bids.index(0)[0], book.book.bids.index(0)[1]
        best_ask_price, best_ask_volume = book.book.asks.index(0)[0], book.book.asks.index(0)[1]


        # Prepare a book tuple for the batch
        book_tuple = (
            format(receipt_timestamp),
            book.symbol,
            format(book.timestamp),
            book.exchange,
            format(float(book.book.bids.index(0)[0])),
            format(float(book.book.bids.index(0)[1])),
            format(float(book.book.asks.index(0)[0])),
            format(float(book.book.asks.index(0)[1])),
        )
        self.books.append(book_tuple)
        
        # Check if batch size is met
        if len(self.books) >= self.batch_size:
            await self.publish_books()

    async def publish_books(self):
        try:
            # Unpack and prepare the batch data
            timestamps, symbols, timestamps_exchange, exchanges, bid_prices, bid_sizes, ask_prices, ask_sizes = zip(*self.books)
            # Format lists properly for Q
            timestamps_str = " ".join(str(t) for t in timestamps)
            timestamps_exchange_str = " ".join([str(te) if te != 'None' else t for te, t in zip(timestamps_exchange, timestamps)])
            symbols_q_format = ";".join(f"`$\"{s}\"" for s in symbols)
            symbols_str = f"({symbols_q_format})"
            exchanges_str = "`" + "`".join(exchanges)
            bid_prices_str_q = " ".join(str(b) for b in bid_prices)
            bid_prices_str = f"`float$({bid_prices_str_q})"
            bid_sizes_str_q = " ".join(str(bs) for bs in bid_sizes)
            bid_sizes_str = f"`float$({bid_sizes_str_q})"
            ask_prices_str_q = " ".join(str(ap) for ap in ask_prices)
            ask_prices_str = f"`float$({ask_prices_str_q})"
            ask_sizes_str_q = " ".join(str(as_) for as_ in ask_sizes)
            ask_sizes_str = f"`float$({ask_sizes_str_q})"
           
            # Construct the full Q update string
            batch_str = f"({timestamps_str}; {symbols_str}; {timestamps_exchange_str}; {exchanges_str}; {bid_prices_str}; {bid_sizes_str}; {ask_prices_str}; {ask_sizes_str})"
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
    #f.add_feed(CryptoDotCom(**common_cfg))
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
