trade:([]
	time: `float$();
	sym:`$();
	timeExch: `float$();
	exchange: `$();
	side:`$();
	amount:`float$();
	price:`float$()
	)

quote:([]
	time: `float$();
	sym: `$();
	timeExch: `float$();
	exchange: `$();
	bestBid: `float$();
	bestBidSize: `float$();
	bestAsk: `float$();
	bestAskSize: `float$();
	midprice: `float$();
	bidAskSpread: `float$();
	marketDepthBids: `float$();
	marketDepthAsks: `float$();
	orderBookImbalance: `float$();
	vwap: `float$();
	orderBookRatio: `float$();
	bidSlippagePrice: `float$();
	askSlippagePrice: `float$()
	)