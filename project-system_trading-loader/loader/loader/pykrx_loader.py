# pykrx_loader
from pykrx import stock as PYKRX_BROKER


class PYKRX_LOADER:
    def __init__(self, PYKRX_BROKER) -> None:
        self.pykrx_broker = PYKRX_BROKER

    def get_stock_fundamental_df(self, StockCode, start, end, freq="d"):
        """
        종목의 fundamental 지표를 제공한다.
        """
        stock_fundamental_df = self.pykrx_broker.get_market_fundamental(
            fromdate=start, todate=end, ticker=StockCode, freq=freq
        )
        return stock_fundamental_df

    def get_stock_trader_df(self, StockCode, start, end, detail=True):
        """
        종목의 거래자 현황을 제공한다.
        """
        stock_trader_df = self.pykrx_broker.get_market_trading_value_by_date(
            fromdate=start, todate=end, ticker=StockCode, detail=detail
        )
        return stock_trader_df

    def get_market_fundamental_df(self, date):
        """
        시장의 fundamental 지표를 제공한다.
        """
        market_fundamental_df = self.pykrx_broker.get_market_fundamental_by_ticker(
            date=date, market="ALL", alternative=True
        )
        return market_fundamental_df

    def get_market_short_df(self, date):
        """
        시장의 공매도 현황을 제공한다.
        """
        market_short_df = self.pykrx_broker.get_shorting_volume_by_ticker(date)
        return market_short_df


def get_pykrx_loader():
    pykrx_loader = PYKRX_LOADER(PYKRX_BROKER)
    return pykrx_loader
