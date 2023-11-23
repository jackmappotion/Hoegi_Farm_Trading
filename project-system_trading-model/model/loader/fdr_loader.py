import pandas as pd

import FinanceDataReader as FDR_BROKER


class FDR_LOADER:
    def __init__(self, FDR_BROKER) -> None:
        self.fdr_broker = FDR_BROKER

    def get_krx_df(self):
        krx_df = self.fdr_broker.StockListing("KRX")
        return krx_df

    def get_stock_ohlcv_df(self, StockCode, start, end):
        stock_ohlcv_df = self.fdr_broker.DataReader(symbol=StockCode, start=start, end=end)
        stock_ohlcv_df = stock_ohlcv_df.loc[:, ["Open", "High", "Low", "Close", "Volume"]]
        stock_ohlcv_df["StockCode"] = StockCode
        return stock_ohlcv_df

    def get_stocks_ohlcv_df(self, StockCodes, start, end):
        stock_ohlcv_df_list = list()
        for StockCode in StockCodes:
            try:
                stock_ohlcv_df = self.get_stock_ohlcv_df(StockCode, start, end)
                stock_ohlcv_df_list.append(stock_ohlcv_df)
            except:
                pass
        stocks_ohlcv_df = pd.concat(stock_ohlcv_df_list, axis=0).reset_index()
        return stocks_ohlcv_df


def get_fdr_loader():
    fdr_loader = FDR_LOADER(FDR_BROKER)
    return fdr_loader
