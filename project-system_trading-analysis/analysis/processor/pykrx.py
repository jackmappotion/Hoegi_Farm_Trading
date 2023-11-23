import pandas as pd


class PYKRX_INFO_PREPROCESSOR:
    def __init__(self, pykrx_info_df) -> None:
        self.pykrx_info_df = pykrx_info_df
        self.trader_df = self.preprocessing()

    def preprocessing(self):
        trader_df = self.pykrx_info_df.copy()
        trader_df = self.append_Corp(trader_df)
        trader_df = self.append_Foreign(trader_df)
        trader_df = self.append_Individual(trader_df)
        trader_df["Date"] = pd.to_datetime(trader_df["Date"])
        return trader_df

    @staticmethod
    def append_Corp(trader_df, corp_args=["금융투자", "투신", "사모"]):
        trader_df["Corp"] = trader_df.loc[:, corp_args].sum(axis=1)
        return trader_df

    @staticmethod
    def append_Foreign(trader_df, foreign_args=["외국인"]):
        trader_df["Foreign"] = trader_df.loc[:, foreign_args].sum(axis=1)
        return trader_df

    @staticmethod
    def append_Individual(trader_df, indivisual_args=["개인"]):
        trader_df["Individual"] = trader_df.loc[:, indivisual_args].sum(axis=1)
        return trader_df

    def get_main_trader_df(self, market="KOSPI"):
        trader_df = self.trader_df
        trader_market_df = trader_df[trader_df["Market"] == market].copy()
        main_columns = ["Date", "Corp", "Foreign", "Individual"]
        main_trader_df = trader_market_df.loc[:, main_columns]
        return main_trader_df
