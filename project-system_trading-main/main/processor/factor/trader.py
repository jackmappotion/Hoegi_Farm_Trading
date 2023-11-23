import pandas as pd


class TRADER_FACTOR_PROCESSOR:
    def __init__(self, pykrx_loader) -> None:
        self.pykrx_loader = pykrx_loader

    def get_trader_factor_df(self, stockcodes, TRADER_CFG):
        trader_factor_dict = dict()
        for stockcode in stockcodes:
            trader_df = self.load_trader_df(stockcode, TRADER_CFG["start"], TRADER_CFG["end"])
            pps_trader_df = self.get_pps_trader_df(trader_df)
            weighted_dict = self.get_weighted_dict(pps_trader_df)
            trader_factor_dict[stockcode] = weighted_dict
        trader_factor_df = pd.DataFrame(trader_factor_dict).T.reset_index(names="StockCode")
        return trader_factor_df

    def load_trader_df(self, stockcode, start, end):
        trader_df = self.pykrx_loader.get_stock_trader_df(StockCode=stockcode, start=start, end=end)
        return trader_df

    @staticmethod
    def get_pps_trader_df(trader_df):
        format_dict = {
            "Corp": ["금융투자", "보험", "투신", "사모"],
            "Indivisual": ["개인"],
            "Foreign": ["외국인"],
        }
        for key, value in format_dict.items():
            trader_df[key] = trader_df.loc[:, value].sum(axis=1)
        pps_trader_df = trader_df.loc[:, list(format_dict.keys())]
        return pps_trader_df

    @staticmethod
    def get_weighted_dict(pps_trader_df):
        weighted_trader_df = pps_trader_df.reset_index(drop=True).multiply(
            pd.Series(range(1, len(pps_trader_df) + 1)), axis=0
        )
        weighted_dict = weighted_trader_df.mean(axis=0).to_dict()
        return weighted_dict
