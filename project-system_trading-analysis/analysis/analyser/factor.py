import numpy as np
import pandas as pd


class FACTOR_ANALYSER:
    def __init__(self, factors_df) -> None:
        self.factors_df = factors_df

    def get_analysis_1d_df(self, stockcodes=None):
        factors_df = self.factors_df
        factors = [col for col in factors_df.columns if col != "StockCode"]
        pcts = (
            np.arange(0, 1, 0.1),
            np.arange(0.1, 1.1, 0.1),
        )
        total_dict = dict()
        for factor in factors:
            quantile_dict = self.get_quantile_dict(factors_df, factor, pcts)
            factor_dict = dict()
            for arg, arg_stockcodes in quantile_dict.items():
                if stockcodes:
                    arg_stockcodes = [code for code in arg_stockcodes if code in stockcodes]
                factor_dict[arg.split("_")[-1]] = arg_stockcodes
            total_dict[factor] = factor_dict
        analysis_1d_df = pd.DataFrame(total_dict)
        return analysis_1d_df

    def get_analysis_2d_df(self, factors, stockcodes=None):
        factors_df = self.factors_df
        pcts = (
            np.arange(0, 1, 0.1),
            np.arange(0.1, 1.1, 0.1),
        )
        factor_1 = factors[0]
        factor_2 = factors[1]
        f1_quantile_dict = self.get_quantile_dict(factors_df, factor_1, pcts)
        f2_quantile_dict = self.get_quantile_dict(factors_df, factor_2, pcts)
        total_dict = dict()
        for f1_arg, f1_arg_stockcodes in f1_quantile_dict.items():
            f1_dict = dict()
            for f2_arg, f2_arg_stockcodes in f2_quantile_dict.items():
                filtered_stockcodes = f1_arg_stockcodes & f2_arg_stockcodes
                if stockcodes:
                    filtered_stockcodes = [code for code in filtered_stockcodes if code in stockcodes]
                f1_dict[f2_arg] = filtered_stockcodes
            total_dict[f1_arg] = f1_dict
        analysis_2d_df = pd.DataFrame(total_dict)
        return analysis_2d_df

    def get_cnt_analysis_1d_df(self, stockcodes=None):
        analysis_1d_df = self.get_analysis_1d_df(stockcodes)
        cnt_analysis_1d_df = analysis_1d_df.map(lambda x: len(x))
        return cnt_analysis_1d_df

    def get_profit_analysis_1d_df(self, ohlcv_df, FACTOR_CFG, stockcodes=None):
        analysis_1d_df = self.get_analysis_1d_df(stockcodes)
        recent_ohlcv_df = self.get_recent_ohlcv_df(ohlcv_df, FACTOR_CFG["profit_recent_n"])
        profit_analysis_1d_df = analysis_1d_df.map(
            lambda x: self.get_profit_row(self.get_filtered_ohlcv_df(recent_ohlcv_df, x)).mean()
        )
        return profit_analysis_1d_df

    def get_cnt_analysis_2d_df(self, factors, stockcodes=None):
        analysis_2d_df = self.get_analysis_2d_df(factors, stockcodes)
        cnt_analysis_2d_df = analysis_2d_df.map(lambda x: len(x))
        return cnt_analysis_2d_df

    def get_profit_analysis_2d_df(self, ohlcv_df, FACTOR_CFG, factors, stockcodes=None):
        analysis_2d_df = self.get_analysis_2d_df(factors, stockcodes)
        recent_ohlcv_df = self.get_recent_ohlcv_df(ohlcv_df, FACTOR_CFG["profit_recent_n"])
        profit_analysis_2d_df = analysis_2d_df.map(
            lambda x: self.get_profit_row(self.get_filtered_ohlcv_df(recent_ohlcv_df, x)).mean()
        )
        return profit_analysis_2d_df

    @staticmethod
    def get_filtered_stockcodes(df, factor, lower_pct, upper_pct):
        lower_limit = df[factor].quantile(lower_pct)
        upper_limit = df[factor].quantile(upper_pct)
        filtered_df = df.query(f"({lower_limit} <= {factor}) and ({factor} <= {upper_limit})")
        filtered_stockcodes = set(filtered_df["StockCode"])
        return filtered_stockcodes

    def get_quantile_dict(self, df, factor, pcts):
        quantile_dict = dict()
        for pct in zip(*pcts):
            filtered_stockcodes = self.get_filtered_stockcodes(df, factor, pct[0], pct[1])
            quantile_dict[f"{factor}_{round(pct[0],1)}~{round(pct[1],1)}"] = filtered_stockcodes
        return quantile_dict

    @staticmethod
    def get_filtered_ohlcv_df(ohlcv_df, stockcodes):
        filtered_ohlcv_df = ohlcv_df[ohlcv_df["StockCode"].isin(stockcodes)]
        return filtered_ohlcv_df

    @staticmethod
    def get_recent_ohlcv_df(ohlcv_df, recent_n):
        recent_dates = ohlcv_df["Date"].drop_duplicates().nsmallest(recent_n)
        recent_ohlcv_df = ohlcv_df[ohlcv_df["Date"].isin(recent_dates)]
        return recent_ohlcv_df

    @staticmethod
    def get_profit_row(ohlcv_df):
        def get_profit(price_row):
            price_list = list(price_row)
            profit = (price_list[-1] - price_list[0]) / price_list[0]
            return profit

        profit_row = ohlcv_df.sort_values("Date").groupby("StockCode")["Close"].apply(lambda x: get_profit(x))
        return profit_row
