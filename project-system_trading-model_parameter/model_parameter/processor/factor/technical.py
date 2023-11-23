import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

from .preprocessor import OHLCV_PREPROCESSOR

lr = LinearRegression()


class LINEAR_COEFFICENT_FACTOR_PROCESSOR(OHLCV_PREPROCESSOR):
    def __init__(self, ohlcv_df, fdr_info_processor) -> None:
        self.ohlcv_df = ohlcv_df
        self.fdr_info_processor = fdr_info_processor

    def get_linear_coef_factor_df(self, LINEAR_COEF_CFG):
        ohlcv_df = self.get_pps_ohlcv_df(self.ohlcv_df, self.fdr_info_processor)
        linear_coef_dict = dict()
        for factor in LINEAR_COEF_CFG["factors"]:
            moving_average_df = (
                ohlcv_df.sort_values("Date")
                .set_index("Date")
                .groupby("StockCode")[factor]
                .rolling(window=LINEAR_COEF_CFG["moving_average_window"])
                .mean()
                .reset_index()
            )
            recent_moving_average_df = self.get_recent_n_df(moving_average_df, LINEAR_COEF_CFG["coef_recent_n"])
            coef_dict = (
                recent_moving_average_df.sort_values("Date")
                .groupby("StockCode")[factor]
                .apply(lambda x: self.get_coef(x))
            )
            linear_coef_dict[factor] = coef_dict
        linear_coef_df = pd.DataFrame(linear_coef_dict)
        linear_coef_df.reset_index(names="StockCode", inplace=True)
        return linear_coef_df

    def get_pps_ohlcv_df(self, ohlcv_df, fdr_info_processor):
        pps_ohlcv_df = super().filter_zero(ohlcv_df, "Volume")
        pps_ohlcv_df = super().append_VolumeRotation(pps_ohlcv_df, fdr_info_processor)
        pps_ohlcv_df = super().filter_cnt(pps_ohlcv_df)
        return pps_ohlcv_df

    @staticmethod
    def get_recent_n_df(df, n):
        recent_dates = df["Date"].drop_duplicates().nlargest(n)
        recent_n_df = df[df["Date"].isin(recent_dates)]
        return recent_n_df

    @staticmethod
    def get_coef(array):
        x = np.arange(1, len(array) + 1).reshape(-1, 1)
        y = np.array(array).reshape(-1, 1)
        lr.fit(x, y)
        coef = lr.coef_[0][0]
        scaled_coef = coef / np.abs(np.mean(y))
        return scaled_coef


class MOVING_AVERAGE_FACTOR_PROCESSOR(OHLCV_PREPROCESSOR):
    def __init__(self, ohlcv_df, fdr_info_processor) -> None:
        super().__init__()
        self.ohlcv_df = ohlcv_df
        self.fdr_info_processor = fdr_info_processor

    def get_moving_average_factor_df(self, MOVING_AVERAGE_CFG):
        try:
            moving_average_df = self.moving_average_df
        except:
            moving_average_df = self.get_moving_average_df(MOVING_AVERAGE_CFG)
            self.moving_average_df = moving_average_df

        recent_n_moving_average_df = (
            moving_average_df.sort_values("Date").groupby("StockCode").tail(MOVING_AVERAGE_CFG["signal_recent_n"])
        )
        factors = MOVING_AVERAGE_CFG["factors"]
        factor_signals = [f"{factor}Signal" for factor in factors]
        factor_dict_list = list()
        for factor_signal in factor_signals:
            factor_dict = recent_n_moving_average_df.groupby("StockCode")[factor_signal].sum().to_dict()
            factor_dict_list.append(factor_dict)
        recent_moving_average_df = pd.DataFrame(factor_dict_list).T
        recent_moving_average_df.columns = factors
        recent_moving_average_df.reset_index(names="StockCode", inplace=True)
        return recent_moving_average_df

    def get_moving_average_df(self, MOVING_AVERAGE_CFG):
        moving_average_df = self.get_pps_ohlcv_df(self.ohlcv_df, self.fdr_info_processor)
        for factor in MOVING_AVERAGE_CFG["factors"]:
            moving_average_df = self.append_moving_average(
                moving_average_df, factor, MOVING_AVERAGE_CFG["short_term_window"]
            )
            moving_average_df = self.append_moving_average(
                moving_average_df, factor, MOVING_AVERAGE_CFG["long_term_window"]
            )
            moving_average_df = self.append_signal(
                moving_average_df,
                factor,
                MOVING_AVERAGE_CFG["short_term_window"],
                MOVING_AVERAGE_CFG["long_term_window"],
            )
        return moving_average_df

    def get_pps_ohlcv_df(self, ohlcv_df, fdr_info_processor):
        pps_ohlcv_df = super().filter_zero(ohlcv_df, "Volume")
        pps_ohlcv_df = super().append_VolumeRotation(pps_ohlcv_df, fdr_info_processor)
        pps_ohlcv_df = super().filter_cnt(pps_ohlcv_df)
        return pps_ohlcv_df

    @staticmethod
    def append_moving_average(df, column, window):
        moving_average_row = (
            df.set_index("Date").sort_index().groupby("StockCode")[column].rolling(window=window).mean()
        )
        moving_average_row.name = f"{column}_{window}"
        appended_df = pd.merge(df, moving_average_row, on=["StockCode", "Date"])
        return appended_df

    @staticmethod
    def append_signal(df, column, st_window, lt_window):
        df[f"{column}Signal"] = (df[f"{column}_{st_window}"] - df[f"{column}_{lt_window}"]) / df[
            f"{column}_{st_window}"
        ]
        return df
