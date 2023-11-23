import numpy as np
import pandas as pd

from .preprocessor import OHLCV_PREPROCESSOR


class FUNDAMENTAL_FACTOR_PROCESSOR(OHLCV_PREPROCESSOR):
    def __init__(self, ohlcv_df, dart_fundamental_processor, fdr_info_processor) -> None:
        self.ohlcv_df = ohlcv_df
        self.dart_fundamental_processor = dart_fundamental_processor
        self.fdr_info_processor = fdr_info_processor

    def get_fundamental_factor_df(self, FUNDAMENTAL_CFG):
        recent_n = FUNDAMENTAL_CFG["ohlcv_recent_n"]
        main_df = super().get_recent_n_mean(self.ohlcv_df, recent_n)
        main_df = self.append_dart_fundamental(main_df, self.dart_fundamental_processor)
        main_df = self.append_fdr_info(main_df, self.fdr_info_processor)
        factor_df = self._get_factor_df(main_df)
        factor_df.dropna(inplace=True)
        return factor_df

    @staticmethod
    def append_dart_fundamental(df, dart_fundamental_processor):
        # profit
        df["NetProfit"] = df["StockCode"].map(dart_fundamental_processor.get_NetProfit_dict())
        df["OperationProfit"] = df["StockCode"].map(dart_fundamental_processor.get_OperationProfit_dict())

        # current
        df["CurrentAssets"] = df["StockCode"].map(dart_fundamental_processor.get_CurrentAssets_dict())
        df["CurrentLiabilities"] = df["StockCode"].map(dart_fundamental_processor.get_CurrentLiabilities_dict())

        # total
        df["TotalAssets"] = df["StockCode"].map(dart_fundamental_processor.get_TotalAssets_dict())
        df["TotalLiabilities"] = df["StockCode"].map(dart_fundamental_processor.get_TotalLiabilities_dict())
        return df

    @staticmethod
    def append_fdr_info(df, fdr_info_processor):
        # shares
        df["Shares"] = df["StockCode"].map(fdr_info_processor.get_Shares_dict())
        return df

    @staticmethod
    def _get_factor_df(main_df):
        factor_df = main_df.loc[:, ["StockCode"]].copy()
        # my Factors
        # Current Liabilities Ratio
        factor_df["CLR"] = main_df["CurrentLiabilities"] / main_df["CurrentAssets"]
        # Total Liabilities Ratio
        factor_df["TLR"] = main_df["TotalLiabilities"] / main_df["TotalAssets"]

        # Netprofit Per Price
        factor_df["NPP"] = (main_df["Close"] * main_df["Shares"]) / main_df["NetProfit"]
        # OperationProfit Per Price
        factor_df["OPP"] = (main_df["Close"] * main_df["Shares"]) / main_df["OperationProfit"]

        # TotalAssets Per Price
        factor_df["TAPP"] = (main_df["Close"] * main_df["Shares"]) / main_df["TotalAssets"]
        # TotalEquity Per Price
        factor_df["TEPP"] = (main_df["Close"] * main_df["Shares"]) / (
            main_df["TotalAssets"] - main_df["TotalLiabilities"]
        )
        # CurrentAssets Per Price
        factor_df["CAPP"] = (main_df["Close"] * main_df["Shares"]) / main_df["CurrentAssets"]
        # CurrentEquity Per Price
        factor_df["CEPP"] = (main_df["Close"] * main_df["Shares"]) / (
            main_df["CurrentAssets"] - main_df["CurrentLiabilities"]
        )
        return factor_df
