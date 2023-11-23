import pandas as pd
from .preprocessor import PREPROCESSOR


# fdr info
class FDR_INFO_PREPROCESSOR(PREPROCESSOR):
    def __init__(self, fdr_info_df) -> None:
        self.fdr_info_df = fdr_info_df

    def get_pps_fdr_info_df(self):
        df = self.fdr_info_df
        pps_df = super().format_stockcode(df)
        pps_df = super().set_index_stockcode(pps_df)
        return pps_df


class FDR_INFO_PROCESSOR(FDR_INFO_PREPROCESSOR):
    def __init__(self, fdr_info_df) -> None:
        super().__init__(fdr_info_df)
        self.fdr_info_df = super().get_pps_fdr_info_df()

    def get_StockName_dict(self):
        """
        StockName
        """
        df = self.fdr_info_df
        StockName_dict = df["StockName"].to_dict()
        return StockName_dict

    def get_Market_dict(self):
        """
        Market
        """
        df = self.fdr_info_df
        Market_dict = df["Market"].to_dict()
        return Market_dict

    def get_Shares_dict(self):
        """
        Shares
        """
        df = self.fdr_info_df
        Shares_dict = df["Shares"].to_dict()
        return Shares_dict


# fdr_ohlcv
class FDR_OHLCV_PREPROCESSOR(PREPROCESSOR):
    def __init__(self, fdr_ohlcv_df) -> None:
        self.fdr_ohlcv_df = fdr_ohlcv_df

    def get_pps_fdr_ohlcv_df(self):
        df = self.fdr_ohlcv_df
        pps_df = super().format_stockcode(df)
        pps_df = self.format_date(pps_df, "Date")
        return pps_df

    @staticmethod
    def format_date(df, column="Date"):
        df[column] = pd.to_datetime(df[column])
        return df
