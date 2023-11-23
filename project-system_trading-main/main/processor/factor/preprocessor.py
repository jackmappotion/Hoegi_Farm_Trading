# ohlcv
class OHLCV_PREPROCESSOR:
    @staticmethod
    def filter_zero(ohlcv_df, column):
        filtered_ohlcv_df = ohlcv_df[ohlcv_df[column] != 0].copy()
        return filtered_ohlcv_df

    @staticmethod
    def filter_cnt(ohlcv_df):
        ohlcv_groupby_cnt = ohlcv_df.groupby("StockCode")["Close"].count()
        cnt_mode = ohlcv_groupby_cnt.mode().squeeze()
        filtered_stockcodes = set(ohlcv_groupby_cnt[ohlcv_groupby_cnt == cnt_mode].index)
        filtered_ohlcv_df = ohlcv_df[ohlcv_df["StockCode"].isin(filtered_stockcodes)].copy()
        return filtered_ohlcv_df

    @staticmethod
    def append_VolumeRotation(ohlcv_df, fdr_info_processor):
        ohlcv_df["Share"] = ohlcv_df["StockCode"].map(fdr_info_processor.get_Shares_dict())
        ohlcv_df.dropna(subset=["Share"], inplace=True)
        ohlcv_df["VolumeRotation"] = (ohlcv_df["Volume"] / ohlcv_df["Share"]) * 100
        return ohlcv_df
    
    @staticmethod
    def get_recent_n_mean(ohlcv_df, n):
        recent_n_dates = ohlcv_df["Date"].drop_duplicates().nlargest(n)
        recent_n_ohlcv_df = ohlcv_df[ohlcv_df["Date"].isin(recent_n_dates)]
        main_df = recent_n_ohlcv_df.groupby("StockCode")["Close"].mean().reset_index()
        return main_df