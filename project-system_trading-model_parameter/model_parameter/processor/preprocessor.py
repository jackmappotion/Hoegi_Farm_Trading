# Preprocessor


class PREPROCESSOR:
    @staticmethod
    def format_stockcode(df):
        df["StockCode"] = df["StockCode"].apply(lambda x: str(x).zfill(6))
        return df

    @staticmethod
    def set_index_stockcode(df):
        df.set_index("StockCode", inplace=True)
        return df