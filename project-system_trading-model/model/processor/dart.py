from .preprocessor import PREPROCESSOR


# Dart fundamental
class DART_FUNDAMENTAL_PREPROCESSOR(PREPROCESSOR):
    def __init__(self, dart_fundamental_df) -> None:
        self.dart_fundamental_df = dart_fundamental_df

    def get_pps_dart_fundamental_df(self):
        df = self.dart_fundamental_df.copy()
        pps_df = super().format_stockcode(df)
        pps_df = super().set_index_stockcode(pps_df)
        pps_df = self.filter_fsname(pps_df, fsname="재무제표")
        pps_df = self.preprocess_amount(pps_df)
        return pps_df

    @staticmethod
    def filter_fsname(df, fsname):
        fsname_filtered_df = df[df["FSName"] == fsname].copy()
        return fsname_filtered_df

    @staticmethod
    def preprocess_amount(df):
        def _preprocess_amount(amount):
            if amount == "-":
                return None
            else:
                amount = int(amount.replace(",", ""))
                return amount

        df["Amount"] = df["Amount"].apply(lambda x: _preprocess_amount(x))
        df = df[~(df["Amount"].isna())]
        return df


class DART_FUNDAMENTAL_PROCESSOR(DART_FUNDAMENTAL_PREPROCESSOR):
    def __init__(self, dart_fundamental_df) -> None:
        super().__init__(dart_fundamental_df)
        self.dart_fundamental_df = super().get_pps_dart_fundamental_df()

    def get_NetProfit_dict(self):
        """
        당기순이익
        """
        df = self.dart_fundamental_df
        NetProfit_df = df[df["AccountName"] == "당기순이익"]
        NetProfit_dict = NetProfit_df["Amount"].to_dict()
        return NetProfit_dict

    def get_OperationProfit_dict(self):
        """
        영업이익
        """
        df = self.dart_fundamental_df
        OperationProfit_df = df[df["AccountName"] == "영업이익"]
        OperationProfit_dict = OperationProfit_df["Amount"].to_dict()
        return OperationProfit_dict

    def get_TotalAssets_dict(self):
        """
        자산총계
        """
        df = self.dart_fundamental_df
        TotalAssets_df = df[df["AccountName"] == "자산총계"]
        TotalAssets_dict = TotalAssets_df["Amount"].to_dict()
        return TotalAssets_dict

    def get_TotalLiabilities_dict(self):
        """
        부채총계
        """
        df = self.dart_fundamental_df
        TotalLiabilities_df = df[df["AccountName"] == "부채총계"]
        TotalLiabilities_dict = TotalLiabilities_df["Amount"].to_dict()
        return TotalLiabilities_dict

    def get_CurrentAssets_dict(self):
        """
        유동자산
        """
        df = self.dart_fundamental_df
        CurrentAssets_df = df[df["AccountName"] == "유동자산"]
        CurrentAssets_dict = CurrentAssets_df["Amount"].to_dict()
        return CurrentAssets_dict

    def get_CurrentLiabilities_dict(self):
        """
        유동부채
        """
        df = self.dart_fundamental_df
        CurrentLiabilities_df = df[df["AccountName"] == "유동부채"]
        CurrentLiabilities_dict = CurrentLiabilities_df["Amount"].to_dict()
        return CurrentLiabilities_dict


# Dart info
class DART_INFO_PREPROCESSOR(PREPROCESSOR):
    def __init__(self, dart_info_df) -> None:
        self.dart_info_df = dart_info_df

    def get_pps_dart_info_df(self):
        df = self.dart_info_df
        columns = ["StockCode", "Sector", "Product"]
        df = self.drop_na(df, columns)

        df["StockCode"] = df["StockCode"].astype(int)

        pps_df = super().format_stockcode(df)
        pps_df = super().set_index_stockcode(pps_df)
        return pps_df

    @staticmethod
    def drop_na(df, columns):
        filtered_index = df.loc[:, columns].dropna().index
        filtered_df = df.loc[filtered_index, :]
        return filtered_df


class DART_INFO_PROCESSOR(DART_INFO_PREPROCESSOR):
    def __init__(self, dart_info_df) -> None:
        super().__init__(dart_info_df)
        self.dart_info_df = super().get_pps_dart_info_df()

    def get_StockName_dict(self):
        """
        StockName
        """
        df = self.dart_info_df
        StockName_dict = df["StockName"].to_dict()
        return StockName_dict

    def get_Sector_dict(self):
        """
        Sector
        """
        df = self.dart_info_df
        Sector_dict = df["Sector"].to_dict()
        return Sector_dict

    def get_Product_dict(self):
        """
        Product
        """
        df = self.dart_info_df
        Product_dict = df["Product"].to_dict()
        return Product_dict
