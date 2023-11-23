from .preprocessor import PREPROCESSOR


# krx_info
class KRX_INFO_PREPROCESSOR(PREPROCESSOR):
    def __init__(self, krx_info_df) -> None:
        self.krx_info_df = krx_info_df

    def get_pps_krx_info_df(self):
        df = self.krx_info_df
        pps_df = super().format_stockcode(df)
        pps_df = super().set_index_stockcode(pps_df)
        pps_df = self.append_MarketSector(pps_df)
        return pps_df

    @staticmethod
    def append_MarketSector(df):
        df["MarketSector"] = df["MarketName"].apply(lambda x: x + "_") + df["SectorName"]
        return df


class KRX_INFO_PROCESSOR(KRX_INFO_PREPROCESSOR):
    def __init__(self, krx_info_df) -> None:
        super().__init__(krx_info_df)
        self.krx_info_df = super().get_pps_krx_info_df()

    def get_MarketSector_dict(self):
        df = self.krx_info_df
        MarketSector_dict = df["MarketSector"].to_dict()
        return MarketSector_dict
