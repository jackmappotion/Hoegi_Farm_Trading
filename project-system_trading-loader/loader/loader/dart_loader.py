import requests
import pandas as pd

import dart_fss as DART_FSS_BROKER

from .private import DART_PRIVATE


class DART_API_LOADER:
    def __init__(self, dart_private) -> None:
        self.dart_private = dart_private

    def get_corp_fundamental_df(self, corp_code, year, reprt_code):
        api_key = self.dart_private.get_api_key()
        url = self.get_corp_fundamental_url(api_key=api_key, corp_code=corp_code, year=year, reprt_code=reprt_code)
        resp = requests.get(url=url)
        corp_fundamental_df = pd.DataFrame(resp.json()["list"])
        return corp_fundamental_df

    def get_corps_fundamental_df(self, corp_codes, year, reprt_code):
        api_key = self.dart_private.get_api_key()
        corp_codes_zips = self.get_chunked_list(corp_codes, 99)

        _corps_fundamental_df_list = list()
        for corp_codes_zip in corp_codes_zips:
            try:
                url = self.get_corps_fundamental_url(
                    api_key=api_key, corp_codes=corp_codes_zip, year=year, reprt_code=reprt_code
                )
                resp = requests.get(url=url)
                _corps_fundamental_df = pd.DataFrame(resp.json()["list"])
                _corps_fundamental_df_list.append(_corps_fundamental_df)
            except:
                pass
        corps_fundamental_df = pd.concat(_corps_fundamental_df_list, axis=0)
        return corps_fundamental_df

    @staticmethod
    def get_corp_fundamental_url(api_key, corp_code, year, reprt_code):
        """
        1분기보고서 : 11013
        반기보고서 : 11012
        3분기보고서 : 11014
        사업보고서 : 11011
        """
        url = (
            f"https://opendart.fss.or.kr/api/fnlttSinglAcnt.json?"
            f"crtfc_key={api_key}&"
            f"corp_code={corp_code}&"
            f"bsns_year={year}&"
            f"reprt_code={reprt_code}"
        )
        return url

    @staticmethod
    def get_corps_fundamental_url(api_key, corp_codes, year, reprt_code):
        """
        1분기보고서 : 11013
        반기보고서 : 11012
        3분기보고서 : 11014
        사업보고서 : 11011
        """
        url = (
            f"https://opendart.fss.or.kr/api/fnlttMultiAcnt.json?"
            f"crtfc_key={api_key}&"
            f"corp_code={','.join(corp_codes)}&"
            f"bsns_year={year}&"
            f"reprt_code={reprt_code}"
        )
        return url

    @staticmethod
    def get_chunked_list(_list, n):
        chunked_list = [_list[i : i + n] for i in range(0, len(_list), n)]
        return chunked_list


def get_dart_api_loader():
    dart_api_loader = DART_API_LOADER(DART_PRIVATE)
    return dart_api_loader


class DART_FSS_LOADER:
    def __init__(self, DART_FSS_BROKER, DART_PRIVATE) -> None:
        DART_FSS_BROKER.set_api_key(DART_PRIVATE.get_api_key())
        self.dart_fss_broker = DART_FSS_BROKER

    def get_info_df(self):
        corp_list = self.dart_fss_broker.get_corp_list()
        corp_info_df = pd.DataFrame([corp.to_dict() for corp in corp_list._corps])
        return corp_info_df


def get_dart_fss_loader():
    dart_fss_loader = DART_FSS_LOADER(DART_FSS_BROKER, DART_PRIVATE)
    return dart_fss_loader