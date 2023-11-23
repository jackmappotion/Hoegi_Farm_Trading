import pandas as pd
from .private import KI_PRIVATE
from .koreainvestment_broker import KoreaInvestment


class KI_BROKER:
    def __init__(self, ki_private) -> None:
        self.ki_private = ki_private

    def __call__(self):
        ki_broker = KoreaInvestment(
            api_key=self.ki_private.get_app_key(),
            api_secret=self.ki_private.get_app_secret(),
            acc_no=self.ki_private.get_acc_number(),
        )
        return ki_broker


def get_ki_broker():
    ki_broker = KI_BROKER(KI_PRIVATE)()
    return ki_broker


class ORDER_CONTROLLER:
    def __init__(self, ki_broker) -> None:
        self.ki_broker = ki_broker

    def order_preprocessor(self):
        order_preprocessor = self.ORDER_PREPROCESSOR(self.ki_broker)
        return order_preprocessor

    def order_requester(self):
        order_requester = self.ORDER_REQUESTER(self.ki_broker)
        return order_requester

    class ORDER_PREPROCESSOR:
        def __init__(self, ki_broker) -> None:
            self.ki_broker = ki_broker

        def get_buying_df(self, stockcodes):
            current_cash = self._get_current_cash()
            invest_cash = current_cash // 2
            buying_df = (
                pd.DataFrame()
                .from_dict(self._get_price_dict(stockcodes), orient="index", columns=["Price"])
                .reset_index(names="StockCode")
            )
            stock_current_cash = int(invest_cash) / len(buying_df)
            buying_df["Quantity"] = stock_current_cash // buying_df["Price"].astype(int)
            buying_df = buying_df[buying_df["Quantity"] > 0]
            buying_df.loc[:, ["Quantity"]] = buying_df["Quantity"].apply(lambda x: str(int(x)))
            buying_df = buying_df.loc[:, ["StockCode", "Quantity", "Price"]]
            return buying_df

        def get_position_df(self):
            balance_resp = self._load_balance_resp()
            raw_position_df = self._get_position_df(balance_resp)
            position_df = self._format_position_df(raw_position_df)
            return position_df

        def _get_current_price(self, stockcode):
            resp = self.ki_broker.fetch_price(stockcode)["output"]
            current_price = int(resp["stck_prpr"])
            return current_price

        def _get_price_dict(self, stockcodes):
            price_dict = dict()
            for stockcode in stockcodes:
                price_dict[stockcode] = self._get_current_price(stockcode)
            return price_dict

        def _get_current_cash(self):
            balance_resp = self.ki_broker.fetch_balance()
            current_cash = int(balance_resp["output2"][0]["prvs_rcdl_excc_amt"])
            return current_cash

        # position_df
        def _load_balance_resp(self):
            balance_resp = self.ki_broker.fetch_balance()
            return balance_resp

        # position_df
        @staticmethod
        def _get_position_df(balance_resp):
            position_df = pd.DataFrame(balance_resp["output1"])
            return position_df

        # position_df
        @staticmethod
        def _format_position_df(position_df):
            rename_dict = {
                "pdno": "StockCode",
                "prdt_name": "StockName",
                "pchs_amt": "BuyingTotalPrice",
                "evlu_amt": "CurrentTotalPrice",
                "hldg_qty": "Quantity",
            }
            retype_dict = {
                "BuyingTotalPrice": int,
                "CurrentTotalPrice": int,
                "Quantity": int,
            }

            pps_position_df = position_df.rename(columns=rename_dict)
            pps_position_df = pps_position_df.astype(retype_dict)
            pps_position_df = pps_position_df[pps_position_df["Quantity"] > 0]

            pps_position_df["BuyingPrice"] = (pps_position_df["BuyingTotalPrice"] / pps_position_df["Quantity"]).astype(
                int
            )
            pps_position_df["CurrentPrice"] = (
                pps_position_df["CurrentTotalPrice"] / pps_position_df["Quantity"]
            ).astype(int)
            pps_position_df = pps_position_df.loc[
                :, ["StockCode", "StockName", "BuyingPrice", "CurrentPrice", "Quantity"]
            ]
            return pps_position_df

    class ORDER_REQUESTER:
        def __init__(self, ki_broker) -> None:
            self.ki_broker = ki_broker

        def request_market_buy_order(self, stockcode, quantity):
            """
            시장가 매수
            """
            order_resp = self.ki_broker.create_order(
                side="buy",
                symbol=stockcode,
                price="0",
                quantity=quantity,
                order_type="01",
            )
            return order_resp

        def request_market_sell_order(self, stockcode, quantity):
            """
            시장가 매도
            """
            order_resp = self.ki_broker.create_order(
                side="sell",
                symbol=stockcode,
                price="0",
                quantity=quantity,
                order_type="01",
            )
            return order_resp

        def request_limit_sell_order_resv(self, stockcode, price, quantity, resv_date):
            """
            매도 예약
            """
            order_resv_resp = self.ki_broker.create_order_resv(
                side="sell",
                symbol=stockcode,
                price=price,
                quantity=quantity,
                order_type="02",
                resv_date=resv_date,
            )
            return order_resv_resp

        def request_market_sell_order_resv(self, stockcode, quantity, resv_date):
            order_resv_resp = self.ki_broker.create_order_resv(
                side="sell",
                symbol=stockcode,
                quantity=quantity,
                price="0",
                order_type="01",
                resv_date=resv_date,
            )
            return order_resv_resp

        def request_market_buy_order_resv(self, stockcode, quantity, resv_date):
            order_resv_resp = self.ki_broker.create_order_resv(
                side="sell",
                symbol=stockcode,
                quantity=quantity,
                price="0",
                order_type="01",
                resv_date=resv_date,
            )
            return order_resv_resp
