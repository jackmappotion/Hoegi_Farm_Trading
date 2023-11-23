from functools import reduce


class FACTOR_FILTER:
    def __init__(self, factor_df) -> None:
        self.factor_df = factor_df

    def get_filtered_df(self, FILTER_CFG, stockcodes=None):
        df = self.factor_df.copy()
        try:
            conditions = FILTER_CFG["quantile_conditions"]
            filtered_df = self._get_quantile_filtered_df(df, conditions)
        except:
            conditions = FILTER_CFG["absolute_conditions"]
            filtered_df = self._get_absolute_filtered_df(df, conditions)
        if stockcodes:
            filtered_df = filtered_df[filtered_df["StockCode"].isin(stockcodes)]
        return filtered_df

    def get_filtered_stockcodes(self, FILTER_CFG, stockcodes=None):
        filtered_df = self.get_filtered_df(FILTER_CFG, stockcodes)
        filtered_stockcodes = set(filtered_df["StockCode"])
        if stockcodes:
            filtered_stockcodes = filtered_stockcodes & stockcodes
        return filtered_stockcodes

    def _get_quantile_filtered_df(self, df, quantile_conditions):
        combined_condition = self._get_combined_condition(df, quantile_conditions)
        filtered_df = df.loc[combined_condition, :]
        return filtered_df

    def _get_absolute_filtered_df(self, df, absolute_conditions):
        query = reduce(lambda x, y: " and ".join((x, y)), absolute_conditions)
        filtered_df = df.query(query)
        return filtered_df

    def _get_combined_condition(self, df, conditions):
        quantile_conditions = [
            self._filter_by_quantile(df, condition[0], condition[1], condition[2]) for condition in conditions
        ]
        combined_condition = reduce(lambda x, y: x & y, quantile_conditions)
        return combined_condition

    @staticmethod
    def _filter_by_quantile(df, column, low_quantile, high_quantile):
        return (df[column] > df[column].quantile(low_quantile)) & (df[column] < df[column].quantile(high_quantile))
