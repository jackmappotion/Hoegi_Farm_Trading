import numpy as np


class FACTOR_PARAMETER_PROCESSOR:
    def __init__(self, analysis_2d_df, n=3, m=3) -> None:
        self.analysis_2d_df = analysis_2d_df
        self.average_array = self.get_average_array(analysis_2d_df, n, m)
        self.n = n
        self.m = m

    def get_average_array(self, analysis_2d_df, n, m):
        windows = np.lib.stride_tricks.sliding_window_view(analysis_2d_df.to_numpy(), (n, m))
        average_array = np.mean(windows, axis=(2, 3))
        return average_array

    def get_best_args(self):
        analysis_2d_df = self.analysis_2d_df
        average_array = self.average_array

        max_arg = np.unravel_index(average_array.argmax(), average_array.shape)
        factor_1 = list(analysis_2d_df.columns[max_arg[0] : max_arg[0] + self.n])
        factor_1_key = factor_1[0].split("_")[0]
        factor_1_lower_pct = factor_1[0].split("_")[-1].split("~")[0]
        factor_1_upper_pct = factor_1[-1].split("_")[-1].split("~")[-1]

        factor_2 = list(analysis_2d_df.index[max_arg[1] : max_arg[1] + self.m])
        factor_2_key = factor_2[0].split("_")[0]
        factor_2_lower_pct = factor_2[0].split("_")[-1].split("~")[0]
        factor_2_upper_pct = factor_2[-1].split("_")[-1].split("~")[-1]

        best_args = (
            (factor_1_key, float(factor_1_lower_pct), float(factor_1_upper_pct)),
            (factor_2_key, float(factor_2_lower_pct), float(factor_2_upper_pct)),
        )
        return best_args

    def get_best_value(self):
        best_value = np.max(self.average_array)
        return best_value
