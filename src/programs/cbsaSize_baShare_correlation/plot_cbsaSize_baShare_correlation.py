import pandas as pd

from utils.correlation_plots import two_variable_scatter_plot_with_adjusted_regression_line

size_baShare_corr = pd.read_csv('size_baShare_corr.csv')
size_baShare_corr = size_baShare_corr.sort_values(by=['PERWT'], ascending=False).reset_index(drop=True)
down = 0
for top in [10, 50, 100, 304]:
    x = size_baShare_corr.loc[down:top, 'PERWT']
    y = size_baShare_corr.loc[down:top, 'ba']
    two_variable_scatter_plot_with_adjusted_regression_line(x, y, f'baShare-cbsaSize. Top {down}-{top}')
    down = top
