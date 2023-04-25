import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def _plot_2d_scatterplot(x, y, title, labels):
    # Calculate the regression line
    slope, intercept = np.polyfit(x, y, 1)
    line = slope * x + intercept
    # Highlight some observations
    highlight = []
    for i, label in enumerate(shares['cbsa']):
        if label in labels:
            highlight.append(i)
    colors = np.full(len(x), 'b')  # Set all points to blue
    colors[highlight] = 'y'  # Set specific points to red
    alpha = 0.5
    plt.scatter(x, y, c=colors, alpha=[alpha if color == 'b' else 1.0 for color in colors])
    # Plot the regression line
    plt.plot(x, line, color='red')
    # for e in highlight:
    #     plt.annotate(list(shares['cbsa'])[e].split("-")[0], (x[e], y[e]), textcoords="offset points", xytext=(0, 10),
    #     ha='center', fontsize=7)
    # Set the x-axis label
    plt.title(title)
    plt.savefig(f'{title}.png')
    plt.show()


def _create_ranking_of_demeaned_variables(df, variables):
    demeaned_vars = []
    for var in variables:
        mean = df[var].mean()
        std = df[var].std()
        demeaned_vars.append([(x - mean) / std for x in df[var]])
    return [demeaned_vars[0][i] + demeaned_vars[1][i] for i in range(len(demeaned_vars[0]))]


shares = pd.read_csv('entrep_share_by_cbsa.csv')[:302].dropna().reset_index(drop=True)
shares['ranking'] = _create_ranking_of_demeaned_variables(shares, ['count_ba', 'share_ba'])
high_size_bashare_cities = shares.sort_values(by=['ranking'], ascending=False)[:30]['cbsa'].tolist()
_plot_2d_scatterplot(shares['share_SELF'], shares['share_INC'], 'shareInc-shareSelf', high_size_bashare_cities)
_plot_2d_scatterplot(shares['share_SELF'], shares['share_entrep'], 'shareLinkedInEntrep-shareSelf', high_size_bashare_cities)
# _plot_2d_scatterplot(shares['share_INC'], shares['share_entrep'], 'shareEntrep-shareInc', high_size_bashare_cities)
# _plot_2d_scatterplot(shares['count_SELF'], shares['share_SELF'], 'shareSelf-countSelf', high_size_bashare_cities)
# _plot_2d_scatterplot(shares['count_entrep'], shares['share_entrep'], 'shareEntrep-countEntrep', high_size_bashare_cities)




# Save the plot as a PNG file
