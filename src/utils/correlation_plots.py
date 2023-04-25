import numpy as np
from matplotlib import pyplot as plt


def two_variable_scatter_plot_with_adjusted_regression_line(x, y, title, labels=False, annotate=False, df=False):
    slope, intercept = np.polyfit(x, y, 1)
    line = slope * x + intercept
    colors = np.full(len(x), 'b')  # Set all points to blue
    if labels:
        highlight = []
        for i, label in enumerate(df['cbsa']):
            if label in labels:
                highlight.append(i)
        colors[highlight] = 'y'  # Set specific points to yellow
        if annotate:
            for e in highlight:
                plt.annotate(list(df['cbsa'])[e].split("-")[0], (x[e], y[e]), textcoords="offset points",
                             xytext=(0, 10), ha='center', fontsize=7)
    alpha = 0.5
    plt.scatter(x, y, c=colors, alpha=[alpha if color == 'b' else 1.0 for color in colors])
    plt.plot(x, line, color='red')
    plt.title(title)
    plt.savefig(f'{title}.png')
    plt.show()
