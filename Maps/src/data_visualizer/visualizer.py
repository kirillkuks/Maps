from matplotlib import pyplot as plt
from numpy import arange, floor

import pandas as pd
import seaborn as sns



class Visualizer:
    def __init__(self, output_dir: str) -> None:
        self._output_dir = output_dir
        self._color = 'green'

    def set_plot_color(self, color: str) -> None:
        self._color = color

    def plot_bar(self, data: list, labels: list, name: str, plot_labels: str) -> None:
        fig, ax = plt.subplots()
        width = 0.75
        ind = arange(len(data))
        ax.barh(ind, data, 0.75, color=self._color)
        ax.set_yticks(ind + width / 2)
        ax.set_yticklabels(['' for _ in range(len(data))], minor=False)

        print(labels)

        for bar, label, dist in zip(ax.patches, labels, data):
            ax.text(0.1, bar.get_y() + bar.get_height() / 2, f'{label}, {round(dist, 2)}', color='black', ha='left', va='center')        

        plt.title(plot_labels[0])
        plt.xlabel(plot_labels[1])
        plt.ylabel(plot_labels[2])

        plt.margins(0, 0.05)
        plt.savefig(f'{self._output_dir}/{name}.png', dpi=300)
        plt.close()

    def plot_hist(self, data: list, name: str, additional_info: str, plot_labels: str) -> None:
        df = pd.DataFrame(data=data)

        quant_5, quant_25, quant_50, quant_75, quant_95 = df.quantile(0.05).item(), \
            df.quantile(0.25).item(), \
            df.quantile(0.50).item(), \
            df.quantile(0.75).item(), \
            df.quantile(0.95).item()

        quants = [
            [quant_5, 0.6, 0.16],
            [quant_25, 0.8, 0.26],
            [quant_50, 1.0, 0.36],
            [quant_75, 0.8, 0.46],
            [quant_95, 0.6, 0.56]
        ]

        fig, ax = plt.subplots(figsize=(6, 4))
        plt.style.use('bmh')
        ax.set_xlim([0, max(data)])

        ax.hist(data, density=True, bins=int(floor(1.72 * (len(data) ** (1.0 / 3.0)))), alpha=0.50, edgecolor='black')
        sns.kdeplot(data, bw_method=0.5, color='red')

        y_max = ax.get_ylim()[1]

        texts = ['5th', '25th', '50th', '75th', '95th']

        for q, text in zip(quants, texts):
            ax.axvline(q[0], alpha=q[1], ymax=q[2], linestyle=':')
            ax.text(q[0], (q[2] + 0.01) * y_max, text, color='black', ha='left', va='center')

        ax.grid(False)

        plt.title(plot_labels[0])
        plt.xlabel(plot_labels[1])
        plt.ylabel(plot_labels[2])

        plt.savefig(f'{self._output_dir}/{name}_{additional_info}.png', dpi=300)
        plt.close()

    def plot_donut(self, data: list, labels: list, name: str, plot_labels: list) -> None:
        plt.gcf().set_size_inches((9, 9))
        plt.pie(data, labels=labels, autopct='%1.1f%%', pctdistance=0.80, explode=[0.05 for _ in range(len(data))])

        circle = plt.Circle((0, 0), 0.70, fc='white')
        plt.gcf().gca().add_artist(circle)

        plt.title(plot_labels[0])

        plt.savefig(f'{self._output_dir}/{name}.png')
        
        plt.close()
