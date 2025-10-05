import matplotlib.pyplot as plt


class Visualizer:
    def __init__(self, figsize=(12, 8)):
        self.fig = plt.figure(figsize=figsize)
        self.subplots = []

    def add_subplot(self, height_ratio=1.0):
        self.subplots.append({"height_ratio": height_ratio, "plotters": []})
        return self

    def add_plotter(self, subplot_index, plotter):
        self.subplots[subplot_index]["plotters"].append(plotter)
        return self

    def draw(self, data):
        n = len(self.subplots)

        height_ratios = [sp["height_ratio"] for sp in self.subplots]
        gs = self.fig.add_gridspec(n, 1, height_ratios=height_ratios, hspace=0.05)

        axes = []
        for i, sp in enumerate(self.subplots):
            ax = self.fig.add_subplot(gs[i, 0])
            axes.append(ax)
            for plotter in sp["plotters"]:
                plotter(ax, data)

        return self.fig
