import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def plot_graph(df, path):
    sns.set_theme(style="whitegrid")
    sns.set_context("paper", font_scale=1.5)
    plt = sns.catplot(x="query", y="latency", hue="format", data=df, errwidth=1, capsize=0.1, errorbar="sd", kind="bar", palette="muted")
    plt.set(xlabel="Query", ylabel="Duration (ms)")
    plt.savefig(path)


if __name__ == "__main__":
    # Load data
    with open('paper/w_o_opt/flight', 'r') as f:
        data_flight = f.readlines()
        data_flight = [float(x.strip()) for x in data_flight]

    with open('paper/w_o_opt/thallium-optimized', 'r') as f:
        data_thallium_opt = f.readlines()
        data_thallium_opt = [float(x.strip()) for x in data_thallium_opt]

    with open('paper/w_o_opt/thallium-nonoptimized', 'r') as f:
        data_thallium_noopt = f.readlines()
        data_thallium_noopt = [float(x.strip()) for x in data_thallium_noopt]

    queries_large = [1, 2]
    queries_medium = [3, 4, 5]
    queries_small = [6, 7, 8]

    # Plot Queries Large
    table = {
        "query": [],
        "format": [],
        "latency": [],
    }

    for q in queries_large:
        for i in range(0, 10):
            table["query"].append(q)
            table["format"].append("flight")
            table["latency"].append(data_flight[10*(q-1)+i])

            table["query"].append(q)
            table["format"].append("thallium-no-opt")
            table["latency"].append(data_thallium_noopt[10*(q-1)+i])

            table["query"].append(q)
            table["format"].append("thallium-opt")
            table["latency"].append(data_thallium_opt[10*(q-1)+i])

    df = pd.DataFrame(table)
    print(df)
    plot_graph(df, "paper/w_o_opt/plot_large.pdf")

    # Plot Queries Medium
    table = {
        "query": [],
        "format": [],
        "latency": [],
    }

    for q in queries_medium:
        for i in range(0, 10):
            table["query"].append(q)
            table["format"].append("flight")
            table["latency"].append(data_flight[10*(q-1)+i])

            table["query"].append(q)
            table["format"].append("thallium-no-opt")
            table["latency"].append(data_thallium_noopt[10*(q-1)+i])

            table["query"].append(q)
            table["format"].append("thallium-opt")
            table["latency"].append(data_thallium_opt[10*(q-1)+i])

    df = pd.DataFrame(table)
    print(df)
    plot_graph(df, "paper/w_o_opt/plot_medium.pdf")

    # Plot Queries Small
    table = {
        "query": [],
        "format": [],
        "latency": [],
    }

    for q in queries_small:
        for i in range(0, 10):
            table["query"].append(q)
            table["format"].append("flight")
            table["latency"].append(data_flight[10*(q-1)+i])

            table["query"].append(q)
            table["format"].append("thallium-no-opt")
            table["latency"].append(data_thallium_noopt[10*(q-1)+i])

            table["query"].append(q)
            table["format"].append("thallium-opt")
            table["latency"].append(data_thallium_opt[10*(q-1)+i])

    df = pd.DataFrame(table)
    print(df)
    plot_graph(df, "paper/w_o_opt/plot_small.pdf")
