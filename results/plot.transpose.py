import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def plot_graph(df, path):
    sns.set_theme(style="whitegrid")
    sns.set_context("paper", font_scale=1.5)
    plt = sns.catplot(x="format", y="latency", hue="query", data=df, errwidth=1, capsize=0.1, errorbar="sd", kind="bar", palette="muted")
    plt.despine(left=False, bottom=False, top=False, right=False)
    plt.set(xlabel="Query No.", ylabel="Duration (ms)")
    plt.savefig(path)


if __name__ == "__main__":
    # Load data
    with open('paper/final/flight-acero', 'r') as f:
        data_flight_acero = f.readlines()
        data_flight_acero = [float(x.strip()) for x in data_flight_acero]

    with open('paper/final/flight-duckdb', 'r') as f:
        data_flight_duckdb = f.readlines()
        data_flight_duckdb = [float(x.strip()) for x in data_flight_duckdb]

    with open('paper/final/thallium-duckdb', 'r') as f:
        data_thallium_duckdb = f.readlines()
        data_thallium_duckdb = [float(x.strip()) for x in data_thallium_duckdb]

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
            table["format"].append("flight-acero")
            table["latency"].append(data_flight_acero[10*(q-1)+i])

            table["query"].append(q)
            table["format"].append("flight-duckdb")
            table["latency"].append(data_flight_duckdb[10*(q-1)+i])

            table["query"].append(q)
            table["format"].append("thallium-duckdb")
            table["latency"].append(data_thallium_duckdb[10*(q-1)+i])

    df = pd.DataFrame(table)
    print(df)
    plot_graph(df, "paper/final/plot_large.transpose.pdf")

    # Plot Queries Medium
    table = {
        "query": [],
        "format": [],
        "latency": [],
    }

    for q in queries_medium:
        for i in range(0, 10):
            table["query"].append(q)
            table["format"].append("flight-acero")
            table["latency"].append(data_flight_acero[10*(q-1)+i])

            table["query"].append(q)
            table["format"].append("flight-duckdb")
            table["latency"].append(data_flight_duckdb[10*(q-1)+i])

            table["query"].append(q)
            table["format"].append("thallium-duckdb")
            table["latency"].append(data_thallium_duckdb[10*(q-1)+i])

    df = pd.DataFrame(table)
    print(df)
    plot_graph(df, "paper/final/plot_medium.transpose.pdf")

    # Plot Queries Small
    table = {
        "query": [],
        "format": [],
        "latency": [],
    }

    for q in queries_small:
        for i in range(0, 10):
            table["query"].append(q)
            table["format"].append("flight-acero")
            table["latency"].append(data_flight_acero[10*(q-1)+i])

            table["query"].append(q)
            table["format"].append("flight-duckdb")
            table["latency"].append(data_flight_duckdb[10*(q-1)+i])

            table["query"].append(q)
            table["format"].append("thallium-duckdb")
            table["latency"].append(data_thallium_duckdb[10*(q-1)+i])

    df = pd.DataFrame(table)
    print(df)
    plot_graph(df, "paper/final/plot_small.transpose.pdf")
