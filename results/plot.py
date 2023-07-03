import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def plot_graph(df, path):
    sns.set_theme(style="whitegrid")
    sns.set_context("paper", font_scale=1.5)
    plt = sns.catplot(x="query", y="latency", hue="format", data=df, errwidth=1, capsize=0.1, errorbar="sd", kind="bar", palette="muted")
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

    queries_large = ["1/20.76GB", "2/2.26GB"]
    queries_medium = ["3/240MB", "4/23.4MB", "5/2.34MB"]
    queries_small = ["6/220KB", "7/51.2KB", "8/8B"]

    # Plot Queries Large
    table = {
        "query": [],
        "format": [],
        "latency": [],
    }

    for q in queries_large:
        a, b = q.split("/")
        for i in range(0, 10):
            table["query"].append(q)
            table["format"].append("flight-acero")
            table["latency"].append(data_flight_acero[10*(int(a)-1)+i])

            table["query"].append(q)
            table["format"].append("flight-duckdb")
            table["latency"].append(data_flight_duckdb[10*(int(a)-1)+i])

            table["query"].append(q)
            table["format"].append("thallium-duckdb")
            table["latency"].append(data_thallium_duckdb[10*(int(a)-1)+i])

    df = pd.DataFrame(table)
    print(df)
    plot_graph(df, "paper/final/plot_large.pdf")

    # Plot Queries Medium
    table = {
        "query": [],
        "format": [],
        "latency": [],
    }

    for q in queries_medium:
        a, b = q.split("/")
        for i in range(0, 10):
            table["query"].append(q)
            table["format"].append("flight-acero")
            table["latency"].append(data_flight_acero[10*(int(a)-1)+i])

            table["query"].append(q)
            table["format"].append("flight-duckdb")
            table["latency"].append(data_flight_duckdb[10*(int(a)-1)+i])

            table["query"].append(q)
            table["format"].append("thallium-duckdb")
            table["latency"].append(data_thallium_duckdb[10*(int(a)-1)+i])

    df = pd.DataFrame(table)
    print(df)
    plot_graph(df, "paper/final/plot_medium.pdf")

    # Plot Queries Small
    table = {
        "query": [],
        "format": [],
        "latency": [],
    }

    for q in queries_small:
        a, b = q.split("/")
        for i in range(0, 10):
            table["query"].append(q)
            table["format"].append("flight-acero")
            table["latency"].append(data_flight_acero[10*(int(a)-1)+i])

            table["query"].append(q)
            table["format"].append("flight-duckdb")
            table["latency"].append(data_flight_duckdb[10*(int(a)-1)+i])

            table["query"].append(q)
            table["format"].append("thallium-duckdb")
            table["latency"].append(data_thallium_duckdb[10*(int(a)-1)+i])

    df = pd.DataFrame(table)
    print(df)
    plot_graph(df, "paper/final/plot_small.pdf")
