import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


if __name__ == "__main__":
    # Load data
    with open('results/131072/flight-duckdb-2-8-nopt', 'r') as f:
        data_duckdb = f.readlines()
        data_duckdb = [float(x.strip()) for x in data_duckdb]

    with open('results/131072/thallium-duckdb-2-8-nopt', 'r') as f:
        data_thallium = f.readlines()
        data_thallium = [float(x.strip()) for x in data_thallium]

    queries_large = [1, 2, 3]
    queries_small = [4, 5, 6, 7, 8]

    # Plot Queries Large
    table = {
        "query": [],
        "format": [],
        "latency": [],
    }

    for q in queries_large:
        for i in range(0, 10):
            table["query"].append(q)
            table["format"].append("duckdb")
            table["latency"].append(data_duckdb[10*(q-1)+i])

        for i in range(0, 10):
            table["query"].append(q)
            table["format"].append("thallium")
            table["latency"].append(data_thallium[10*(q-1)+i])

    df = pd.DataFrame(table)
    print(df)

    sns.set_theme(style="whitegrid")
    sns.set_context("paper", font_scale=1.5)
    plt = sns.catplot(x="query", y="latency", hue="format", data=df, kind="bar", palette="muted")
    plt.set(xlabel="Query", ylabel="Latency (ms)")
    plt.savefig("results/131072/plot_large.pdf")

    # Plot Queries Small
    table = {
        "query": [],
        "format": [],
        "latency": [],
    }

    for q in queries_small:
        for i in range(0, 10):
            table["query"].append(q)
            table["format"].append("duckdb")
            table["latency"].append(data_duckdb[10*(q-1)+i])

        for i in range(0, 10):
            table["query"].append(q)
            table["format"].append("thallium")
            table["latency"].append(data_thallium[10*(q-1)+i])

    df = pd.DataFrame(table)
    print(df)

    sns.set_theme(style="whitegrid")
    sns.set_context("paper", font_scale=1.5)
    plt = sns.catplot(x="query", y="latency", hue="format", data=df, kind="bar", palette="muted")
    plt.set(xlabel="Query", ylabel="Latency (ms)")
    plt.savefig("results/131072/plot_small.pdf")