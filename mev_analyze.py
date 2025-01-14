import polars as pl
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import timedelta

DEBUG = False

def bloc_to_day(block_no):
    block_delta = block_no - 114200000
    day_delta = block_delta // 43200
    base_date = datetime(2024, 1, 1)  # Starting reference date
    target_date = base_date + timedelta(days=day_delta)
    return target_date.strftime("%Y-%m-%d")

def plot_p(date_freq):
    df = pl.read_parquet("<PATH_TO_DAILY_ETH_PRICE_DATA>")
    df = df.filter(
        (pl.col("date") >= pl.date(2023, 12, 27))
        & (pl.col("date") <= pl.date(2025, 1, 3))
    )
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df["date"], df["price"], linewidth=2, color="#2E86C1", label="Price Trend")
    scatter_keys = date_freq.keys()
    scatter_s = list(map(lambda y: y * 0.05, date_freq.values()))
    date_price = {}
    scatter_vals = []
    for x in scatter_keys:
        if x not in date_price.keys():
            price = df.filter(
                pl.col("date") == pl.lit(x).str.strptime(pl.Date, "%Y-%m-%d")
            ).row(0)[1]
            date_price[x] = price
        scatter_vals.append(date_price[x])
    ax.scatter(
        scatter_keys,
        scatter_vals,
        color="#E74C3C",
        s=scatter_s,
        alpha=0.6,
        zorder=2,
        label="Data Points",
    )
    ax.set_title(
        "Price: Dec 30, 2023 - Jan 3, 2025", pad=15, fontsize=14, fontweight="bold"
    )
    ax.set_xlabel("Date")
    ax.set_ylabel("Price")
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    plt.xticks(rotation=45)
    ax.grid(True, linestyle="--", alpha=0.7)
    plt.tight_layout()
    plt.show()


def analyze(path: str = "<PATH_TO_RESULT_OF_THE_SPLIT.PY_SCRIPTS_PARQUET>"):
    df = pl.read_parquet(path)
    rows = 50 if DEBUG else df.shape[0]
    profit_tokens = {}
    profit_means = {}
    print(rows)
    date_freq = {}
    for i in range(0, rows):
        num = df["block_number"][i]
        day = bloc_to_day(num)
        if day in date_freq.keys():
            date_freq[day] = date_freq[day] + 1
        else:
            date_freq[day] = 1
        profit_token = df["profit_token"][i]
        profit_amount = df["profit_amount"][i]
        if profit_token in profit_tokens.keys():
            curr_n = profit_tokens[profit_token]
            profit_means[profit_token] = (
                profit_means[profit_token] * curr_n + profit_amount
            ) / (curr_n + 1)
            profit_tokens[profit_token] = 1 + curr_n
        else:
            profit_tokens[profit_token] = 1
            profit_means[profit_token] = profit_amount
    plot_p(date_freq)
    pass
    profit_tokens = {
        k: v for k, v in sorted(profit_tokens.items(), key=lambda item: -item[1])
    }
    newly_gen_pt = {}
    index = 0
    sumr = 0
    for x in profit_tokens:
        if index < 10:
            newly_gen_pt[x] = profit_tokens[x]
        else:
            sumr = sumr + profit_tokens[x]
        index = index + 1
    newly_gen_pt["Rest"] = sumr
    keys = newly_gen_pt.keys()
    newly_gen_pm = {}
    for x in keys:
        if x != "Rest":
            newly_gen_pm[x] = profit_means[x]

    freq_bar = newly_gen_pt.values()
    means_bar = newly_gen_pm.values()
    plt.style.use("dark_background")
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 5))
    fig.patch.set_facecolor("black")
    ax1.bar(keys, freq_bar, color="#7bd400")
    ax1.set_yscale("log")
    ax1.set_title("fq mev tokens", color="white")
    ax1.set_xlabel("tokens", color="white")
    ax1.set_ylabel("log number of mev tx", color="white")
    ax1.set_facecolor("black")
    ax1.tick_params(colors="white")
    ax2.bar(newly_gen_pm.keys(), means_bar, color="#7bd400")
    ax2.set_yscale("log")
    ax2.set_title("mean mev revenue", color="white")
    ax2.set_xlabel("tokens", color="white")
    ax2.set_ylabel("log value", color="white")
    ax2.set_facecolor("black")
    ax2.tick_params(colors="white")

    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    analyze()
