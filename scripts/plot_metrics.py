#!/usr/bin/env python3
"""Post-processing plots for the metrics_log.txt capture.

    ./scripts/plot_metrics.py [results/24_hour_capture.txt] 

Produces the three plots the report asks for:

    jitter.png          Jitter of the periodic thread vs time.
    load_buffer.png     Message rate (Hz) and Buffer occupancy (%)
                          vs time, to show network busyness.
    cpu_load.png        Message rate (Hz) vs CPU busy (%).
"""
import argparse
import os
import sys
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

COLUMNS = ["sec", "nsec", "commit", "identity", "account", "info", "buf_pct", "cpu_pct"]


def load(path):
    df = pd.read_csv(path, header=None, names=COLUMNS)
    df["time"] = df["sec"].apply(datetime.fromtimestamp)
    df["rate"] = df["commit"] + df["identity"] + df["account"] + df["info"]

    step = df["sec"].diff()
    clean = step.isna() | (step == 1)
    ns = df["nsec"]
    df["jitter_ms"] = (ns / 1e6).where(ns < 5e8, (ns - 1e9) / 1e6)
    df.loc[~clean, "jitter_ms"] = float("nan")  # stall-flush row, not real jitter

    return df


def plot_jitter(df, out):
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(df["time"], df["jitter_ms"], linewidth=0.5, color="tab:blue")
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("Monitor thread jitter vs ideal 1 Hz deadline")
    ax.set_xlabel("Local time")
    ax.set_ylabel("Jitter (ms)")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.grid(True, linewidth=0.3, alpha=0.6)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)


def plot_load_buffer(df, out):
    fig, ax1 = plt.subplots(figsize=(12, 4))
    ax1.plot(df["time"], df["rate"], linewidth=0.5, color="tab:blue", label="Message rate (Hz)")
    ax1.set_xlabel("Local time")
    ax1.set_ylabel("Message rate (Hz)", color="tab:blue")
    ax1.tick_params(axis="y", labelcolor="tab:blue")
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))

    ax2 = ax1.twinx()
    ax2.plot(df["time"], df["buf_pct"], linewidth=0.6, color="tab:orange", label="Buffer occupancy (%)")
    ax2.set_ylabel("Buffer occupancy (%)", color="tab:orange")
    ax2.tick_params(axis="y", labelcolor="tab:orange")

    ax1.set_title("Message rate and ring-buffer occupancy vs time")
    ax1.grid(True, linewidth=0.3, alpha=0.6)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)


def plot_cpu(df, out):
    fig, ax = plt.subplots(figsize=(7, 6))
    ax.scatter(df["rate"], df["cpu_pct"], s=4, alpha=0.25, color="tab:red")
    ax.set_title("CPU load vs incoming message rate")
    ax.set_xlabel("Message rate (Hz)")
    ax.set_ylabel("CPU busy (%)   [idle = 100 - this]")
    ax.grid(True, linewidth=0.3, alpha=0.6)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("input", nargs="?", default="results/24_hour_capture.txt")
    ap.add_argument("-o", "--outdir", default="results/plots")
    args = ap.parse_args()

    if not os.path.exists(args.input):
        sys.exit(f"{args.input}: not found")
    os.makedirs(args.outdir, exist_ok=True)

    df = load(args.input)

    plot_jitter(df, os.path.join(args.outdir, "jitter.png"))
    plot_load_buffer(df, os.path.join(args.outdir, "load_buffer.png"))
    plot_cpu(df, os.path.join(args.outdir, "cpu_load.png"))

    print(f"wrote 3 plots to {args.outdir}/ from {len(df)} rows "
          f"({datetime.fromtimestamp(df['sec'].iloc[0])} .. {datetime.fromtimestamp(df['sec'].iloc[-1])})")


if __name__ == "__main__":
    main()
