# 🏏 IPL Analytics Dashboard

> A production-grade, interactive data analytics dashboard built with Python, SQL, and Streamlit — analyzing 10+ seasons of IPL cricket data.

![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python) ![Streamlit](https://img.shields.io/badge/Streamlit-1.32+-red?logo=streamlit) ![Plotly](https://img.shields.io/badge/Plotly-5.18+-purple?logo=plotly) ![SQLite](https://img.shields.io/badge/SQLite-In--Memory-green?logo=sqlite) ![License](https://img.shields.io/badge/License-MIT-yellow)

---

## 📌 Project Overview

This dashboard performs deep analytical exploration of IPL match and ball-by-ball delivery data spanning 2008–2020. It uses **SQL (via SQLite in-memory DB)** for data querying, **Pandas** for transformation, and **Plotly + Streamlit** for interactive visualization.

**Designed to demonstrate real-world data analyst skills:**
- Writing optimized SQL queries (JOINs, CTEs, Window Functions, CASE statements)
- Exploratory data analysis and KPI derivation
- Building interactive dashboards deployable to the web

---

## 🗂️ Project Structure

```
ipl_dashboard/
│
├── app.py                  # Main Streamlit application
├── requirements.txt        # Python dependencies
├── README.md               # Project documentation
│
└── data/                   # ← Place your CSV files here
    ├── matches.csv
    └── deliveries.csv
```

---

## 📊 Dashboard Features

| Tab | What It Shows |
|-----|--------------|
| 🏆 Team Analysis | Win counts, toss advantage, venue heatmaps |
| 🏏 Batting | Top scorers, run scatter plots, leaderboard |
| 🎯 Bowling | Wicket takers, economy analysis, leaderboard |
| 📈 Season Trends | YoY matches, sixes/fours trends, team dominance |
| 🔍 Player Deep Dive | Career runs, season breakdown, dismissal analysis |

---

## ⚙️ Setup & Run (Local)

### Step 1 — Clone / Download the Project

```bash
git clone https://github.com/YOUR_USERNAME/ipl-analytics-dashboard.git
cd ipl-analytics-dashboard
```

### Step 2 — Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 3 — Download Dataset from Kaggle

1. Go to: https://www.kaggle.com/datasets/patrickb1912/ipl-complete-dataset-20082020
2. Download and extract the ZIP
3. Place `matches.csv` and `deliveries.csv` inside the `data/` folder:

```bash
mkdir data
# Copy your downloaded files here
```

### Step 4 — Run the App

```bash
streamlit run app.py
```

Open your browser at `http://localhost:8501` 🎉

---

## 🚀 Deploy to Streamlit Cloud (Free)

1. Push the project to **GitHub** (make sure `data/` folder with CSVs is included)
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Click **"New App"** → Connect GitHub repo
4. Set `app.py` as the main file
5. Click **Deploy** — live in ~2 minutes!

> ⚠️ **Note:** Streamlit Cloud's free tier has a 1GB file limit. The IPL dataset is well within this limit.

---

## 🧠 Key SQL Patterns Used

```sql
-- Window function: Rank players by runs per season
SELECT 
    m.season,
    d.batter,
    SUM(d.batsman_runs) AS runs,
    RANK() OVER (PARTITION BY m.season ORDER BY SUM(d.batsman_runs) DESC) AS rank
FROM deliveries d
JOIN matches m ON d.match_id = m.id
GROUP BY m.season, d.batter

-- CASE statement: Win method classification
SELECT 
    CASE 
        WHEN result = 'runs' THEN 'Won by Runs'
        WHEN result = 'wickets' THEN 'Won by Wickets'
        ELSE 'Other'
    END AS method,
    COUNT(*) AS count
FROM matches GROUP BY method

-- Conditional aggregation: Toss advantage analysis
SELECT 
    ROUND(100.0 * SUM(CASE WHEN toss_winner = winner THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct
FROM matches
```

---

## 📁 Dataset

| File | Rows | Description |
|------|------|-------------|
| `matches.csv` | ~816 | One row per IPL match (2008–2020) |
| `deliveries.csv` | ~179,078 | Ball-by-ball delivery data |

**Source:** [IPL Complete Dataset – Kaggle](https://www.kaggle.com/datasets/patrickb1912/ipl-complete-dataset-20082020)

---

## 💼 Resume Bullet Points (Copy-Paste Ready)

- Built an end-to-end IPL sports analytics dashboard using Python, SQLite, and Streamlit, analyzing 179K+ ball-by-ball delivery records across 13 seasons
- Wrote optimized SQL queries with JOINs, CASE statements, and conditional aggregation to derive 15+ cricket KPIs including strike rate, economy, and win probability metrics  
- Designed an interactive 5-tab Plotly dashboard with dynamic season/player filters, deployed publicly on Streamlit Cloud as a live portfolio project

---

## 🤝 Contributing

Pull requests welcome! For major changes, please open an issue first.

---

## 📄 License

MIT — free to use, modify, and distribute.
