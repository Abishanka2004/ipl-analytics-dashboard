import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlite3
import os

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="IPL Analytics Dashboard",
    page_icon="🏏",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─────────────────────────────────────────────
# CUSTOM CSS
# ─────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@400;600;700&family=Inter:wght@300;400;500&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    .main { background-color: #0a0e1a; }
    .stApp { background: linear-gradient(135deg, #0a0e1a 0%, #0d1b2a 50%, #0a0e1a 100%); }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0d1b2a, #111827);
        border-right: 1px solid #1e3a5f;
    }

    /* Metric Cards */
    div[data-testid="metric-container"] {
        background: linear-gradient(135deg, #0d1b2a, #1a2744);
        border: 1px solid #1e3a5f;
        border-radius: 12px;
        padding: 16px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.4);
    }
    div[data-testid="metric-container"] label {
        color: #64b5f6 !important;
        font-family: 'Rajdhani', sans-serif !important;
        font-size: 13px !important;
        letter-spacing: 1.5px !important;
        text-transform: uppercase !important;
    }
    div[data-testid="metric-container"] div[data-testid="stMetricValue"] {
        color: #ffffff !important;
        font-family: 'Rajdhani', sans-serif !important;
        font-size: 28px !important;
        font-weight: 700 !important;
    }

    /* Headers */
    h1, h2, h3 { font-family: 'Rajdhani', sans-serif !important; color: #e8f4fd !important; }
    h1 { font-size: 2.4rem !important; letter-spacing: 2px; }

    /* Section Dividers */
    .section-header {
        font-family: 'Rajdhani', sans-serif;
        font-size: 1.4rem;
        font-weight: 700;
        color: #64b5f6;
        letter-spacing: 2px;
        text-transform: uppercase;
        border-left: 4px solid #f97316;
        padding-left: 12px;
        margin: 24px 0 16px 0;
    }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        background: #0d1b2a;
        border-radius: 10px;
        gap: 4px;
        padding: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        background: transparent;
        color: #94a3b8;
        font-family: 'Rajdhani', sans-serif;
        font-weight: 600;
        letter-spacing: 1px;
        border-radius: 8px;
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #1e40af, #1d4ed8) !important;
        color: white !important;
    }

    /* Selectbox */
    .stSelectbox > div > div {
        background: #0d1b2a;
        border: 1px solid #1e3a5f;
        color: white;
        border-radius: 8px;
    }

    /* Dataframe */
    .stDataFrame { border: 1px solid #1e3a5f; border-radius: 10px; }

    .hero-title {
        font-family: 'Rajdhani', sans-serif;
        font-size: 3rem;
        font-weight: 700;
        background: linear-gradient(90deg, #f97316, #fbbf24, #f97316);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        letter-spacing: 3px;
    }
    .hero-sub {
        color: #64b5f6;
        font-family: 'Inter', sans-serif;
        font-size: 0.95rem;
        letter-spacing: 1px;
    }
    .badge {
        display: inline-block;
        background: linear-gradient(135deg, #1e3a5f, #1e40af);
        color: #93c5fd;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.78rem;
        font-family: 'Rajdhani', sans-serif;
        letter-spacing: 1px;
        margin: 2px;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────
DB_PATH = "data/ipl_analytics.db"
RAW_MATCHES    = "data/matches.csv"
RAW_DELIVERIES = "data/deliveries.csv"

@st.cache_data
def load_data():
    """
    Prefer pre-built SQLite DB (from pipeline.py).
    Falls back to raw CSVs if DB not found.
    """
    if os.path.exists(DB_PATH):
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        matches    = pd.read_sql("SELECT * FROM matches",    conn)
        deliveries = pd.read_sql("SELECT * FROM deliveries", conn)
        conn.close()
        matches['date'] = pd.to_datetime(matches['date'], errors='coerce')
        return matches, deliveries, True   # True = loaded from pipeline DB

    # Fallback: raw CSVs
    if not os.path.exists(RAW_MATCHES) or not os.path.exists(RAW_DELIVERIES):
        st.error("❌ Dataset not found! Run `python pipeline.py` first, or place CSVs in `data/`.")
        st.info("📥 https://www.kaggle.com/datasets/patrickb1912/ipl-complete-dataset-20082020")
        st.stop()

    matches    = pd.read_csv(RAW_MATCHES)
    deliveries = pd.read_csv(RAW_DELIVERIES)
    matches['date']   = pd.to_datetime(matches['date'], dayfirst=True, errors='coerce')
    matches['season'] = matches['date'].dt.year
    matches.dropna(subset=['winner'], inplace=True)
    return matches, deliveries, False  # False = loaded from raw CSV


def build_sqlite(matches, deliveries):
    """Build in-memory SQLite from dataframes (fallback path)."""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    matches.to_sql("matches",    conn, if_exists="replace", index=False)
    deliveries.to_sql("deliveries", conn, if_exists="replace", index=False)
    return conn


def get_db_conn():
    """
    Return a SQLite connection.
    Uses the persistent DB if pipeline has been run, else in-memory.
    """
    if os.path.exists(DB_PATH):
        return sqlite3.connect(DB_PATH, check_same_thread=False)
    return build_sqlite(*load_data()[:2])


# ─────────────────────────────────────────────
# ANALYTICS FUNCTIONS (SQL-powered)
# ─────────────────────────────────────────────
def get_team_wins(conn, season_filter=None):
    season_clause = f"WHERE season = {season_filter}" if season_filter else ""
    query = f"""
        SELECT winner AS team, COUNT(*) AS wins
        FROM matches {season_clause}
        GROUP BY winner
        ORDER BY wins DESC
    """
    return pd.read_sql(query, conn)


def get_toss_win_advantage(conn):
    query = """
        SELECT 
            COUNT(*) AS total_matches,
            SUM(CASE WHEN toss_winner = winner THEN 1 ELSE 0 END) AS toss_then_won,
            ROUND(100.0 * SUM(CASE WHEN toss_winner = winner THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct
        FROM matches
    """
    return pd.read_sql(query, conn)


def get_top_batsmen(conn, season_filter=None, top_n=10):
    season_clause = "WHERE m.season = " + str(season_filter) if season_filter else ""
    query = f"""
        SELECT 
            d.batter AS player,
            SUM(d.batsman_runs) AS runs,
            COUNT(DISTINCT d.match_id) AS matches,
            MAX(d.batsman_runs) AS highest_score,
            ROUND(AVG(d.batsman_runs), 2) AS avg_per_delivery
        FROM deliveries d
        JOIN matches m ON d.match_id = m.id
        {season_clause}
        GROUP BY d.batter
        ORDER BY runs DESC
        LIMIT {top_n}
    """
    return pd.read_sql(query, conn)


def get_top_bowlers(conn, season_filter=None, top_n=10):
    season_clause = "WHERE m.season = " + str(season_filter) if season_filter else ""
    query = f"""
        SELECT 
            d.bowler AS player,
            COUNT(CASE WHEN d.dismissal_kind IS NOT NULL 
                  AND d.dismissal_kind NOT IN ('run out','retired hurt','obstructing the field') 
                  THEN 1 END) AS wickets,
            COUNT(DISTINCT d.match_id) AS matches,
            ROUND(SUM(d.total_runs) * 6.0 / NULLIF(COUNT(*),0), 2) AS economy
        FROM deliveries d
        JOIN matches m ON d.match_id = m.id
        {season_clause}
        GROUP BY d.bowler
        ORDER BY wickets DESC
        LIMIT {top_n}
    """
    return pd.read_sql(query, conn)


def get_season_summary(conn):
    query = """
        SELECT 
            season,
            COUNT(*) AS matches_played,
            COUNT(DISTINCT city) AS cities,
            COUNT(DISTINCT team1) AS teams
        FROM matches
        GROUP BY season
        ORDER BY season
    """
    return pd.read_sql(query, conn)


def get_venue_stats(conn, top_n=10):
    query = f"""
        SELECT venue, COUNT(*) AS matches
        FROM matches
        GROUP BY venue
        ORDER BY matches DESC
        LIMIT {top_n}
    """
    return pd.read_sql(query, conn)


def get_player_season_runs(conn, player):
    query = f"""
        SELECT m.season, SUM(d.batsman_runs) AS runs
        FROM deliveries d
        JOIN matches m ON d.match_id = m.id
        WHERE d.batter = '{player}'
        GROUP BY m.season
        ORDER BY m.season
    """
    return pd.read_sql(query, conn)


def get_win_by_method(conn):
    query = """
        SELECT 
            CASE 
                WHEN result = 'runs' THEN 'Won by Runs'
                WHEN result = 'wickets' THEN 'Won by Wickets'
                WHEN result = 'tie' THEN 'Tie'
                ELSE 'Other'
            END AS method,
            COUNT(*) AS count
        FROM matches
        GROUP BY method
    """
    return pd.read_sql(query, conn)


def get_6s_4s_by_season(conn):
    query = """
        SELECT m.season,
               SUM(CASE WHEN d.batsman_runs = 6 THEN 1 ELSE 0 END) AS sixes,
               SUM(CASE WHEN d.batsman_runs = 4 THEN 1 ELSE 0 END) AS fours
        FROM deliveries d
        JOIN matches m ON d.match_id = m.id
        GROUP BY m.season
        ORDER BY m.season
    """
    return pd.read_sql(query, conn)


# ─────────────────────────────────────────────
# PLOTLY THEME
# ─────────────────────────────────────────────
PLOTLY_THEME = dict(
    paper_bgcolor="#0a0e1a",
    plot_bgcolor="#0d1b2a",
    font=dict(family="Rajdhani, Inter, sans-serif", color="#94a3b8", size=12),
    title_font=dict(family="Rajdhani", color="#e8f4fd", size=16),
    xaxis=dict(gridcolor="#1e3a5f", linecolor="#1e3a5f", tickfont=dict(color="#94a3b8")),
    yaxis=dict(gridcolor="#1e3a5f", linecolor="#1e3a5f", tickfont=dict(color="#94a3b8")),
    margin=dict(l=20, r=20, t=50, b=20),
    colorway=["#f97316", "#3b82f6", "#10b981", "#a855f7", "#fbbf24", "#06b6d4", "#ef4444", "#84cc16"]
)

IPL_COLORS = {
    "Mumbai Indians": "#004BA0",
    "Chennai Super Kings": "#FDB913",
    "Royal Challengers Bangalore": "#EC1C24",
    "Kolkata Knight Riders": "#3A225D",
    "Delhi Capitals": "#0078BC",
    "Sunrisers Hyderabad": "#F7A721",
    "Rajasthan Royals": "#2D5DA1",
    "Punjab Kings": "#ED1B24",
    "default": "#f97316"
}


# ─────────────────────────────────────────────
# MAIN APP
# ─────────────────────────────────────────────
matches, deliveries, from_pipeline = load_data()
conn = get_db_conn()

all_seasons = sorted(matches['season'].dropna().unique().astype(int).tolist())
all_teams = sorted(matches['team1'].unique().tolist())

# ── SIDEBAR ──────────────────────────────────
with st.sidebar:
    st.markdown('<p class="hero-title" style="font-size:1.8rem;">🏏 IPL</p>', unsafe_allow_html=True)
    st.markdown('<p class="hero-sub">Analytics Dashboard</p>', unsafe_allow_html=True)
    st.markdown("---")

    st.markdown("### 🎛️ Filters")
    season_options = ["All Seasons"] + [str(s) for s in all_seasons]
    selected_season = st.selectbox("📅 Season", season_options)
    season_filter = int(selected_season) if selected_season != "All Seasons" else None

    st.markdown("---")
    st.markdown("### 📊 Dataset Info")
    st.markdown(f'<span class="badge">🏟️ {len(matches):,} Matches</span>', unsafe_allow_html=True)
    st.markdown(f'<span class="badge">🎯 {len(deliveries):,} Deliveries</span>', unsafe_allow_html=True)
    st.markdown(f'<span class="badge">📅 {all_seasons[0]}–{all_seasons[-1]}</span>', unsafe_allow_html=True)
    st.markdown(f'<span class="badge">🏏 {len(all_teams)} Teams</span>', unsafe_allow_html=True)

    st.markdown("---")
    if from_pipeline:
        st.markdown('<span class="badge" style="background:linear-gradient(135deg,#064e3b,#065f46);color:#6ee7b7;">✅ ETL Pipeline DB</span>', unsafe_allow_html=True)
    else:
        st.markdown('<span class="badge" style="background:linear-gradient(135deg,#7c2d12,#9a3412);color:#fdba74;">⚠️ Raw CSV Mode<br><small>Run pipeline.py</small></span>', unsafe_allow_html=True)
    st.markdown("---")
    st.markdown('<p style="color:#475569;font-size:0.75rem;">Built with Python · SQL · Streamlit · Plotly</p>', unsafe_allow_html=True)


# ── HERO HEADER ──────────────────────────────
st.markdown('<p class="hero-title">IPL ANALYTICS</p>', unsafe_allow_html=True)
st.markdown(f'<p class="hero-sub">Deep-dive into {all_seasons[0]}–{all_seasons[-1]} | Season: {selected_season}</p>', unsafe_allow_html=True)
st.markdown("---")

# ── KPI METRICS ──────────────────────────────
filtered_matches = matches[matches['season'] == season_filter] if season_filter else matches
filtered_del = deliveries[deliveries['match_id'].isin(filtered_matches['id'])] if season_filter else deliveries

total_matches = len(filtered_matches)
total_runs = filtered_del['total_runs'].sum()
total_sixes = (filtered_del['batsman_runs'] == 6).sum()
total_fours = (filtered_del['batsman_runs'] == 4).sum()

col1, col2, col3, col4 = st.columns(4)
col1.metric("🏟️ Matches Played", f"{total_matches:,}")
col2.metric("🏏 Total Runs Scored", f"{total_runs:,}")
col3.metric("💥 Sixes Hit", f"{total_sixes:,}")
col4.metric("🔥 Fours Hit", f"{total_fours:,}")

st.markdown("<br>", unsafe_allow_html=True)

# ── TABS ─────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🏆 Team Analysis",
    "🏏 Batting",
    "🎯 Bowling",
    "📈 Season Trends",
    "🔍 Player Deep Dive"
])


# ═══════════════════════════════════════════
# TAB 1: TEAM ANALYSIS
# ═══════════════════════════════════════════
with tab1:
    st.markdown('<p class="section-header">Team Win Analysis</p>', unsafe_allow_html=True)

    team_wins = get_team_wins(conn, season_filter)

    col1, col2 = st.columns([3, 2])

    with col1:
        colors = [IPL_COLORS.get(t, IPL_COLORS["default"]) for t in team_wins['team']]
        fig = go.Figure(go.Bar(
            x=team_wins['wins'],
            y=team_wins['team'],
            orientation='h',
            marker=dict(color=colors, line=dict(color='rgba(255,255,255,0.1)', width=1)),
            text=team_wins['wins'],
            textposition='outside',
            textfont=dict(color='white', family='Rajdhani', size=13)
        ))
        fig.update_layout(
            title="Wins by Team",
            **PLOTLY_THEME,
            height=420,

            showlegend=False
        )
        fig.update_yaxes(autorange="reversed")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Toss advantage
        toss_data = get_toss_win_advantage(conn)
        toss_pct = float(toss_data['pct'].iloc[0])

        fig2 = go.Figure(go.Pie(
            labels=["Won Toss + Match", "Won Toss, Lost Match"],
            values=[toss_pct, 100 - toss_pct],
            hole=0.65,
            marker=dict(colors=["#f97316", "#1e3a5f"]),
            textfont=dict(family="Rajdhani", size=13)
        ))
        fig2.update_layout(
            title=f"Toss → Win Advantage ({toss_pct}%)",
            **PLOTLY_THEME,
            height=210,
            showlegend=False
        )
        fig2.update_layout(margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig2, use_container_width=True)

        # Win method
        wm = get_win_by_method(conn)
        fig3 = go.Figure(go.Pie(
            labels=wm['method'],
            values=wm['count'],
            hole=0.55,
            marker=dict(colors=["#3b82f6", "#10b981", "#f97316"]),
            textfont=dict(family="Rajdhani", size=12)
        ))
        fig3.update_layout(
            title="Win Methods",
            **PLOTLY_THEME,
            height=210
        )
        fig3.update_layout(margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig3, use_container_width=True)

    # Venue analysis
    st.markdown('<p class="section-header">Top Venues</p>', unsafe_allow_html=True)
    venues = get_venue_stats(conn)
    fig4 = px.bar(
        venues, x='venue', y='matches',
        color='matches',
        color_continuous_scale=[[0, '#1e3a5f'], [1, '#f97316']],
        title="Matches Hosted by Venue"
    )
    fig4.update_layout(**PLOTLY_THEME, height=350, showlegend=False, coloraxis_showscale=False)
    fig4.update_xaxes(tickangle=-30)
    st.plotly_chart(fig4, use_container_width=True)


# ═══════════════════════════════════════════
# TAB 2: BATTING
# ═══════════════════════════════════════════
with tab2:
    st.markdown('<p class="section-header">Top Run-Scorers</p>', unsafe_allow_html=True)

    top_n_bat = st.slider("Show Top N Batsmen", 5, 20, 10, key="bat_slider")
    batsmen = get_top_batsmen(conn, season_filter, top_n_bat)

    col1, col2 = st.columns([3, 2])

    with col1:
        fig = px.bar(
            batsmen, x='player', y='runs',
            color='runs',
            color_continuous_scale=[[0, '#1e3a5f'], [0.5, '#3b82f6'], [1, '#f97316']],
            title="Total Runs Scored",
            text='runs'
        )
        fig.update_traces(textfont=dict(family='Rajdhani', color='white'), textposition='outside')
        fig.update_layout(**PLOTLY_THEME, height=380, coloraxis_showscale=False)
        fig.update_xaxes(tickangle=-35)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig2 = px.scatter(
            batsmen, x='matches', y='runs',
            size='runs', color='runs',
            hover_name='player',
            color_continuous_scale=[[0, '#1e3a5f'], [1, '#f97316']],
            title="Runs vs Matches Played"
        )
        fig2.update_layout(**PLOTLY_THEME, height=380, coloraxis_showscale=False)
        st.plotly_chart(fig2, use_container_width=True)

    # Table
    st.markdown('<p class="section-header">Batting Leaderboard</p>', unsafe_allow_html=True)
    batsmen_display = batsmen.copy()
    batsmen_display.index = range(1, len(batsmen_display) + 1)
    batsmen_display.columns = ['Player', 'Total Runs', 'Matches', 'Highest Score', 'Avg/Delivery']
    def highlight_runs(val, max_val):
        intensity = int(200 * val / max_val) if max_val > 0 else 0
        return f'background-color: rgba(249, {200 - intensity}, 50, 0.4); color: white'

    max_runs = batsmen_display['Total Runs'].max()
    st.dataframe(
        batsmen_display.style
            .applymap(lambda v: highlight_runs(v, max_runs), subset=['Total Runs'])
            .format({'Avg/Delivery': '{:.2f}'}),
        use_container_width=True
    )


# ═══════════════════════════════════════════
# TAB 3: BOWLING
# ═══════════════════════════════════════════
with tab3:
    st.markdown('<p class="section-header">Top Wicket-Takers</p>', unsafe_allow_html=True)

    top_n_bowl = st.slider("Show Top N Bowlers", 5, 20, 10, key="bowl_slider")
    bowlers = get_top_bowlers(conn, season_filter, top_n_bowl)

    col1, col2 = st.columns([3, 2])

    with col1:
        fig = px.bar(
            bowlers, x='player', y='wickets',
            color='wickets',
            color_continuous_scale=[[0, '#1e3a5f'], [0.5, '#10b981'], [1, '#f97316']],
            title="Total Wickets Taken",
            text='wickets'
        )
        fig.update_traces(textfont=dict(family='Rajdhani', color='white'), textposition='outside')
        fig.update_layout(**PLOTLY_THEME, height=380, coloraxis_showscale=False)
        fig.update_xaxes(tickangle=-35)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Economy bubble
        bowlers_clean = bowlers.dropna(subset=['economy'])
        fig2 = px.scatter(
            bowlers_clean, x='economy', y='wickets',
            size='wickets', color='economy',
            hover_name='player',
            color_continuous_scale=[[0, '#10b981'], [0.5, '#fbbf24'], [1, '#ef4444']],
            title="Wickets vs Economy Rate"
        )
        fig2.add_vline(x=bowlers_clean['economy'].mean(), line_dash="dash",
                       line_color="#64b5f6", annotation_text="Avg Economy",
                       annotation_font_color="#64b5f6")
        fig2.update_layout(**PLOTLY_THEME, height=380, coloraxis_showscale=False)
        st.plotly_chart(fig2, use_container_width=True)

    # Table
    st.markdown('<p class="section-header">Bowling Leaderboard</p>', unsafe_allow_html=True)
    bowlers_display = bowlers.copy()
    bowlers_display.index = range(1, len(bowlers_display) + 1)
    bowlers_display.columns = ['Player', 'Wickets', 'Matches', 'Economy']
    def highlight_wickets(val, max_val):
        intensity = int(200 * val / max_val) if max_val > 0 else 0
        return f'background-color: rgba(16, {200 - intensity}, 130, 0.4); color: white'

    max_wkts = bowlers_display['Wickets'].max()
    st.dataframe(
        bowlers_display.style
            .applymap(lambda v: highlight_wickets(v, max_wkts), subset=['Wickets'])
            .format({'Economy': '{:.2f}'}),
        use_container_width=True
    )


# ═══════════════════════════════════════════
# TAB 4: SEASON TRENDS
# ═══════════════════════════════════════════
with tab4:
    st.markdown('<p class="section-header">Season-by-Season Trends</p>', unsafe_allow_html=True)

    season_df = get_season_summary(conn)
    boundaries = get_6s_4s_by_season(conn)

    col1, col2 = st.columns(2)

    with col1:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=season_df['season'], y=season_df['matches_played'],
            mode='lines+markers',
            line=dict(color='#f97316', width=3),
            marker=dict(size=9, color='#f97316', symbol='diamond'),
            fill='tozeroy',
            fillcolor='rgba(249,115,22,0.1)',
            name='Matches'
        ))
        fig.update_layout(title="Matches Per Season", **PLOTLY_THEME, height=320)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            x=boundaries['season'], y=boundaries['sixes'],
            name='Sixes', marker_color='#f97316'
        ))
        fig2.add_trace(go.Bar(
            x=boundaries['season'], y=boundaries['fours'],
            name='Fours', marker_color='#3b82f6'
        ))
        fig2.update_layout(
            title="Sixes & Fours Per Season",
            barmode='group',
            **PLOTLY_THEME, height=320
        )
        st.plotly_chart(fig2, use_container_width=True)

    # Animated win chart
    st.markdown('<p class="section-header">Team Dominance Over Seasons</p>', unsafe_allow_html=True)
    all_wins_by_season = pd.read_sql("""
        SELECT season, winner AS team, COUNT(*) AS wins
        FROM matches
        GROUP BY season, winner
        ORDER BY season, wins DESC
    """, conn)

    top_teams_list = team_wins.head(8)['team'].tolist()
    filtered_season_wins = all_wins_by_season[all_wins_by_season['team'].isin(top_teams_list)]

    fig3 = px.line(
        filtered_season_wins, x='season', y='wins', color='team',
        markers=True,
        title="Wins Per Season – Top 8 Teams",
        color_discrete_map=IPL_COLORS
    )
    fig3.update_traces(line=dict(width=2.5), marker=dict(size=8))
    fig3.update_layout(**PLOTLY_THEME, height=420, legend=dict(
        bgcolor="rgba(13,27,42,0.8)",
        bordercolor="#1e3a5f",
        font=dict(color="#94a3b8", family="Rajdhani")
    ))
    st.plotly_chart(fig3, use_container_width=True)


# ═══════════════════════════════════════════
# TAB 5: PLAYER DEEP DIVE
# ═══════════════════════════════════════════
with tab5:
    st.markdown('<p class="section-header">Player Career Tracker</p>', unsafe_allow_html=True)

    all_batters = sorted(deliveries['batter'].dropna().unique().tolist())
    selected_player = st.selectbox("🔍 Search Player", all_batters)

    if selected_player:
        player_runs = get_player_season_runs(conn, selected_player)

        col1, col2, col3 = st.columns(3)
        total_r = player_runs['runs'].sum()
        peak_season = player_runs.loc[player_runs['runs'].idxmax(), 'season'] if not player_runs.empty else "N/A"
        seasons_played = len(player_runs)
        col1.metric("🏏 Career Runs", f"{total_r:,}")
        col2.metric("📅 Seasons Played", seasons_played)
        col3.metric("🌟 Best Season", peak_season)

        if not player_runs.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=player_runs['season'], y=player_runs['runs'],
                marker=dict(
                    color=player_runs['runs'],
                    colorscale=[[0, '#1e3a5f'], [0.5, '#3b82f6'], [1, '#f97316']],
                    showscale=False
                ),
                name='Runs'
            ))
            fig.add_trace(go.Scatter(
                x=player_runs['season'], y=player_runs['runs'],
                mode='lines+markers',
                line=dict(color='#fbbf24', width=2.5, dash='dot'),
                marker=dict(size=8, color='#fbbf24'),
                name='Trend'
            ))
            fig.update_layout(
                title=f"{selected_player} — Season-wise Runs",
                **PLOTLY_THEME,
                height=380
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No batting data found for this player.")

        # Head-to-head dismissal breakdown
        st.markdown('<p class="section-header">Dismissal Analysis</p>', unsafe_allow_html=True)
        dismissals = pd.read_sql(f"""
            SELECT dismissal_kind, COUNT(*) AS count
            FROM deliveries
            WHERE player_dismissed = '{selected_player}'
              AND dismissal_kind IS NOT NULL
            GROUP BY dismissal_kind
            ORDER BY count DESC
        """, conn)

        if not dismissals.empty:
            fig2 = px.pie(
                dismissals, values='count', names='dismissal_kind',
                hole=0.5,
                color_discrete_sequence=px.colors.sequential.Plasma_r
            )
            fig2.update_layout(
                title="How Was This Player Dismissed?",
                **PLOTLY_THEME, height=380
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No dismissal records found for this player.")
