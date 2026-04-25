"""
Interactive Plotly Dash dashboard — Urban Health & Park Access
=============================================================

Visualisations:
  1. Scatter — Park access vs physical inactivity (urban counties)
     Drill-down: click a state in the dropdown to highlight it.
  2. Scatter — Inactivity rate vs life expectancy (all counties)
     Bubble size = obesity rate, colour = smoking rate.
  3. Choropleth — County-level physical inactivity rates across the US.
  4. Box plot — Life expectancy distribution by urban-rural category.
  5. Heatmap — Spearman correlation matrix of all health variables.

Running:
    pip install -r requirements.txt
    python dashboard/app.py
    Open http://127.0.0.1:8050 in your browser.
"""

import sys
from pathlib import Path

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
from scipy import stats

import dash
from dash import dcc, html, Input, Output, callback
import dash_bootstrap_components as dbc

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import PROCESSED_DIR

# ── Data loading ──────────────────────────────────────────────────────────────

def load_data() -> pd.DataFrame:
    csv = PROCESSED_DIR / "county_analysis.csv"
    if not csv.exists():
        raise FileNotFoundError(
            f"county_analysis.csv not found at {csv}.\n"
            "Run the pipeline first:  python run_pipeline.py"
        )
    df = pd.read_csv(csv, dtype={"fips": str})
    df["fips"] = df["fips"].str.zfill(5)
    return df


df = load_data()

# Pre-compute Spearman correlation matrix for heatmap
HEALTH_COLS = [c for c in
               ["park_access_pct", "inactivity_rate", "obesity_rate",
                "smoking_rate", "life_expectancy"]
               if c in df.columns]
CORR_LABELS = {
    "park_access_pct":  "Park Access %",
    "inactivity_rate":  "Inactivity %",
    "obesity_rate":     "Obesity %",
    "smoking_rate":     "Smoking %",
    "life_expectancy":  "Life Expectancy",
}
corr_mat = df[HEALTH_COLS].corr(method="spearman")

# State options for filter
states = sorted(df["state_abbr"].dropna().unique())

# ── Helper: add regression line to a scatter figure ──────────────────────────

def add_trendline(fig: go.Figure, x: pd.Series, y: pd.Series,
                  color: str = "black") -> go.Figure:
    mask = x.notna() & y.notna()
    slope, intercept, *_ = stats.linregress(x[mask], y[mask])
    x_range = np.linspace(x[mask].min(), x[mask].max(), 200)
    fig.add_trace(go.Scatter(
        x=x_range, y=slope * x_range + intercept,
        mode="lines", line=dict(color=color, dash="dash", width=2),
        name="Trend line", showlegend=True
    ))
    return fig


# ── Layout ────────────────────────────────────────────────────────────────────

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.FLATLY],
    title="Urban Health & Park Access Dashboard",
)

SIDEBAR = dbc.Card([
    html.H5("Filters", className="card-title"),
    html.Hr(),
    html.Label("State (multi-select)"),
    dcc.Dropdown(
        id="state-filter",
        options=[{"label": s, "value": s} for s in states],
        value=[],
        multi=True,
        placeholder="All states",
        clearable=True,
    ),
    html.Br(),
    html.Label("Urban-Rural Category"),
    dcc.Checklist(
        id="urban-filter",
        options=[
            {"label": "Urban (codes 1–4)",   "value": 1},
            {"label": "Non-urban (codes 5–6)", "value": 0},
        ],
        value=[0, 1],
        inputStyle={"margin-right": "6px"},
    ),
    html.Br(),
    html.Label("Park Access Quartile"),
    dcc.Checklist(
        id="quartile-filter",
        options=[{"label": q, "value": q}
                 for q in sorted(df["park_quartile"].dropna().unique())],
        value=sorted(df["park_quartile"].dropna().unique()),
        inputStyle={"margin-right": "6px"},
    ),
    html.Hr(),
    html.Small(
        "Data: CDC PLACES 2024, County Health Rankings 2024, NCHS 2013",
        className="text-muted",
    ),
], body=True, style={"position": "sticky", "top": 0})

HEADER = dbc.Row([
    dbc.Col([
        html.H2("Urban Health & Park Access", className="display-5 fw-bold"),
        html.P(
            "Exploring the relationship between park access, physical inactivity, "
            "and life expectancy across U.S. counties.",
            className="lead",
        ),
    ])
], className="mb-4")

HYPOTHESIS_CARDS = dbc.Row([
    dbc.Col(dbc.Card([
        dbc.CardHeader("Hypothesis 1"),
        dbc.CardBody(html.P(
            "In urban counties, higher park / exercise access is associated "
            "with lower physical inactivity rates.",
            className="card-text",
        )),
    ], color="primary", outline=True), md=6),
    dbc.Col(dbc.Card([
        dbc.CardHeader("Hypothesis 2"),
        dbc.CardBody(html.P(
            "Higher inactivity rates are associated with lower life expectancy, "
            "even after controlling for obesity and smoking.",
            className="card-text",
        )),
    ], color="success", outline=True), md=6),
], className="mb-4")

TABS = dcc.Tabs(id="tabs", value="tab-h1", children=[
    dcc.Tab(label="H1 — Park Access vs Inactivity", value="tab-h1"),
    dcc.Tab(label="H2 — Inactivity vs Life Expectancy", value="tab-h2"),
    dcc.Tab(label="National Map", value="tab-map"),
    dcc.Tab(label="Urban-Rural Breakdown", value="tab-urb"),
    dcc.Tab(label="Correlation Heatmap", value="tab-corr"),
])

app.layout = dbc.Container([
    html.Br(),
    HEADER,
    HYPOTHESIS_CARDS,
    dbc.Row([
        dbc.Col(SIDEBAR, md=2),
        dbc.Col([
            TABS,
            html.Div(id="tab-content", className="mt-3"),
        ], md=10),
    ]),
], fluid=True)


# ── Callbacks ─────────────────────────────────────────────────────────────────

def _filter(state_vals: list, urban_vals: list, q_vals: list) -> pd.DataFrame:
    """Apply sidebar filters to the global DataFrame."""
    out = df.copy()
    if state_vals:
        out = out[out["state_abbr"].isin(state_vals)]
    if set(urban_vals) != {0, 1}:
        out = out[out["is_urban"].isin(urban_vals)]
    if q_vals and "park_quartile" in out.columns:
        out = out[out["park_quartile"].isin(q_vals)]
    return out


@callback(
    Output("tab-content", "children"),
    Input("tabs", "value"),
    Input("state-filter", "value"),
    Input("urban-filter", "value"),
    Input("quartile-filter", "value"),
)
def render_tab(tab, state_vals, urban_vals, q_vals):
    filtered = _filter(state_vals or [], urban_vals or [0, 1], q_vals or [])

    # ── Tab 1: H1 scatter ──────────────────────────────────────────────────────
    if tab == "tab-h1":
        urban_df = filtered[filtered["is_urban"] == 1].dropna(
            subset=["park_access_pct", "inactivity_rate"]
        )
        fig = px.scatter(
            urban_df,
            x="park_access_pct", y="inactivity_rate",
            color="state_abbr",
            hover_name="county_name",
            hover_data={"state_abbr": True, "ur_label": True,
                        "park_access_pct": ":.1f", "inactivity_rate": ":.1f"},
            labels={
                "park_access_pct":  "Park / Exercise Access (%)",
                "inactivity_rate":  "Physical Inactivity (%)",
                "state_abbr":       "State",
            },
            title=(
                f"Park Access vs Physical Inactivity — Urban Counties "
                f"(n={len(urban_df):,})"
            ),
            opacity=0.7,
            height=550,
        )
        if len(urban_df) > 1:
            fig = add_trendline(fig, urban_df["park_access_pct"],
                                urban_df["inactivity_rate"])
        fig.update_layout(legend_title_text="State", showlegend=True)
        fig.update_xaxes(title="Park / Exercise Access (%)")
        fig.update_yaxes(title="Physical Inactivity (%)")

        rho, p = stats.spearmanr(urban_df["park_access_pct"],
                                 urban_df["inactivity_rate"])
        annotation = (
            f"Spearman ρ = {rho:.3f}  (p {'< 0.001' if p < 0.001 else f'= {p:.3f}'})"
        )
        fig.add_annotation(
            xref="paper", yref="paper", x=0.01, y=0.99,
            text=annotation, showarrow=False,
            bordercolor="black", borderwidth=1, bgcolor="white", opacity=0.8,
        )
        return dcc.Graph(figure=fig)

    # ── Tab 2: H2 bubble scatter ───────────────────────────────────────────────
    elif tab == "tab-h2":
        sub = filtered.dropna(
            subset=["inactivity_rate", "life_expectancy",
                    "obesity_rate", "smoking_rate"]
        )
        fig = px.scatter(
            sub,
            x="inactivity_rate", y="life_expectancy",
            size="obesity_rate",
            color="smoking_rate",
            color_continuous_scale="RdYlGn_r",
            hover_name="county_name",
            hover_data={"state_abbr": True, "ur_label": True,
                        "inactivity_rate": ":.1f", "life_expectancy": ":.1f",
                        "obesity_rate": ":.1f", "smoking_rate": ":.1f"},
            labels={
                "inactivity_rate":  "Physical Inactivity (%)",
                "life_expectancy":  "Life Expectancy (years)",
                "obesity_rate":     "Obesity % (bubble size)",
                "smoking_rate":     "Smoking %",
            },
            title=f"Inactivity vs Life Expectancy (n={len(sub):,})",
            opacity=0.65,
            height=580,
        )
        if len(sub) > 1:
            fig = add_trendline(fig, sub["inactivity_rate"],
                                sub["life_expectancy"])
        r, p = stats.pearsonr(sub["inactivity_rate"], sub["life_expectancy"])
        fig.add_annotation(
            xref="paper", yref="paper", x=0.99, y=0.99,
            text=f"Pearson r = {r:.3f}  (p {'< 0.001' if p < 0.001 else f'= {p:.3f}'})",
            showarrow=False, xanchor="right",
            bordercolor="black", borderwidth=1, bgcolor="white", opacity=0.8,
        )
        return dcc.Graph(figure=fig)

    # ── Tab 3: National choropleth ─────────────────────────────────────────────
    elif tab == "tab-map":
        map_df = filtered.dropna(subset=["inactivity_rate"])
        fig = px.choropleth(
            map_df,
            locations="fips",
            locationmode="USA-states",   # will use county FIPS with geojson below
            color="inactivity_rate",
            color_continuous_scale="YlOrRd",
            scope="usa",
            hover_name="county_name",
            hover_data={"state_abbr": True, "inactivity_rate": ":.1f",
                        "life_expectancy": ":.1f"},
            labels={"inactivity_rate": "Inactivity (%)"},
            title="Physical Inactivity Rate by County",
            height=560,
        )
        # Use county-level geojson for true county polygons
        import urllib.request, json
        geojson_url = (
            "https://raw.githubusercontent.com/plotly/datasets/master/"
            "geojson-counties-fips.json"
        )
        try:
            with urllib.request.urlopen(geojson_url, timeout=10) as r:
                counties_geo = json.loads(r.read())
            fig = px.choropleth(
                map_df,
                geojson=counties_geo,
                locations="fips",
                color="inactivity_rate",
                color_continuous_scale="YlOrRd",
                scope="usa",
                hover_name="county_name",
                hover_data={"state_abbr": True, "inactivity_rate": ":.1f",
                            "life_expectancy": ":.1f"},
                labels={"inactivity_rate": "Inactivity (%)"},
                title="Physical Inactivity Rate by County (darker = more inactive)",
                height=560,
            )
            fig.update_geos(fitbounds="locations", visible=False)
        except Exception:
            pass  # Fall back to state-level if network unavailable
        return dcc.Graph(figure=fig)

    # ── Tab 4: Box plot by urban-rural category ────────────────────────────────
    elif tab == "tab-urb":
        order = [
            "Large central metro", "Large fringe metro", "Medium metro",
            "Small metro", "Micropolitan", "Non-core (rural)",
        ]
        sub = filtered.dropna(subset=["life_expectancy", "ur_label"])
        present = [o for o in order if o in sub["ur_label"].values]
        fig = px.box(
            sub,
            x="ur_label", y="life_expectancy",
            color="ur_label",
            category_orders={"ur_label": present},
            points="outliers",
            hover_name="county_name",
            hover_data={"state_abbr": True, "inactivity_rate": ":.1f"},
            labels={"ur_label": "Urban-Rural Category",
                    "life_expectancy": "Life Expectancy (years)"},
            title="Life Expectancy by Urban-Rural Classification",
            height=520,
        )
        fig.update_layout(showlegend=False, xaxis_tickangle=-20)

        # Overlay inactivity as a secondary scatter on right axis
        fig2 = px.strip(
            sub, x="ur_label", y="inactivity_rate",
            category_orders={"ur_label": present},
        )
        return dbc.Row([
            dbc.Col(dcc.Graph(figure=fig), md=7),
            dbc.Col([
                html.H6("Mean inactivity by category", className="mt-3"),
                html.Table(
                    [html.Thead(html.Tr([html.Th("Category"), html.Th("Inactivity %")]))] +
                    [html.Tbody([
                        html.Tr([html.Td(row["ur_label"]),
                                 html.Td(f"{row['inactivity_rate']:.1f}")])
                        for _, row in (
                            sub.groupby("ur_label")["inactivity_rate"]
                               .mean().reset_index()
                               .rename(columns={"inactivity_rate": "inactivity_rate"})
                               .sort_values("inactivity_rate")
                        ).iterrows()
                    ])],
                    className="table table-sm table-striped",
                ),
            ], md=5),
        ])

    # ── Tab 5: Correlation heatmap ─────────────────────────────────────────────
    elif tab == "tab-corr":
        sub = filtered[HEALTH_COLS].dropna()
        corr = sub.corr(method="spearman")
        labels = [CORR_LABELS.get(c, c) for c in corr.columns]
        z = corr.values

        fig = go.Figure(data=go.Heatmap(
            z=z,
            x=labels,
            y=labels,
            colorscale="RdBu",
            zmid=0,
            zmin=-1, zmax=1,
            text=np.round(z, 2),
            texttemplate="%{text}",
            hovertemplate="x: %{x}<br>y: %{y}<br>ρ: %{z:.3f}<extra></extra>",
        ))
        fig.update_layout(
            title=(
                f"Spearman Correlation Matrix — Health Variables "
                f"(n={len(sub):,} counties)"
            ),
            height=480,
            xaxis_side="bottom",
        )
        return dcc.Graph(figure=fig)

    return html.P("Select a tab.")


# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(debug=True, port=8050)
