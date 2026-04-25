"""
Statistical analysis for both project hypotheses.

Hypothesis 1 (H1):
  In urban U.S. counties, higher park access (% with exercise opportunity access)
  is associated with lower rates of physical inactivity.
  → Spearman correlation + OLS regression on urban counties only.

Hypothesis 2 (H2):
  Higher physical inactivity rates are associated with lower life expectancy,
  even after controlling for smoking and obesity rates.
  → Multiple OLS regression (life_expectancy ~ inactivity + obesity + smoking).
  → Partial correlation of inactivity with life expectancy controlling for the
    other two variables.

All results are printed to stdout and returned as dicts for use in the dashboard.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats
import statsmodels.api as sm
import statsmodels.formula.api as smf

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.db_utils import read_table
from config import PROCESSED_DIR, ALPHA


def _load_analysis() -> pd.DataFrame:
    csv = PROCESSED_DIR / "county_analysis.csv"
    if csv.exists():
        return pd.read_csv(csv, dtype={"fips": str})
    return read_table("county_analysis")


# ─────────────────────────────────────────────────────────────────────────────
# Hypothesis 1
# ─────────────────────────────────────────────────────────────────────────────

def test_h1(df: pd.DataFrame) -> dict:
    """
    H1: In urban counties, park_access_pct ↑  →  inactivity_rate ↓
    """
    urban = df[df["is_urban"] == 1].dropna(
        subset=["park_access_pct", "inactivity_rate"]
    )
    print(f"\n{'='*60}")
    print(f"HYPOTHESIS 1  (n = {len(urban):,} urban counties)")
    print(f"{'='*60}")

    # ── Spearman correlation (non-parametric, robust to outliers) ─────────────
    rho, p_spearman = stats.spearmanr(urban["park_access_pct"],
                                      urban["inactivity_rate"])
    sig = "✓ significant" if p_spearman < ALPHA else "✗ not significant"
    print(f"\nSpearman ρ = {rho:.4f},  p = {p_spearman:.4e}  ({sig})")

    # ── Pearson correlation ───────────────────────────────────────────────────
    r, p_pearson = stats.pearsonr(urban["park_access_pct"],
                                  urban["inactivity_rate"])
    print(f"Pearson  r = {r:.4f},  p = {p_pearson:.4e}")

    # ── OLS: inactivity_rate ~ park_access_pct ────────────────────────────────
    model = smf.ols("inactivity_rate ~ park_access_pct", data=urban).fit()
    print(f"\nOLS (simple): inactivity_rate ~ park_access_pct")
    print(f"  β(park_access_pct) = {model.params['park_access_pct']:.4f}")
    print(f"  R² = {model.rsquared:.4f},  F-p = {model.f_pvalue:.4e}")

    # ── OLS by park quartile ──────────────────────────────────────────────────
    if "park_quartile" in urban.columns:
        q_means = urban.groupby("park_quartile")["inactivity_rate"].mean()
        print(f"\nMean inactivity by park access quartile:")
        print(q_means.to_string())

    return {
        "n_urban": len(urban),
        "spearman_rho": rho,
        "spearman_p": p_spearman,
        "pearson_r": r,
        "pearson_p": p_pearson,
        "ols_beta": model.params["park_access_pct"],
        "ols_r2": model.rsquared,
        "ols_f_p": model.f_pvalue,
        "supported": (p_spearman < ALPHA) and (rho < 0),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Hypothesis 2
# ─────────────────────────────────────────────────────────────────────────────

def _partial_corr(df: pd.DataFrame, x: str, y: str,
                  controls: list[str]) -> tuple[float, float]:
    """
    Compute partial Pearson correlation of x ~ y controlling for `controls`.
    Residualises both x and y on the control variables via OLS.
    """
    cols = [x, y] + controls
    sub = df[cols].dropna()
    X_ctrl = sm.add_constant(sub[controls])

    res_x = sm.OLS(sub[x], X_ctrl).fit().resid
    res_y = sm.OLS(sub[y], X_ctrl).fit().resid
    r, p = stats.pearsonr(res_x, res_y)
    return r, p


def test_h2(df: pd.DataFrame) -> dict:
    """
    H2: inactivity_rate ↑  →  life_expectancy ↓, controlling for obesity & smoking.
    """
    sub = df.dropna(
        subset=["inactivity_rate", "obesity_rate", "smoking_rate", "life_expectancy"]
    )
    print(f"\n{'='*60}")
    print(f"HYPOTHESIS 2  (n = {len(sub):,} counties, all urban-rural)")
    print(f"{'='*60}")

    # ── Multiple OLS ──────────────────────────────────────────────────────────
    formula = "life_expectancy ~ inactivity_rate + obesity_rate + smoking_rate"
    model = smf.ols(formula, data=sub).fit()
    print(f"\nMultiple OLS: {formula}")
    print(model.summary2().tables[1].to_string())
    print(f"\nR² = {model.rsquared:.4f}  (adj R² = {model.rsquared_adj:.4f})")
    print(f"F-statistic p-value = {model.f_pvalue:.4e}")

    inact_beta = model.params["inactivity_rate"]
    inact_p    = model.pvalues["inactivity_rate"]
    sig = "✓ significant" if inact_p < ALPHA else "✗ not significant"
    print(f"\ninactivity_rate coefficient: {inact_beta:.4f}  "
          f"(p={inact_p:.4e})  {sig}")

    # ── Partial correlation ───────────────────────────────────────────────────
    r_partial, p_partial = _partial_corr(
        sub, "inactivity_rate", "life_expectancy",
        controls=["obesity_rate", "smoking_rate"]
    )
    print(f"\nPartial correlation (inactivity ↔ life_exp | obesity, smoking):")
    print(f"  r = {r_partial:.4f},  p = {p_partial:.4e}")

    # ── Simple correlation for context ────────────────────────────────────────
    r_simple, p_simple = stats.pearsonr(sub["inactivity_rate"],
                                        sub["life_expectancy"])
    print(f"\nSimple Pearson r (inactivity, life_exp) = {r_simple:.4f}  "
          f"(p={p_simple:.4e})")

    return {
        "n": len(sub),
        "ols_beta_inactivity": inact_beta,
        "ols_p_inactivity": inact_p,
        "ols_r2": model.rsquared,
        "ols_adj_r2": model.rsquared_adj,
        "partial_r": r_partial,
        "partial_p": p_partial,
        "simple_r": r_simple,
        "supported": (inact_p < ALPHA) and (inact_beta < 0),
        "model": model,  # full statsmodels result (not JSON-serialisable)
    }


# ─────────────────────────────────────────────────────────────────────────────
# Correlation matrix (all numeric health variables)
# ─────────────────────────────────────────────────────────────────────────────

def correlation_matrix(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "park_access_pct", "inactivity_rate", "obesity_rate",
        "smoking_rate", "life_expectancy",
    ]
    cols = [c for c in cols if c in df.columns]
    corr = df[cols].corr(method="spearman")
    print("\nSpearman correlation matrix:")
    print(corr.to_string())
    return corr


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_all() -> dict:
    df = _load_analysis()
    h1 = test_h1(df)
    h2 = test_h2(df)
    corr = correlation_matrix(df)
    return {"h1": h1, "h2": h2, "corr_matrix": corr}


if __name__ == "__main__":
    results = run_all()
    print("\n── Summary ────────────────────────────────────────────────────")
    print(f"H1 supported: {results['h1']['supported']}")
    print(f"H2 supported: {results['h2']['supported']}")
