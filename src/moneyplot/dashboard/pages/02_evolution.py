"""Page 2 — Évolution temporelle des prix."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from moneyplot.storage.db import get_connection

st.set_page_config(page_title="Évolution des prix", layout="wide")
st.title("Évolution des prix au m\u00b2")

try:
    con = get_connection()
except Exception:
    st.error("Base de données non disponible.")
    st.stop()

# ── Filters ──────────────────────────────────────────────────────────────────

col1, col2 = st.columns(2)

with col1:
    depts = con.execute(
        "SELECT DISTINCT code_departement FROM mutations ORDER BY 1"
    ).fetchdf()["code_departement"].tolist()
    selected_depts = st.multiselect("Départements", depts, default=depts[:1] if depts else [])

with col2:
    selected_type = st.selectbox(
        "Type de bien",
        ["Tous", "Appartement", "Maison"],
    )

# ── Query ────────────────────────────────────────────────────────────────────

if not selected_depts:
    st.info("Sélectionnez au moins un département.")
    st.stop()

dept_list = ", ".join(f"'{d}'" for d in selected_depts)
type_filter = f"AND type_local = '{selected_type}'" if selected_type != "Tous" else ""

query = f"""
    SELECT
        annee,
        trimestre,
        code_departement,
        MEDIAN(prix_m2) AS prix_m2_median,
        AVG(prix_m2) AS prix_m2_moyen,
        COUNT(*) AS nb_transactions
    FROM mutations
    WHERE prix_m2 IS NOT NULL
      AND code_departement IN ({dept_list})
      {type_filter}
    GROUP BY annee, trimestre, code_departement
    ORDER BY annee, trimestre
"""

df = con.execute(query).fetchdf()

if df.empty:
    st.warning("Aucune donnée pour les filtres sélectionnés.")
    con.close()
    st.stop()

# Create a proper date column (first day of the quarter)
df["date"] = df.apply(
    lambda r: f"{int(r['annee'])}-{int((r['trimestre'] - 1) * 3 + 1):02d}-01", axis=1
)
df["date"] = df["date"].astype("datetime64[ns]")

# ── Chart — Price evolution ──────────────────────────────────────────────────

fig = px.line(
    df,
    x="date",
    y="prix_m2_median",
    color="code_departement",
    labels={
        "date": "Date",
        "prix_m2_median": "Prix médian (€/m²)",
        "code_departement": "Département",
    },
    title="Prix médian au m² par trimestre",
)
fig.update_layout(hovermode="x unified")
st.plotly_chart(fig, use_container_width=True)

# ── Chart — Transaction volume ───────────────────────────────────────────────

fig2 = px.bar(
    df,
    x="date",
    y="nb_transactions",
    color="code_departement",
    labels={
        "date": "Date",
        "nb_transactions": "Nombre de transactions",
        "code_departement": "Département",
    },
    title="Volume de transactions par trimestre",
    barmode="group",
)
st.plotly_chart(fig2, use_container_width=True)

# ── Overlay mortgage rates if available ──────────────────────────────────────

try:
    taux = con.execute("""
        SELECT date, taux FROM taux_hypothecaires ORDER BY date
    """).fetchdf()
    if not taux.empty:
        st.subheader("Taux hypothécaires (overlay)")
        fig3 = go.Figure()
        for dept in selected_depts:
            dept_df = df[df["code_departement"] == dept]
            fig3.add_trace(go.Scatter(
                x=dept_df["date"], y=dept_df["prix_m2_median"],
                name=f"Prix {dept}", yaxis="y1",
            ))
        fig3.add_trace(go.Scatter(
            x=taux["date"], y=taux["taux"],
            name="Taux hypothécaire (%)", yaxis="y2",
            line=dict(dash="dash", color="red"),
        ))
        fig3.update_layout(
            yaxis=dict(title="Prix médian (€/m²)"),
            yaxis2=dict(title="Taux (%)", overlaying="y", side="right"),
            hovermode="x unified",
        )
        st.plotly_chart(fig3, use_container_width=True)
except Exception:
    pass  # Table may not exist yet

con.close()
