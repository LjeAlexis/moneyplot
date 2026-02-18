"""Page 1 — Carte des prix immobiliers."""

import streamlit as st
import pandas as pd
import pydeck as pdk

from moneyplot.storage.db import get_connection

st.set_page_config(page_title="Carte des prix", layout="wide")
st.title("Carte des prix au m\u00b2")

# ── Filters ──────────────────────────────────────────────────────────────────

try:
    con = get_connection()
except Exception:
    st.error("Base de données non disponible. Lancez le pipeline Dagster.")
    st.stop()

col1, col2, col3 = st.columns(3)

with col1:
    depts = con.execute(
        "SELECT DISTINCT code_departement FROM mutations ORDER BY 1"
    ).fetchdf()["code_departement"].tolist()
    selected_dept = st.selectbox("Département", ["Tous"] + depts)

with col2:
    types = con.execute(
        "SELECT DISTINCT type_local FROM mutations ORDER BY 1"
    ).fetchdf()["type_local"].tolist()
    selected_type = st.selectbox("Type de bien", ["Tous"] + types)

with col3:
    years = con.execute(
        "SELECT DISTINCT annee FROM mutations ORDER BY annee DESC"
    ).fetchdf()["annee"].tolist()
    selected_year = st.selectbox("Année", ["Toutes"] + [int(y) for y in years])

# ── Query ────────────────────────────────────────────────────────────────────

where_clauses = ["prix_m2 IS NOT NULL", "latitude IS NOT NULL", "longitude IS NOT NULL"]
if selected_dept != "Tous":
    where_clauses.append(f"code_departement = '{selected_dept}'")
if selected_type != "Tous":
    where_clauses.append(f"type_local = '{selected_type}'")
if selected_year != "Toutes":
    where_clauses.append(f"annee = {selected_year}")

where_sql = " AND ".join(where_clauses)

# Aggregate by commune for the map
query = f"""
    SELECT
        nom_commune,
        code_commune,
        AVG(latitude) AS lat,
        AVG(longitude) AS lon,
        MEDIAN(prix_m2) AS prix_m2_median,
        COUNT(*) AS nb_transactions
    FROM mutations
    WHERE {where_sql}
    GROUP BY nom_commune, code_commune
    HAVING COUNT(*) >= 5
    ORDER BY prix_m2_median DESC
"""

df = con.execute(query).fetchdf()
con.close()

if df.empty:
    st.warning("Aucune donnée pour les filtres sélectionnés.")
    st.stop()

st.caption(f"{len(df)} communes affichées, {df['nb_transactions'].sum():,.0f} transactions")

# ── Map ──────────────────────────────────────────────────────────────────────

# Normalize price for color
p_min = df["prix_m2_median"].quantile(0.05)
p_max = df["prix_m2_median"].quantile(0.95)
df["price_norm"] = ((df["prix_m2_median"] - p_min) / (p_max - p_min)).clip(0, 1)
df["r"] = (df["price_norm"] * 255).astype(int)
df["g"] = ((1 - df["price_norm"]) * 200).astype(int)
df["b"] = 80

layer = pdk.Layer(
    "ScatterplotLayer",
    data=df,
    get_position=["lon", "lat"],
    get_radius="nb_transactions * 30 + 200",
    get_fill_color=["r", "g", "b", 180],
    pickable=True,
    auto_highlight=True,
)

view = pdk.ViewState(latitude=46.6, longitude=2.3, zoom=5.5, pitch=0)

tooltip = {
    "html": (
        "<b>{nom_commune}</b><br>"
        "Prix médian : {prix_m2_median:.0f} €/m²<br>"
        "Transactions : {nb_transactions}"
    ),
    "style": {"backgroundColor": "#333", "color": "white"},
}

st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view, tooltip=tooltip))

# ── Table ────────────────────────────────────────────────────────────────────

st.subheader("Top communes par prix médian")
top = df.head(20)[["nom_commune", "code_commune", "prix_m2_median", "nb_transactions"]]
top.columns = ["Commune", "Code", "Prix médian €/m²", "Transactions"]
st.dataframe(top, use_container_width=True, hide_index=True)
