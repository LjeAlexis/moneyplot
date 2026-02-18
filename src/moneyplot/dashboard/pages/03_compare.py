"""Page 3 — Comparaison de communes."""

import streamlit as st
import plotly.express as px
import pandas as pd

from moneyplot.storage.db import get_connection

st.set_page_config(page_title="Comparaison", layout="wide")
st.title("Comparaison de communes")

try:
    con = get_connection()
except Exception:
    st.error("Base de données non disponible.")
    st.stop()

# ── Commune selector ─────────────────────────────────────────────────────────

communes = con.execute("""
    SELECT DISTINCT nom_commune, code_commune, code_departement
    FROM mutations
    WHERE nom_commune IS NOT NULL
    ORDER BY nom_commune
""").fetchdf()

commune_options = (
    communes["nom_commune"] + " (" + communes["code_departement"] + ")"
).tolist()

selected = st.multiselect(
    "Communes à comparer (max 5)",
    commune_options,
    max_selections=5,
)

if not selected:
    st.info("Sélectionnez des communes pour les comparer.")
    con.close()
    st.stop()

# Extract code_commune from selection
selected_codes = []
for s in selected:
    name = s.rsplit(" (", 1)[0]
    dept = s.rsplit("(", 1)[1].rstrip(")")
    match = communes[
        (communes["nom_commune"] == name) & (communes["code_departement"] == dept)
    ]
    if not match.empty:
        selected_codes.append(match.iloc[0]["code_commune"])

if not selected_codes:
    st.stop()

codes_sql = ", ".join(f"'{c}'" for c in selected_codes)

# ── Filters ──────────────────────────────────────────────────────────────────

selected_type = st.selectbox("Type de bien", ["Tous", "Appartement", "Maison"])
type_filter = f"AND type_local = '{selected_type}'" if selected_type != "Tous" else ""

# ── Key metrics ──────────────────────────────────────────────────────────────

st.subheader("Indicateurs clés")

metrics = con.execute(f"""
    SELECT
        nom_commune,
        code_departement,
        MEDIAN(prix_m2) AS prix_m2_median,
        AVG(prix_m2) AS prix_m2_moyen,
        COUNT(*) AS nb_transactions,
        MEDIAN(surface_reelle_bati) AS surface_mediane,
        MEDIAN(valeur_fonciere) AS prix_median
    FROM mutations
    WHERE code_commune IN ({codes_sql})
      AND prix_m2 IS NOT NULL
      {type_filter}
    GROUP BY nom_commune, code_departement
""").fetchdf()

cols = st.columns(len(metrics))
for i, (_, row) in enumerate(metrics.iterrows()):
    with cols[i]:
        st.metric(f"{row['nom_commune']} ({row['code_departement']})", "")
        st.write(f"**Prix médian/m²** : {row['prix_m2_median']:,.0f} €")
        st.write(f"**Prix médian** : {row['prix_median']:,.0f} €")
        st.write(f"**Surface médiane** : {row['surface_mediane']:,.0f} m²")
        st.write(f"**Transactions** : {row['nb_transactions']:,}")

# ── Evolution comparison ─────────────────────────────────────────────────────

st.subheader("Évolution comparée")

evo = con.execute(f"""
    SELECT
        nom_commune,
        annee,
        trimestre,
        MEDIAN(prix_m2) AS prix_m2_median,
        COUNT(*) AS nb
    FROM mutations
    WHERE code_commune IN ({codes_sql})
      AND prix_m2 IS NOT NULL
      {type_filter}
    GROUP BY nom_commune, annee, trimestre
    ORDER BY annee, trimestre
""").fetchdf()

if not evo.empty:
    evo["date"] = evo.apply(
        lambda r: f"{int(r['annee'])}-{int((r['trimestre'] - 1) * 3 + 1):02d}-01", axis=1
    )
    evo["date"] = evo["date"].astype("datetime64[ns]")

    fig = px.line(
        evo,
        x="date",
        y="prix_m2_median",
        color="nom_commune",
        labels={
            "date": "Date",
            "prix_m2_median": "Prix médian (€/m²)",
            "nom_commune": "Commune",
        },
    )
    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

# ── Distribution ─────────────────────────────────────────────────────────────

st.subheader("Distribution des prix au m²")

distrib = con.execute(f"""
    SELECT nom_commune, prix_m2
    FROM mutations
    WHERE code_commune IN ({codes_sql})
      AND prix_m2 IS NOT NULL
      AND prix_m2 < 15000
      {type_filter}
""").fetchdf()

if not distrib.empty:
    fig2 = px.histogram(
        distrib,
        x="prix_m2",
        color="nom_commune",
        nbins=50,
        labels={"prix_m2": "Prix au m² (€)", "nom_commune": "Commune"},
        barmode="overlay",
        opacity=0.6,
    )
    st.plotly_chart(fig2, use_container_width=True)

con.close()
