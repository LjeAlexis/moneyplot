"""Moneyplot dashboard — Streamlit entry point."""

from pathlib import Path

import streamlit as st

from moneyplot.storage.db import get_connection


def main():
    st.set_page_config(
        page_title="Moneyplot",
        page_icon="\U0001f3e0",
        layout="wide",
    )
    st.title("Moneyplot — Suivi des prix immobiliers")
    st.markdown(
        "Explorez les prix de l'immobilier en France "
        "à partir des transactions réelles (DVF)."
    )

    # Quick stats on the sidebar
    try:
        con = get_connection()
        total = con.execute("SELECT count(*) FROM mutations").fetchone()[0]
        st.sidebar.metric("Transactions en base", f"{total:,.0f}")

        date_range = con.execute(
            "SELECT min(date_mutation), max(date_mutation) FROM mutations"
        ).fetchone()
        if date_range[0]:
            st.sidebar.caption(f"Période : {date_range[0]} → {date_range[1]}")
        con.close()
    except Exception:
        st.sidebar.warning("Base de données non initialisée. Lancez le pipeline Dagster.")


if __name__ == "__main__":
    main()
