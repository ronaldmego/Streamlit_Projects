import streamlit as st

st.set_page_config(
    page_title="Welcome",
    page_icon="📊",
)

st.write("# Welcome to the MFS Data Team Hub! 📊")

st.sidebar.success("Select a report above.")

st.markdown(
    """
    ### Welcome to the MFS Data Team Hub! Our reports are seamlessly connected to Redshift.

    **👈 Select a report from the sidebar** to explore our latest analytics and visualizations.
    ### Want to learn more about our news?
    - Explore [MFS Data Team](https://millicom.sharepoint.com/sites/MFS-DataTeam)

    ### Discover the Lakehouse project
    - Project [Github](https://github.com/Millicom-MFS/mfs-data-lakehouse)
"""
)