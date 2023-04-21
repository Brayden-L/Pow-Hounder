import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, ColumnsAutoSizeMode
import streamlit_nested_layout
from ph_long_strs import blue_gradient_bg_img
from ph_general_funcs import pull_scrape_sample

st.markdown(blue_gradient_bg_img, unsafe_allow_html=True)

st.markdown("### Data is currently being collected to build a foundational model.")
st.markdown("See below for a recent sampling of scraped data")

df_lift_status, df_wind_log, df_snow_log = pull_scrape_sample()
if st.button("Refresh"):
    pull_scrape_sample()

st.markdown("### Lift Status")
st.dataframe(df_lift_status)
st.markdown("### Wind and Weather Characteristics")
st.dataframe(df_wind_log)
st.markdown("### Snowfall Record")
st.dataframe(df_snow_log)
