import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, ColumnsAutoSizeMode
import streamlit_nested_layout
from ph_long_strs import mnt_bg_img

st.set_page_config(layout="wide", page_title="Main Page")
st.markdown(mnt_bg_img, unsafe_allow_html=True)

col1, col2, col3 = st.columns(
    [1, 2, 1]
)  # This formatting is required since set_page_config can only be set once per app and we want this text centered
col2.title("Pow-Hounder")
col2.header("Mammoth Mountain Lift Status Tool")
col2.subheader("Always Be First Chair")
