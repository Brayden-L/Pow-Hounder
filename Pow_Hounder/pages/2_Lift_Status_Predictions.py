import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, ColumnsAutoSizeMode
import streamlit_nested_layout
from ph_long_strs import blue_gradient_bg_img

st.markdown(blue_gradient_bg_img, unsafe_allow_html=True)

st.markdown("Data is currently being collected to build a foundational model.")
