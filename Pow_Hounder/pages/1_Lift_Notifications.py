import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, ColumnsAutoSizeMode
import streamlit_nested_layout
import re
import datetime as dt
from ph_general_funcs import push_number, rem_number
from ph_long_strs import blue_gradient_bg_img

st.markdown(blue_gradient_bg_img, unsafe_allow_html=True)

col1, col2 = st.columns([1, 3])

with col1.expander("✋ Help ✋"):
    st.markdown(
        "* You may add your email using the form below to be notified of lift status changes for the requested time period. \n\n * You may also submit your email to remove yourself from notifications."
    )

user_meth_bool = col1.selectbox("Add or Remove Notications", options=["Add", "Remove"])

invalid_email_bool = False
user_email = col1.text_input(
    "email", placeholder="address@domain.com"
)

def is_valid_email(email):
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))
if not is_valid_email(user_email) and user_email !='':
    invalid_email_bool = True
    st.warning("Invalid Email Format")
if user_email == "":
    invalid_email_bool = True

if user_meth_bool == "Add":
    user_notif_date_range = col1.date_input(
        "Notification Period",
        value=[dt.datetime.today()],
        min_value=dt.datetime.today(),
        max_value=dt.datetime.today() + dt.timedelta(days=7),
        help="Max allowable duration is 7 days",
    )
    add_notif_bool = col1.button(
        "Request Notifications",
        disabled=(len(user_notif_date_range) != 2) or (invalid_email_bool),
    )
    if add_notif_bool:
        with st.spinner():
            push_number(user_email, user_notif_date_range)
            st.success("Notifications Activated")

if user_meth_bool == "Remove":
    rem_notif_bool = col1.button("Remove Notifications", disabled=invalid_email_bool)
    if rem_notif_bool:
        with st.spinner():
            rem_number(user_email)
            st.success("Notifications Removed")
