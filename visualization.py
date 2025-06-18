import streamlit as st
from streamlit_pdf_viewer import pdf_viewer

from extraction import extract


file1_path = "SUBSET-026-1 v360.pdf"
file2_path = "SUBSET-026-1 v400.pdf"

diff_list = extract(file1_path, file2_path)

# visualize PDF differences in two synced streamlit panes
st.title("PDF Comparison")
st.set_page_config(layout="wide")
col1, col2 = st.columns(2, gap="small")
with col1:
    st.header("Version 360")
    pdf_viewer(file1_path, width=700)

with col2:
    st.header("Version 400")
    pdf_viewer(file2_path, width=700)

st.divider()

col1_l, col2_l = st.columns(2, gap="small")
for i, (df1, df2) in enumerate(diff_list):
    with col1_l:
        st.subheader(f"Match {i + 1} - {len(df1)} lines")
        if df1 is not None:
            st.dataframe(df1, use_container_width=True)

    with col2_l:
        st.subheader(f"Match {i + 1} - {len(df2)} lines")
        if df2 is not None:
            st.dataframe(df2, use_container_width=True)
