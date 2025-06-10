import streamlit as st
import pandas as pd
import re
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import io

def update_click_url(original_url, click_tracker, campaign_name):
    """
    Updates the Click URL by prepending the click tracker and appending/updating UTM/TF parameters.
    """
    if pd.isna(original_url):
        original_url = "" # Ensure it's a string if NaN

    # Prepend click tracker if available
    if pd.notna(click_tracker):
        updated_url = click_tracker + original_url
    else:
        updated_url = original_url

    # Parse the URL to manipulate parameters
    parsed_url = urlparse(updated_url)
    query_params = parse_qs(parsed_url.query)

    # Define parameters to append/update
    params_to_add = {
        'utm_source': 'tiktok',
        'utm_medium': 'paid',
        'utm_campaign': campaign_name,
        'tf_source': 'tiktok',
        'tf_medium': 'paid_social',
        'tf_campaign': campaign_name,
    }

    # Append/update parameters
    for key, value in params_to_add.items():
        if key not in query_params:
            query_params[key] = [value]
        # If the parameter exists, ensure its value is correct.
        elif query_params[key][0] != value and key in ['utm_source', 'utm_medium', 'tf_source', 'tf_medium']:
            query_params[key] = [value]
        # For campaign, if it exists, update it to the specific campaign_name if it's different.
        elif (key == 'utm_campaign' or key == 'tf_campaign') and query_params[key][0] != value:
            query_params[key] = [value]

    # Reconstruct the query string
    new_query = urlencode(query_params, doseq=True)

    # Reconstruct the URL
    final_url = urlunparse(parsed_url._replace(query=new_query))

    return final_url

def extract_impression_url(impression_tracker_string):
    """
    Extracts the URL from within quotation marks in the impression tracker string.
    """
    if pd.isna(impression_tracker_string):
        return None
    # Regex to find content inside single or double quotes
    match = re.search(r'["\'](.*?)["\']', impression_tracker_string)
    if match:
        return match.group(1)
    return None

@st.cache_data
def process_files(tiktok_file_buffer, tag_file_buffer):
    """
    Core logic to process TikTok and Tag files, update URLs, and return the processed DataFrame.
    This function is cached for performance with Streamlit.
    """
    # --- Load Data ---
    # Determine file type for TikTok file and read accordingly
    if tiktok_file_buffer.name.endswith('.csv'):
        df_tiktok = pd.read_csv(tiktok_file_buffer)
    elif tiktok_file_buffer.name.endswith('.xlsx'):
        # For Excel, the sheet name is 'Ads'
        df_tiktok = pd.read_excel(tiktok_file_buffer, sheet_name='Ads')
    else:
        raise ValueError("Unsupported TikTok file format. Please upload a .csv or .xlsx file.")


    # Determine file type for Tag file and read accordingly
    if tag_file_buffer.name.endswith('.csv'):
        # Header is in row 11, so pandas header parameter should be 10 (0-indexed)
        df_tags = pd.read_csv(tag_file_buffer, header=10)
    elif tag_file_buffer.name.endswith('.xlsx'):
        # For Excel, the sheet name is 'Tracking Ads' and header is in row 11
        df_tags = pd.read_excel(tag_file_buffer, sheet_name='Tracking Ads', header=10)
    else:
        raise ValueError("Unsupported Tag file format. Please upload a .csv or .xlsx file.")


    # --- Preprocessing: Clean column names and ensure consistency ---
    # Strip whitespace from column names for robust matching
    df_tiktok.columns = df_tiktok.columns.str.strip()
    df_tags.columns = df_tags.columns.str.strip()

    # Ensure matching columns are treated as strings and fill NA for merging
    # TikTok columns
    df_tiktok['Campaign Name'] = df_tiktok['Campaign Name'].astype(str).fillna('')
    df_tiktok['Ad Group Name'] = df_tiktok['Ad Group Name'].astype(str).fillna('')
    df_tiktok['Ad Name'] = df_tiktok['Ad Name'].astype(str).fillna('')
    # IMPORTANT: Use 'Web URL' for TikTok file instead of 'Click URL'
    if 'Web URL' not in df_tiktok.columns:
        raise ValueError(
            f"Expected column 'Web URL' not found in the TikTok file. "
            f"Available columns are: {df_tiktok.columns.tolist()}"
        )


    # Tag file columns
    df_tags['Campaign Name'] = df_tags['Campaign Name'].astype(str).fillna('')
    df_tags['Placement Name'] = df_tags['Placement Name'].astype(str).fillna('')
    df_tags['Ad Name'] = df_tags['Ad Name'].astype(str).fillna('')

    # --- Identify Click Tracker and Impression Tracker columns ---
    # Directly use the exact column names provided by the user
    click_tracker_col = 'Click Tag'
    impression_tracker_col = 'Impression Tag (image)'

    # Validate that these columns exist in the DataFrame
    if click_tracker_col not in df_tags.columns:
        raise ValueError(
            f"Expected column '{click_tracker_col}' not found in the Tag file. "
            f"Available columns are: {df_tags.columns.tolist()}"
        )
    if impression_tracker_col not in df_tags.columns:
        raise ValueError(
            f"Expected column '{impression_tracker_col}' not found in the Tag file. "
            f"Available columns are: {df_tags.columns.tolist()}"
        )

    # --- Matching Logic: Merge DataFrames ---
    # Only select the identified tracker columns from df_tags
    merged_df = pd.merge(
        df_tiktok,
        df_tags[['Campaign Name', 'Placement Name', 'Ad Name', click_tracker_col, impression_tracker_col]],
        left_on=['Campaign Name', 'Ad Group Name', 'Ad Name'],
        right_on=['Campaign Name', 'Placement Name', 'Ad Name'],
        how='left',
        suffixes=('_tiktok', '_tag') # Suffixes to differentiate columns with same names
    )

    # --- Update Click URL (now 'Web URL' in tiktok side) ---
    # Apply the update_click_url function row-wise
    # Use the original 'Web URL' from TikTok and the dynamically found 'Click Tracker' from the merged data
    # Pass 'Campaign Name' from the TikTok side for parameter population
    merged_df['Web URL'] = merged_df.apply( # Changed from 'Click URL' to 'Web URL'
        lambda row: update_click_url(
            row['Web URL'], # Changed from 'Click URL' to 'Web URL'
            row[click_tracker_col],
            row['Campaign Name_tiktok']
        ),
        axis=1
    )

    # --- Update Impression tracking URL ---
    # Apply the extract_impression_url function row-wise
    merged_df['Impression tracking URL'] = merged_df.apply(
        lambda row: extract_impression_url(row[impression_tracker_col]),
        axis=1
    )

    # --- Final Output Preparation ---
    # Drop the temporary columns introduced by the merge from the tag file
    # We only want to keep the original TikTok columns updated
    columns_to_drop = [col for col in merged_df.columns if col.endswith('_tag') or col in [click_tracker_col, impression_tracker_col, 'Placement Name']]
    final_df = merged_df.drop(columns=columns_to_drop, errors='ignore')

    return final_df

# --- Streamlit App Interface ---
st.set_page_config(page_title="TikTok Tag Updater", layout="centered")

st.title("🔗 TikTok Tracking Tag Updater")
st.markdown("""
    Upload your TikTok Export file and DCM Tag file to update Click and Impression URLs
    based on matching Campaign, Ad Group/Placement, and Ad Names.
""")

# File Uploaders
tiktok_file = st.file_uploader(
    "Upload TikTok Export File (e.g., 'ExportAds_Test.xlsx - Ads.csv' or 'ExportAds_Test.xlsx')",
    type=["csv", "xlsx"]
)
tag_file = st.file_uploader(
    "Upload DCM Tag File (e.g., 'Tags_US-TF-AO-BRA-SS-Prospecting-Online_Video-TikTok-Awareness_Influencers_ReachAndFrequency_Marketing_0_PARENT_ADVERTISER.xlsx - Tracking Ads.csv' or 'Tags_US-TF-AO-BRA-SS-Prospecting-Online_Video-TikTok-Awareness_Influencers_ReachAndFrequency_Marketing_0_PARENT_ADVERTISER.xlsx')",
    type=["csv", "xlsx"]
)

if tiktok_file and tag_file:
    if st.button("Process Files"):
        with st.spinner("Processing files... This might take a moment."):
            try:
                updated_df = process_files(tiktok_file, tag_file)
                st.success("Files processed successfully!")

                # Provide download button for CSV
                csv_buffer = io.StringIO()
                updated_df.to_csv(csv_buffer, index=False)
                st.download_button(
                    label="Download Updated TikTok Ads CSV",
                    data=csv_buffer.getvalue(),
                    file_name="Updated_TikTok_Ads.csv",
                    mime="text/csv",
                    help="Click to download the updated TikTok Ads file in CSV format."
                )

                # Provide download button for Excel
                excel_buffer = io.BytesIO()
                updated_df.to_excel(excel_buffer, index=False, sheet_name='Updated Ads')
                excel_buffer.seek(0)
                st.download_button(
                    label="Download Updated TikTok Ads XLSX",
                    data=excel_buffer.getvalue(),
                    file_name="Updated_TikTok_Ads.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    help="Click to download the updated TikTok Ads file in XLSX format."
                )

                st.dataframe(updated_df.head())
            except ValueError as ve:
                st.error(f"File format or column error: {ve}")
                st.info("Please ensure you are uploading the correct file types (CSV or XLSX) and that the specified sheet names exist if uploading Excel files. Also check the exact column names in your tag file.")
            except Exception as e:
                st.error(f"An unexpected error occurred during processing: {e}")
                st.error("Please ensure your files are in the correct format and the correct sheets are selected.")
                st.info("Remember: TikTok export file has standard headers in row 1 (sheet 'Ads') with 'Web URL'. Tag files have headers in row 11 (sheet 'Tracking Ads') with 'Click Tag' and 'Impression Tag (image)'.")
else:
    st.info("Please upload both TikTok Export and DCM Tag files to proceed.")

st.markdown("---")
st.markdown("Developed with ❤️ for efficient ad tracking.")
