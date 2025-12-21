import os
import psycopg2
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DB_URL")

def get_connection():
    return psycopg2.connect(DB_URL)

st.set_page_config(page_title="Reminder Manager", layout="wide")
st.title("🔔 Reminder Database Editor")

# --- EMERGENCY RESET BUTTON ---
with st.sidebar:
    st.warning("Database Connection Management")
    if st.button("🔴 Kill All Active Connections"):
        try:
            conn = psycopg2.connect(DB_URL)
            conn.autocommit = True
            with conn.cursor() as cur:
                kill_query = """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = 'MyDatabase1_pickthrew'
                  AND pid <> pg_backend_pid();
                """
                cur.execute(kill_query)
                st.sidebar.success("All other connections closed!")
            conn.close()
        except Exception as e:
            st.sidebar.error(f"Could not reset: {e}")

# 1. Load Data (Including last_completed_at)
@st.cache_data(show_spinner=False)
def load_data():
    conn = get_connection()
    query = """
    SELECT id, reminder, activated, chat_id, frequency, 
           day_of_week, day_value, month_value, year_value, 
           hour_value, minute_value, last_completed_at
    FROM my_schema_1.reminders 
    ORDER BY id ASC;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

if "df" not in st.session_state:
    st.session_state.df = load_data()

# 2. The Data Editor
st.subheader("Edit Reminders")
edited_data = st.data_editor(
    st.session_state.df,
    num_rows="dynamic",
    column_config={
        "id": st.column_config.NumberColumn("ID", disabled=True),
        "activated": st.column_config.CheckboxColumn("Active"),
        "reminder": st.column_config.TextColumn("Message", required=True),
        "chat_id": st.column_config.TextColumn("Telegram Chat ID"),
        "frequency": st.column_config.SelectboxColumn(
            "Frequency", 
            options=["daily", "weekly", "monthly", "yearly", "once"]
        ),
        "day_of_week": st.column_config.SelectboxColumn(
            "Weekday", 
            options=["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        ),
        "day_value": st.column_config.NumberColumn("Day (1-31)", min_value=1, max_value=31),
        "month_value": st.column_config.NumberColumn("Month (1-12)", min_value=1, max_value=12),
        "year_value": st.column_config.NumberColumn("Year (YYYY)"),
        "hour_value": st.column_config.NumberColumn("Hour (0-23)", min_value=0, max_value=23),
        "minute_value": st.column_config.NumberColumn("Minute (0-59)", min_value=0, max_value=59),
        "last_completed_at": st.column_config.DatetimeColumn("Last completed at"),
    },
    key="editor_key",
    use_container_width=True
)

# 3. Handle Changes
if st.button("Save Changes"):
    changes = st.session_state.editor_key
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # Handle UPDATES
        for row_index, updated_values in changes["edited_rows"].items():
            row_id = st.session_state.df.iloc[row_index]["id"]
            for col, val in updated_values.items():
                sql = f"UPDATE my_schema_1.reminders SET {col} = %s WHERE id = %s"
                cur.execute(sql, (val, int(row_id)))

        # Handle DELETIONS
        for row_index in changes["deleted_rows"]:
            row_id = st.session_state.df.iloc[row_index]["id"]
            cur.execute("DELETE FROM my_schema_1.reminders WHERE id = %s", (int(row_id),))

        # Handle ADDITIONS
        for row in changes["added_rows"]:
            cur.execute(
                """
                INSERT INTO my_schema_1.reminders 
                (reminder, activated, chat_id, frequency, day_of_week, 
                 day_value, month_value, year_value, hour_value, minute_value, last_completed_at) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row.get("reminder", "New Reminder"), 
                    row.get("activated", True),
                    row.get("chat_id"),
                    row.get("frequency", "daily"),
                    row.get("day_of_week"),
                    row.get("day_value"),
                    row.get("month_value"),
                    row.get("year_value"),
                    row.get("hour_value", 9),
                    row.get("minute_value", 0),
                    None # last_completed_at starts as Null for new entries
                )
            )

        conn.commit()
        st.success("Successfully updated database!")
        st.cache_data.clear()
        st.session_state.df = load_data()
        st.rerun()

    except Exception as e:
        conn.rollback()
        st.error(f"Error: {e}")
    finally:
        cur.close()
        conn.close()