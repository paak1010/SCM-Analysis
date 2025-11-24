import duckdb
import pandas as pd
import os
import streamlit as st
import altair as alt

# --- 1. Configuration ---
DB_FILE = 'scm.duckdb'

# CSV files needed to create the DB
# These must be in the GitHub repository
TABLES_AND_CSVS = {
    'Suppliers': 'suppliers_data.csv',
    'Products': 'products_data.csv',
    'Customers': 'customers_data.csv',
    'Orders': 'orders_data.csv',
    'Order_Details': 'order_details_data.csv'
}

# --- 2. Database Initialization (Crucial for Streamlit Cloud) ---
def initialize_database():
    """
    Checks if the DuckDB file exists. If not, creates it from CSVs.
    This runs ONCE when the Streamlit app starts on the server.
    """
    if os.path.exists(DB_FILE):
        return # DB file already exists

    print("--- Database not found. Creating from CSV files... ---")
    try:
        conn = duckdb.connect(database=DB_FILE, read_only=False)
        
        for table_name, csv_file in TABLES_AND_CSVS.items():
            if not os.path.exists(csv_file):
                # This error will show in the Streamlit logs
                print(f"Error: Missing required file: {csv_file}")
                st.error(f"Fatal Error: Missing CSV file {csv_file}. App cannot start.")
                return
            
            # Create table from CSV
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_file}', header=True)")
            print(f"Successfully created table: {table_name}")

        print("--- Database initialization complete. ---")
        conn.close()

    except Exception as e:
        print(f"Error during DB initialization: {e}")
        st.error(f"Database creation failed: {e}")

# Run the initialization ONCE at the start
initialize_database()


# --- 3. Database Connection & Data Fetching (with Caching) ---

# Use st.cache_resource to cache the database connection
@st.cache_resource
def get_db_connection():
    """Gets a cached connection to the DuckDB file."""
    try:
        conn = duckdb.connect(database=DB_FILE, read_only=True)
        return conn
    except Exception as e:
        st.error(f"Failed to connect to DuckDB: {e}")
        return None

# Use st.cache_data to cache the results of data queries
@st.cache_data
def get_all_products(_conn):
    """Fetches all product names and IDs for the selector."""
    try:
        products_df = _conn.execute("SELECT ProductID, ProductName FROM Products ORDER BY ProductName").df()
        return products_df
    except Exception as e:
        st.error(f"Error fetching product list: {e}")
        return pd.DataFrame(columns=["ProductID", "ProductName"])

@st.cache_data
def get_sales_history(_conn, product_id):
    """Fetches and aggregates monthly sales for a specific product."""
    query = f"""
    SELECT 
        strftime(o.OrderDate, '%Y-%m') AS SalesMonth,
        SUM(od.Quantity) AS TotalQuantity
    FROM Order_Details od
    JOIN Orders o ON od.OrderID = o.OrderID
    WHERE od.ProductID = {product_id}
    GROUP BY SalesMonth
    ORDER BY SalesMonth;
    """
    sales_df = _conn.execute(query).df()
    # Ensure SalesMonth is a datetime object for charting
    if not sales_df.empty:
        sales_df['SalesMonth'] = pd.to_datetime(sales_df['SalesMonth'])
    return sales_df

@st.cache_data
def get_product_analysis_details(_conn, product_id):
    """Fetches all details needed for ROP calculation."""
    query = f"""
    SELECT 
        p.ProductName,
        p.StockQuantity,
        p.SafetyStockLevel,
        s.LeadTimeDays
    FROM Products p
    JOIN Suppliers s ON p.SupplierID = s.SupplierID
    WHERE p.ProductID = {product_id};
    """
    details = _conn.execute(query).fetchone()
    if details:
        return {
            "name": details[0],
            "stock": details[1],
            "safety_stock": details[2],
            "lead_time": details[3]
        }
    return None

# --- 4. Analysis & Forecasting Logic ---

def calculate_rop(sales_df, details):
    """Calculates ROP and provides analysis."""
    if sales_df.empty:
        return 0, 0, "판매 이력 없음"

    avg_monthly_sales = sales_df['TotalQuantity'].mean()
    avg_daily_demand = avg_monthly_sales / 30.0
    
    lead_time = details["lead_time"]
    safety_stock = details["safety_stock"]
    
    demand_during_lead_time = avg_daily_demand * lead_time
    reorder_point = demand_during_lead_time + safety_stock
    
    return avg_daily_demand, reorder_point, "분석 완료"


# --- 5. Streamlit App UI ---

# Set page title and layout
st.set_page_config(page_title="SCM 재고 관리 대시보드", layout="wide")

# Get DB connection
conn = get_db_connection()

if conn is None:
    st.error("데이터베이스 연결에 실패했습니다. 앱을 재시작해주세요.")
else:
    # --- Sidebar ---
    st.sidebar.title("SCM Dashboard")
    st.sidebar.image("https://placehold.co/400x200/06B6D4/FFFFFF?text=SCM+Model", use_column_width=True)
    
    product_list_df = get_all_products(conn)
    
    # Create a mapping from "Name (ID)" to just ID
    product_options = {f"{row.ProductName} (ID: {row.ProductID})": row.ProductID for index, row in product_list_df.iterrows()}
    
    selected_option = st.sidebar.selectbox(
        "분석할 제품을 선택하세요:",
        options=list(product_options.keys())
    )
    
    # Get the ID from the selected option
    selected_product_id = product_options[selected_option]

    # --- Main Page ---
    st.title(f"📈 SCM 수요 예측 및 재고 분석")
    st.markdown(f"현재 선택된 제품: **{selected_option}**")
    
    # --- Fetch data for the selected product ---
    details = get_product_analysis_details(conn, selected_product_id)
    sales_history_df = get_sales_history(conn, selected_product_id)
    
    if details is None:
        st.error("제품 상세 정보를 불러오는 데 실패했습니다.")
    else:
        # --- Run analysis ---
        avg_daily_demand, reorder_point, status = calculate_rop(sales_history_df, details)
        
        # --- Display Key Metrics ---
        st.header("📊 핵심 재고 지표 (KPIs)")
        col1, col2, col3 = st.columns(3)
        col1.metric("현재 재고 (Stock)", f"{details['stock']} 개")
        col2.metric("안전 재고 (Safety Stock)", f"{details['safety_stock']} 개")
        col3.metric("공급자 리드타임 (Lead Time)", f"{details['lead_time']} 일")

        st.divider()

        # --- Display Analysis Result ---
        st.header("💡 분석 결과: 재주문점 (ROP)")
        
        col_rop, col_demand = st.columns(2)
        col_rop.metric("계산된 재주문점 (Reorder Point)", f"{reorder_point:.1f} 개")
        col_demand.metric("예측 일평균 수요 (Daily Demand)", f"{avg_daily_demand:.1f} 개/일")

        # --- Final Verdict ---
        current_stock = details['stock']
        if current_stock < reorder_point:
            st.error(f"**[조치 필요]** 현재 재고({current_stock})가 재주문점({reorder_point:.1f})보다 낮습니다. **즉시 발주가 필요합니다!**")
        else:
            st.success(f"**[양호]** 현재 재고({current_stock})가 재주문점({reorder_point:.1f})보다 많습니다. 재고가 충분합니다.")

        # --- Display Sales History Chart ---
        st.header("📉 과거 판매 이력 (월별)")
        if not sales_history_df.empty:
            # Create an Altair chart
            chart = alt.Chart(sales_history_df).mark_bar(color="#06B6D4").encode(
                x=alt.X('SalesMonth', title='월'),
                y=alt.Y('TotalQuantity', title='총 판매량'),
                tooltip=['SalesMonth', 'TotalQuantity']
            ).properties(
                title=f"{details['name']} 월별 판매량"
            ).interactive()
            
            st.altair_chart(chart, use_container_width=True)
        else:
            st.warning("이 제품은 아직 판매 이력이 없습니다.")