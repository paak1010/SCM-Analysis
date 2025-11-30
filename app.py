import duckdb
import pandas as pd
import os
import streamlit as st
import altair as alt
import numpy as np

# --- 1. 환경 설정 ---
DB_FILE = 'scm.duckdb'
st.set_page_config(page_title="Smart SCM: 리스크 최적화", layout="wide", page_icon="📦")

TABLES_AND_CSVS = {
    'Suppliers': 'suppliers_data.csv',
    'Products': 'products_data.csv',
    'Customers': 'customers_data.csv',
    'Orders': 'orders_data.csv',
    'Order_Details': 'order_details_data.csv'
}

# --- 2. 데이터베이스 초기화 ---
def initialize_database():
    if os.path.exists(DB_FILE):
        return
    with st.spinner('시스템 초기화 중...'):
        try:
            conn = duckdb.connect(database=DB_FILE, read_only=False)
            for table_name, csv_file in TABLES_AND_CSVS.items():
                if os.path.exists(csv_file):
                    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_file}', header=True)")
            conn.close()
        except Exception as e:
            st.error(f"초기화 오류: {e}")

initialize_database()

# --- 3. 데이터 조회 ---
@st.cache_resource
def get_db_connection():
    try:
        return duckdb.connect(database=DB_FILE, read_only=True)
    except:
        return None

@st.cache_data
def get_product_list(_conn):
    return _conn.execute("SELECT ProductID, ProductName FROM Products ORDER BY ProductName").df()

@st.cache_data
def get_product_details(_conn, product_id):
    query = f"""
    SELECT p.ProductName, p.StockQuantity, p.SafetyStockLevel, p.UnitPrice,
           s.SupplierName, s.LeadTimeDays as ContractLeadTime
    FROM Products p JOIN Suppliers s ON p.SupplierID = s.SupplierID
    WHERE p.ProductID = {product_id};
    """
    details = _conn.execute(query).fetchone()
    if details:
        return {"name": details[0], "stock": details[1], "safety_stock": details[2],
                "price": details[3], "supplier": details[4], "contract_lead_time": details[5]}
    return None

# [수정] _conn 인자에 언더스코어를 붙여 캐시 해시 계산에서 제외시킵니다.
@st.cache_data
def analyze_risk(_conn, product_id):
    # [핵심] 실제 납기일 계산 (Shipped - Order)
    # BinderException 방지: CAST(... AS TIMESTAMP)를 사용하여 데이터 타입을 명확히 지정
    query = f"""
    SELECT o.OrderDate, o.ShippedDate,
           date_diff('day', CAST(o.OrderDate AS TIMESTAMP), CAST(o.ShippedDate AS TIMESTAMP)) as ActualLeadTime
    FROM Order_Details od JOIN Orders o ON od.OrderID = o.OrderID
    WHERE od.ProductID = {product_id} AND o.ShippedDate IS NOT NULL
    ORDER BY o.OrderDate;
    """
    try:
        df = _conn.execute(query).df()
        if df.empty: return None
        # 평균과 표준편차(변동성) 계산
        return {"avg": df['ActualLeadTime'].mean(), "std": df['ActualLeadTime'].std() if len(df)>1 else 0}
    except Exception as e:
        # 오류 발생 시 구체적인 메시지를 UI에 표시 (디버깅용)
        st.error(f"데이터 분석 오류 (analyze_risk): {e}")
        return None

@st.cache_data
def get_demand_data(_conn, product_id):
    query = f"""
    SELECT strftime(o.OrderDate, '%Y-%m') as Month, SUM(od.Quantity) as Qty
    FROM Order_Details od JOIN Orders o ON od.OrderID = o.OrderID
    WHERE od.ProductID = {product_id} GROUP BY Month ORDER BY Month
    """
    return _conn.execute(query).df()

# --- 4. [핵심] 리스크 분석 및 최적화 로직 ---
def run_optimization(sales_df, risk_data, details):
    daily_demand = sales_df['Qty'].mean() / 30.0
    
    # 리스크 요인 추출
    contract_lt = details['contract_lead_time']
    actual_lt = risk_data['avg']
    lt_variance = risk_data['std'] # 납기 변동성

    # [신뢰도 점수 로직]
    # 납기가 늦거나(delay), 들쭉날쭉하면(variance) 점수 깎임
    delay_penalty = max(0, actual_lt - contract_lt) * 10
    variance_penalty = lt_variance * 5
    score = max(0, 100 - (delay_penalty + variance_penalty))

    # [AI 안전재고 추천 로직]
    # Z값(1.65) * 변동성 * 수요
    rec_safety_stock = int((daily_demand * actual_lt) + (1.65 * lt_variance * daily_demand))
    rec_safety_stock = max(rec_safety_stock, int(daily_demand * 2))

    # 리스크 조정 ROP
    risk_adjusted_rop = (daily_demand * actual_lt) + rec_safety_stock

    return {
        "daily_demand": daily_demand, "score": score,
        "rec_safety_stock": rec_safety_stock, "rop": risk_adjusted_rop,
        "actual_lt": actual_lt, "variance": lt_variance
    }

# --- 5. UI 대시보드 ---
conn = get_db_connection()

if conn:
    st.sidebar.title("🚀 Smart SCM")
    st.sidebar.markdown("**데이터 기반 공급망 리스크 관리**")
    
    # [수정] 호출할 때도 _conn 인자를 명시적으로 전달하는 것이 좋습니다. (Streamlit 캐싱 동작 방식 때문)
    products = get_product_list(conn)
    selected_label = st.sidebar.selectbox("📦 분석 대상 제품", products['ProductName'] + " (ID:" + products['ProductID'].astype(str) + ")")
    pid = int(selected_label.split("ID:")[1].replace(")", ""))
    
    # 데이터 로드
    details = get_product_details(conn, pid)
    risk_data = analyze_risk(conn, pid)
    sales_data = get_demand_data(conn, pid)

    # 상단 정보
    st.title(f"{details['name']} 리스크 분석")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("현재 재고", f"{details['stock']}개")
    col2.metric("공급업체", details['supplier'])
    col3.metric("계약 납기", f"{details['contract_lead_time']}일")
    col4.metric("단가", f"${details['price']}")
    st.divider()

    if risk_data and not sales_data.empty:
        res = run_optimization(sales_data, risk_data, details)
        
        # 탭 1: 리스크 진단 (여기에 빨간색 점수와 경고가 나옵니다!)
        st.subheader("1️⃣ 공급업체 신뢰도 평가")
        
        score = res['score']
        # 점수에 따라 색상 결정 (60점 미만이면 빨간색)
        color = "red" if score < 60 else "orange" if score < 80 else "green"
        
        c1, c2 = st.columns([1, 2])
        
        # [신뢰도 점수 카드]
        with c1:
            st.markdown(f"""
                <div style="text-align: center; border: 2px solid {color}; padding: 20px; border-radius: 10px;">
                    <h2 style="color: {color}; margin:0;">{score:.0f}점</h2>
                    <p style="margin:0;">신뢰도 점수</p>
                </div>
            """, unsafe_allow_html=True)
            
            # [경고 메시지] 점수가 낮으면 경고 출력
            if score < 80:
                delay_days = res['actual_lt'] - details['contract_lead_time']
                st.error(f"⚠️ **위험 감지**: 약속보다 평균 **{delay_days:.1f}일** 지연되고 있습니다.")
        
        # [비교 차트]
        with c2:
            chart_data = pd.DataFrame({
                'Type': ['계약 납기', '실제 납기(평균)'],
                'Days': [details['contract_lead_time'], res['actual_lt']]
            })
            c = alt.Chart(chart_data).mark_bar().encode(
                x='Days', y=alt.Y('Type', title=None),
                color=alt.Color('Type', scale=alt.Scale(range=['gray', color]), legend=None)
            ).properties(height=150)
            st.altair_chart(c, use_container_width=True)

        st.divider()

        # 탭 2: 최적화 제안
        st.subheader("2️⃣ 재고 최적화 제안")
        
        m1, m2, m3 = st.columns(3)
        m1.metric("기존 설정 안전재고", f"{details['safety_stock']}개")
        m2.metric("AI 제안 안전재고", f"{res['rec_safety_stock']}개", f"{res['rec_safety_stock'] - details['safety_stock']}개 조정")
        
        cost = (details['safety_stock'] - res['rec_safety_stock']) * details['price']
        if cost > 0:
            m3.metric("예상 절감 비용", f"${cost:,.0f}")
            st.success("💡 현재 재고가 과다합니다. 안전재고를 줄이세요.")
        elif cost < 0:
            m3.metric("추가 투자 필요", f"${abs(cost):,.0f}")
            st.error("🚨 품절 위험이 높습니다. 안전재고를 늘리세요.")
        else:
            m3.metric("상태", "최적")
            st.info("현재 설정이 최적입니다.")

        # 탭 3: 시뮬레이션
        st.subheader("3️⃣ 미래 재고 시뮬레이션")
        days = range(30)
        stock_flow = [max(0, details['stock'] - (res['daily_demand'] * d)) for d in days]
        sim_df = pd.DataFrame({'Day': days, 'Stock': stock_flow})
        
        line = alt.Chart(sim_df).mark_line().encode(x='Day', y='Stock')
        rule = alt.Chart(pd.DataFrame({'y': [res['rop']]})).mark_rule(color='red', strokeDash=[5,5]).encode(y='y')
        
        st.altair_chart(line + rule, use_container_width=True)

    else:
        st.warning("분석할 데이터가 부족합니다.")
