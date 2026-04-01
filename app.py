import streamlit as st
import pandas as pd

# =========================
# 基础配置
# =========================
st.set_page_config(page_title="果熊采购发货系统", layout="wide")

SPREADSHEET_URL = st.secrets["connections"]["gsheets"]["spreadsheet"]

# =========================
# 连接
# =========================
@st.cache_resource
def get_conn():
    return st.connection("gsheets", type="gsheets")

# =========================
# 读取（带缓存）
# =========================
@st.cache_data(ttl=30)
def read_sheet(worksheet: str) -> pd.DataFrame:
    conn = get_conn()
    try:
        df = conn.read(
            spreadsheet=SPREADSHEET_URL,
            worksheet=worksheet
        )
        return pd.DataFrame(df)
    except Exception:
        return pd.DataFrame()

# =========================
# 写入（自动刷新缓存）
# =========================
def write_sheet(worksheet: str, df: pd.DataFrame):
    conn = get_conn()
    conn.update(
        spreadsheet=SPREADSHEET_URL,
        worksheet=worksheet,
        data=df
    )
    read_sheet.clear()

# =========================
# 数据读取
# =========================
def load_orders():
    return read_sheet("orders")

def load_items():
    return read_sheet("order_items")

# =========================
# 合并数据（缓存）
# =========================
@st.cache_data(ttl=30)
def combine_data_cached():
    orders = load_orders()
    items = load_items()

    if orders.empty or items.empty:
        return pd.DataFrame()

    df = items.merge(orders, on="单号", how="left")
    return df

def combine_data():
    return combine_data_cached()

# =========================
# UI
# =========================
st.title("果熊采购发货系统")
st.caption("订单录入 → 采购清单 → 到货登记 → 标签打印 → 发货登记")

# =========================
# 读取数据
# =========================
df = combine_data()

# =========================
# 展示
# =========================
st.subheader("数据预览")

if df.empty:
    st.warning("当前无数据，请先录入订单")
else:
    st.dataframe(df, use_container_width=True)

# =========================
# 测试写入按钮（你可以删）
# =========================
if st.button("测试写入一条数据"):
    orders = load_orders()

    new_row = pd.DataFrame([{
        "单号": "TEST001",
        "客户": "测试客户",
        "状态": "未采购"
    }])

    orders = pd.concat([orders, new_row], ignore_index=True)
    write_sheet("orders", orders)

    st.success("写入成功")
