import pandas as pd
import streamlit as st
from datetime import date, datetime
from streamlit_gsheets import GSheetsConnection

st.set_page_config(page_title="果熊采购发货系统", page_icon="📦", layout="wide")


# =========================
# 连接 Google Sheet
# =========================
@st.cache_resource
def get_conn():
    return st.connection("gsheets", type=GSheetsConnection)


@st.cache_data(ttl="10m")
def read_sheet(worksheet: str) -> pd.DataFrame:
    conn = get_conn()
    try:
        df = conn.read(worksheet=worksheet, ttl="0")
        if df is None:
            return pd.DataFrame()
        return pd.DataFrame(df)
    except Exception:
        return pd.DataFrame()


def write_sheet(worksheet: str, df: pd.DataFrame):
    conn = get_conn()
    conn.update(worksheet=worksheet, data=df)
    read_sheet.clear()
    combine_data.clear()


# =========================
# 工具函数
# =========================
def today_str():
    return date.today().isoformat()


def now_str():
    return datetime.now().isoformat(timespec="seconds")


def safe_str(v):
    if pd.isna(v):
        return ""
    return str(v).strip()


def safe_int(v, default=0):
    try:
        if pd.isna(v) or v == "":
            return default
        return int(float(v))
    except Exception:
        return default


def item_label_text(row):
    brand = safe_str(row.get("brand"))
    model = safe_str(row.get("model"))
    color = safe_str(row.get("color"))
    size = safe_str(row.get("size"))

    item = " ".join([x for x in [brand, model] if x]).strip()
    spec = " / ".join([x for x in [color, size] if x])

    if item and spec:
        return f"{item} / {spec}"
    return item or spec


def grouped_label_text(customer_name, items_df, max_lines=4):
    """
    70x40 标签紧凑版：
    1 行客户名 + 最多 3 行商品信息
    """
    lines = [safe_str(customer_name)]

    compact_lines = []
    for _, row in items_df.iterrows():
        brand = safe_str(row.get("brand"))
        model = safe_str(row.get("model"))
        color = safe_str(row.get("color"))
        size = safe_str(row.get("size"))
        qty = safe_int(row.get("qty"), 1)

        main = " ".join([x for x in [brand, model] if x]).strip()
        spec = " / ".join([x for x in [color, size] if x])

        if main:
            compact_lines.append(main)
        if spec:
            compact_lines.append(f"{spec} ×{qty}")

    # 70x40 不适合太多内容：客户名占 1 行，商品最多再占 3 行
    if len(compact_lines) > max_lines - 1:
        compact_lines = compact_lines[:max_lines - 1]

    lines.extend(compact_lines)
    return "\n".join(lines)


# =========================
# 数据加载
# =========================
def load_orders():
    cols = ["order_no", "order_date", "customer_name", "source", "remark", "created_at"]
    df = read_sheet("orders")
    if df.empty:
        return pd.DataFrame(columns=cols)

    for c in cols:
        if c not in df.columns:
            df[c] = ""

    return df[cols].fillna("")


def load_items():
    cols = [
        "item_id", "order_no", "brand", "model", "color", "size", "qty", "reserved",
        "purchased", "purchase_store", "purchase_date", "arrived", "arrival_date",
        "printed", "shipped", "shipped_date", "tracking_no", "note"
    ]
    df = read_sheet("order_items")
    if df.empty:
        return pd.DataFrame(columns=cols)

    for c in cols:
        if c not in df.columns:
            df[c] = ""

    df = df[cols].fillna("")

    for c in ["item_id", "qty", "reserved", "purchased", "arrived", "printed", "shipped"]:
        df[c] = df[c].apply(lambda x: safe_int(x, 0))

    return df


def save_orders(df):
    write_sheet("orders", df.fillna(""))


def save_items(df):
    write_sheet("order_items", df.fillna(""))


def gen_order_no(orders_df):
    today = today_str()
    seq = int((orders_df["order_date"] == today).sum()) + 1 if not orders_df.empty else 1
    return f"{date.today().strftime('%Y%m%d')}-{seq:03d}"


def gen_next_item_id(items_df):
    if items_df.empty:
        return 1
    return int(items_df["item_id"].max()) + 1


@st.cache_data(ttl="10m")
def combine_data():
    orders = load_orders()
    items = load_items()

    if orders.empty or items.empty:
        return pd.DataFrame(columns=[
            "order_no", "order_date", "customer_name", "source", "remark",
            "item_id", "brand", "model", "color", "size", "qty", "reserved",
            "purchased", "purchase_store", "purchase_date", "arrived",
            "arrival_date", "printed", "shipped", "shipped_date",
            "tracking_no", "note"
        ])

    # 关键修复：统一 order_no 格式
    orders["order_no"] = orders["order_no"].astype(str).str.strip()
    items["order_no"] = items["order_no"].astype(str).str.strip()

    df = items.merge(orders, on="order_no", how="left")

    base = ["order_no", "order_date", "customer_name", "source", "remark"]
    others = [c for c in df.columns if c not in base]

    return df[base + others].fillna("")


def fetch_dashboard_metrics(df):
    if df.empty:
        return {
            "待采购": 0,
            "已采购未到货": 0,
            "已到货待发货": 0,
            "今日已发货": 0,
        }

    today = today_str()
    return {
        "待采购": int(((df["reserved"] == 1) & (df["purchased"] == 0)).sum()),
        "已采购未到货": int(((df["purchased"] == 1) & (df["arrived"] == 0)).sum()),
        "已到货待发货": int(((df["arrived"] == 1) & (df["shipped"] == 0)).sum()),
        "今日已发货": int(((df["shipped"] == 1) & (df["shipped_date"].astype(str) == today)).sum()),
    }


def download_df(df, filename, label):
    st.download_button(
        label=label,
        data=df.to_csv(index=False).encode("utf-8-sig"),
        file_name=filename,
        mime="text/csv",
        use_container_width=True,
    )


# =========================
# 页面
# =========================
def page_dashboard(df):
    st.subheader("首页仪表盘")
    metrics = fetch_dashboard_metrics(df)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("待采购", metrics["待采购"])
    c2.metric("已采购未到货", metrics["已采购未到货"])
    c3.metric("已到货待发货", metrics["已到货待发货"])
    c4.metric("今日已发货", metrics["今日已发货"])

    st.markdown("### 今日提醒")
    if df.empty:
        st.info("还没有订单，先去“订单录入”添加第一批数据。")
        return

    pending_ship = df[(df["arrived"] == 1) & (df["shipped"] == 0)]
    if pending_ship.empty:
        st.success("目前没有待发货商品。")
    else:
        grouped = (
            pending_ship.groupby("customer_name")
            .size()
            .reset_index(name="件数")
            .sort_values("件数", ascending=False)
        )
        st.dataframe(grouped, use_container_width=True, hide_index=True)


def page_order_entry():
    st.subheader("订单录入")

    with st.form("order_form", clear_on_submit=True):
        c1, c2, c3 = st.columns([1.2, 1, 1.2])
        order_date = c1.date_input("下单日期", value=date.today())
        customer_name = c2.text_input("客户姓名")
        source = c3.selectbox("来源", ["微信", "淘宝", "得物", "官网", "其他"])
        remark = st.text_input("订单备注")

        st.markdown("#### 商品列表")
        empty = pd.DataFrame([
            {"品牌": "", "型号": "", "颜色": "", "尺寸": "", "数量": 1, "已预订": True, "备注": "", "采购店铺": ""}
        ])

        items = st.data_editor(
            empty,
            num_rows="dynamic",
            use_container_width=True,
            hide_index=True,
            key="order_editor",
            column_config={
                "数量": st.column_config.NumberColumn(min_value=1, step=1, default=1),
                "已预订": st.column_config.CheckboxColumn(default=True),
            },
        )

        submitted = st.form_submit_button("保存订单", use_container_width=True)

    if submitted:
        if not customer_name.strip():
            st.error("客户姓名不能为空。")
            return

        valid_rows = [row for _, row in items.iterrows() if safe_str(row.get("型号"))]
        if not valid_rows:
            st.error("至少填写一行商品型号。")
            return

        orders_df = load_orders()
        items_df = load_items()

        order_no = gen_order_no(orders_df)

        new_order = pd.DataFrame([{
            "order_no": order_no,
            "order_date": order_date.isoformat(),
            "customer_name": customer_name.strip(),
            "source": source,
            "remark": remark.strip(),
            "created_at": now_str(),
        }])
        orders_df = pd.concat([orders_df, new_order], ignore_index=True)

        next_id = gen_next_item_id(items_df)
        rows = []
        for row in valid_rows:
            rows.append({
                "item_id": next_id,
                "order_no": order_no,
                "brand": safe_str(row.get("品牌")),
                "model": safe_str(row.get("型号")),
                "color": safe_str(row.get("颜色")),
                "size": safe_str(row.get("尺寸")),
                "qty": safe_int(row.get("数量"), 1),
                "reserved": 1 if bool(row.get("已预订")) else 0,
                "purchased": 0,
                "purchase_store": safe_str(row.get("采购店铺")),
                "purchase_date": "",
                "arrived": 0,
                "arrival_date": "",
                "printed": 0,
                "shipped": 0,
                "shipped_date": "",
                "tracking_no": "",
                "note": safe_str(row.get("备注")),
            })
            next_id += 1

        items_df = pd.concat([items_df, pd.DataFrame(rows)], ignore_index=True)

        save_orders(orders_df)
        save_items(items_df)
        st.success(f"订单已保存：{order_no}")
        st.rerun()


def page_purchase(df):
    st.subheader("采购清单")
    purchase_df = df[(df["reserved"] == 1) & (df["purchased"] == 0)].copy()

    if purchase_df.empty:
        st.success("当前没有待采购商品。")
        return

    purchase_df["选择"] = False
    display_cols = ["item_id", "order_no", "customer_name", "brand", "model", "color", "size", "qty", "purchase_store"]

    edited = st.data_editor(
        purchase_df[["选择"] + display_cols],
        use_container_width=True,
        hide_index=True,
        disabled=display_cols,
        key="purchase_editor",
    )

    with st.form("mark_purchased_form"):
        purchase_date = st.date_input("采购日期", value=date.today())
        submitted = st.form_submit_button("标记为已采购", use_container_width=True)

    if submitted:
        selected_ids = edited.loc[edited["选择"] == True, "item_id"].tolist()
        if not selected_ids:
            st.warning("先勾选要处理的商品。")
            return

        items_df = load_items()
        items_df.loc[items_df["item_id"].isin(selected_ids), "purchased"] = 1
        items_df.loc[items_df["item_id"].isin(selected_ids), "purchase_date"] = purchase_date.isoformat()
        save_items(items_df)
        st.success(f"已标记 {len(selected_ids)} 件商品为已采购。")
        st.rerun()


def page_arrival(df):
    st.subheader("到货登记")
    arrival_df = df[(df["purchased"] == 1) & (df["arrived"] == 0)].copy()

    if arrival_df.empty:
        st.success("当前没有待到货商品。")
        return

    arrival_df["选择"] = False
    display_cols = ["item_id", "order_no", "customer_name", "brand", "model", "color", "size", "qty", "purchase_store", "purchase_date"]

    edited = st.data_editor(
        arrival_df[["选择"] + display_cols],
        use_container_width=True,
        hide_index=True,
        disabled=display_cols,
        key="arrival_editor",
    )

    with st.form("arrival_form"):
        arrival_date = st.date_input("到货日期", value=date.today())
        submitted = st.form_submit_button("标记为已到货", use_container_width=True)

    if submitted:
        selected_ids = edited.loc[edited["选择"] == True, "item_id"].tolist()
        if not selected_ids:
            st.warning("先勾选要处理的商品。")
            return

        items_df = load_items()
        items_df.loc[items_df["item_id"].isin(selected_ids), "arrived"] = 1
        items_df.loc[items_df["item_id"].isin(selected_ids), "arrival_date"] = arrival_date.isoformat()
        save_items(items_df)
        st.success(f"已标记 {len(selected_ids)} 件商品为已到货。")
        st.rerun()


def page_labels(df):
    st.subheader("标签打印")
    ready_df = df[(df["arrived"] == 1) & (df["shipped"] == 0)].copy()

    if ready_df.empty:
        st.success("当前没有可打印标签的商品。")
        return

    mode = st.radio("标签模式", ["同客户合并标签", "单件标签"], horizontal=True)

    if mode == "单件标签":
        ready_df["标签内容"] = ready_df.apply(lambda r: f"{r['customer_name']}\n{item_label_text(r)}", axis=1)
        st.dataframe(
            ready_df[["item_id", "order_no", "customer_name", "brand", "model", "color", "size", "标签内容"]],
            use_container_width=True,
            hide_index=True,
        )

        selected_item = st.selectbox(
            "选择一件商品查看标签",
            ready_df["item_id"].tolist(),
            format_func=lambda x: f"{x} | {ready_df.loc[ready_df['item_id'] == x, 'customer_name'].iloc[0]} | {ready_df.loc[ready_df['item_id'] == x, 'model'].iloc[0]}",
        )

        row = ready_df[ready_df["item_id"] == selected_item].iloc[0]
        label_text = row["标签内容"]

        st.text_area("标签内容", value=label_text, height=140)
        st.code(label_text)

        if st.button("标记此商品已打印", use_container_width=True):
            items_df = load_items()
            items_df.loc[items_df["item_id"] == int(selected_item), "printed"] = 1
            save_items(items_df)
            st.success("已标记为已打印。")
            st.rerun()

    else:
        st.markdown("### 70×40 标签预览")

        grouped_records = []
        for customer_name, g in ready_df.groupby("customer_name", sort=False):
            label_text = grouped_label_text(customer_name, g, max_lines=4)
            grouped_records.append({
                "客户姓名": customer_name,
                "商品数": len(g),
                "标签内容": label_text,
                "item_ids": ",".join([str(x) for x in g["item_id"].tolist()]),
            })

        grouped_df = pd.DataFrame(grouped_records)

        if grouped_df.empty:
            st.info("当前没有可生成的标签。")
            return

        for i, row in grouped_df.iterrows():
            with st.container(border=True):
                c1, c2 = st.columns([3, 1])

                with c1:
                    st.markdown(f"**{row['客户姓名']}**")
                    st.text_area(
                        f"标签内容_{i}",
                        value=row["标签内容"],
                        height=110,
                        key=f"label_preview_{i}"
                    )

                with c2:
                    st.write(f"商品数：{row['商品数']}")
                    if st.button("标记已打印", key=f"mark_printed_{i}", use_container_width=True):
                        ids = [int(x) for x in row["item_ids"].split(",") if x]
                        items_df = load_items()
                        items_df.loc[items_df["item_id"].isin(ids), "printed"] = 1
                        save_items(items_df)
                        st.success(f"{row['客户姓名']} 已标记打印")
                        st.rerun()

        download_df(
            grouped_df[["客户姓名", "商品数", "标签内容"]],
            "70x40标签内容.csv",
            "下载 70×40 标签内容 CSV"
        )


def page_shipping(df):
    st.subheader("发货登记")
    ship_df = df[(df["arrived"] == 1) & (df["shipped"] == 0)].copy()

    if ship_df.empty:
        st.success("当前没有待发货商品。")
        return

    ship_df["选择"] = False
    display_cols = ["item_id", "order_no", "customer_name", "brand", "model", "color", "size", "printed"]

    edited = st.data_editor(
        ship_df[["选择"] + display_cols],
        use_container_width=True,
        hide_index=True,
        disabled=display_cols,
        key="shipping_editor",
    )

    with st.form("shipping_form"):
        c1, c2 = st.columns(2)
        tracking_no = c1.text_input("物流单号")
        shipped_date = c2.date_input("发货日期", value=date.today())
        submitted = st.form_submit_button("标记为已发货", use_container_width=True)

    if submitted:
        selected_ids = edited.loc[edited["选择"] == True, "item_id"].tolist()
        if not selected_ids:
            st.warning("先勾选要发货的商品。")
            return

        items_df = load_items()
        items_df.loc[items_df["item_id"].isin(selected_ids), "shipped"] = 1
        items_df.loc[items_df["item_id"].isin(selected_ids), "shipped_date"] = shipped_date.isoformat()
        items_df.loc[items_df["item_id"].isin(selected_ids), "tracking_no"] = tracking_no.strip()
        save_items(items_df)
        st.success(f"已标记 {len(selected_ids)} 件商品为已发货。")
        st.rerun()


def page_data(df):
    st.subheader("数据总览 / 维护")

    if df.empty:
        st.info("暂无数据。")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

    if st.button("生成示例数据", use_container_width=True):
        orders_df = load_orders()
        items_df = load_items()

        base_order_no = gen_order_no(orders_df)

        new_orders = pd.DataFrame([
            {"order_no": base_order_no, "order_date": today_str(), "customer_name": "黄导", "source": "微信", "remark": "示例订单", "created_at": now_str()},
            {"order_no": base_order_no + "-B", "order_date": today_str(), "customer_name": "蔡子立", "source": "淘宝", "remark": "示例订单", "created_at": now_str()},
        ])
        orders_df = pd.concat([orders_df, new_orders], ignore_index=True)

        next_id = gen_next_item_id(items_df)
        sample_items = pd.DataFrame([
            {"item_id": next_id, "order_no": base_order_no, "brand": "KUSHITANI", "model": "K-2440", "color": "黑黄", "size": "XL", "qty": 1, "reserved": 1, "purchased": 1, "purchase_store": "南海部品", "purchase_date": today_str(), "arrived": 1, "arrival_date": today_str(), "printed": 0, "shipped": 0, "shipped_date": "", "tracking_no": "", "note": "示例"},
            {"item_id": next_id + 1, "order_no": base_order_no, "brand": "56design", "model": "联名外套", "color": "绿色", "size": "LL", "qty": 1, "reserved": 1, "purchased": 1, "purchase_store": "Webike", "purchase_date": today_str(), "arrived": 1, "arrival_date": today_str(), "printed": 0, "shipped": 0, "shipped_date": "", "tracking_no": "", "note": "示例"},
            {"item_id": next_id + 2, "order_no": base_order_no + "-B", "brand": "KUSHITANI", "model": "K-1366", "color": "黑色", "size": "32", "qty": 1, "reserved": 1, "purchased": 0, "purchase_store": "南海部品", "purchase_date": "", "arrived": 0, "arrival_date": "", "printed": 0, "shipped": 0, "shipped_date": "", "tracking_no": "", "note": "示例"},
        ])
        items_df = pd.concat([items_df, sample_items], ignore_index=True)

        save_orders(orders_df)
        save_items(items_df)
        st.success("示例数据已生成到 Google Sheet。")
        st.rerun()


# =========================
# 主体
# =========================
st.markdown("""
<style>
    .block-container {padding-top: 1.2rem; padding-bottom: 3rem;}
    div[data-testid="metric-container"] {
        border: 1px solid rgba(49, 51, 63, 0.12);
        border-radius: 18px;
        padding: 14px 16px;
    }
</style>
""", unsafe_allow_html=True)

st.title("果熊采购发货系统")
st.caption("订单录入 → 采购清单 → 到货登记 → 标签打印 → 发货登记")
st.caption("当前数据源：Google Sheet（持久保存版｜速度优化版）")

try:
    df = combine_data()
except Exception as e:
    st.error("Google Sheet 连接失败，请检查 Secrets、共享权限，或稍后再试。")
    st.exception(e)
    st.stop()

page = st.sidebar.radio("导航", ["首页", "订单录入", "采购清单", "到货登记", "标签打印", "发货登记", "数据总览"])

with st.sidebar:
    st.markdown("---")
    st.markdown("### 当前状态")
    metrics = fetch_dashboard_metrics(df)
    st.write(f"待采购：{metrics['待采购']}")
    st.write(f"已采购未到货：{metrics['已采购未到货']}")
    st.write(f"已到货待发货：{metrics['已到货待发货']}")
    st.write(f"今日已发货：{metrics['今日已发货']}")

if page == "首页":
    page_dashboard(df)
elif page == "订单录入":
    page_order_entry()
elif page == "采购清单":
    page_purchase(df)
elif page == "到货登记":
    page_arrival(df)
elif page == "标签打印":
    page_labels(df)
elif page == "发货登记":
    page_shipping(df)
else:
    page_data(df)
