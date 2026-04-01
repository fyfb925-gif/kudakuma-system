
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

DB_PATH = Path("kudakuma_orders.db")


# -----------------------------
# Database
# -----------------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_no TEXT UNIQUE,
        order_date TEXT NOT NULL,
        customer_name TEXT NOT NULL,
        source TEXT,
        remark TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS order_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER NOT NULL,
        brand TEXT,
        model TEXT NOT NULL,
        color TEXT,
        size TEXT,
        qty INTEGER DEFAULT 1,
        reserved INTEGER DEFAULT 1,
        purchased INTEGER DEFAULT 0,
        purchase_store TEXT,
        purchase_date TEXT,
        arrived INTEGER DEFAULT 0,
        arrival_date TEXT,
        printed INTEGER DEFAULT 0,
        shipped INTEGER DEFAULT 0,
        shipped_date TEXT,
        tracking_no TEXT,
        note TEXT,
        FOREIGN KEY(order_id) REFERENCES orders(id)
    )
    """)

    conn.commit()
    conn.close()


# -----------------------------
# Helpers
# -----------------------------
def run_query(sql: str, params: tuple = ()) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    return df


def execute(sql: str, params: tuple = ()) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql, params)
    conn.commit()
    conn.close()


def executemany(sql: str, params_list: list[tuple]) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.executemany(sql, params_list)
    conn.commit()
    conn.close()


def today_str() -> str:
    return date.today().isoformat()


def gen_order_no() -> str:
    today = date.today().strftime("%Y%m%d")
    df = run_query(
        "SELECT COUNT(*) AS cnt FROM orders WHERE order_date = ?",
        (today_str(),),
    )
    seq = int(df.loc[0, "cnt"]) + 1
    return f"{today}-{seq:03d}"


def item_label_text(row: pd.Series) -> str:
    parts = []
    if str(row.get("brand") or "").strip():
        parts.append(str(row["brand"]).strip())
    if str(row.get("model") or "").strip():
        parts.append(str(row["model"]).strip())
    item = " ".join(parts).strip()
    spec = " / ".join(
        [x for x in [str(row.get("color") or "").strip(), str(row.get("size") or "").strip()] if x]
    )
    if item and spec:
        return f"{item} / {spec}"
    return item or spec


def grouped_label_text(customer_name: str, items_df: pd.DataFrame, max_lines: int = 6) -> str:
    lines = [customer_name.strip()]
    item_lines = [item_label_text(r) for _, r in items_df.iterrows() if item_label_text(r)]
    if len(item_lines) > max_lines:
        shown = item_lines[:max_lines]
        shown.append(f"+{len(item_lines) - max_lines} more")
        item_lines = shown
    lines.extend(item_lines)
    return "\n".join(lines)


def download_df(df: pd.DataFrame, filename: str, label: str):
    st.download_button(
        label=label,
        data=df.to_csv(index=False).encode("utf-8-sig"),
        file_name=filename,
        mime="text/csv",
        use_container_width=True,
    )


def badge(text: str, color: str = "#EDEDED"):
    st.markdown(
        f"""
        <span style="
            background:{color};
            padding:4px 10px;
            border-radius:999px;
            font-size:12px;
            display:inline-block;
            margin-right:6px;
        ">{text}</span>
        """,
        unsafe_allow_html=True,
    )


# -----------------------------
# Load data
# -----------------------------
def fetch_order_items() -> pd.DataFrame:
    sql = """
    SELECT
        oi.id AS item_id,
        o.id AS order_id,
        o.order_no,
        o.order_date,
        o.customer_name,
        o.source,
        o.remark,
        oi.brand,
        oi.model,
        oi.color,
        oi.size,
        oi.qty,
        oi.reserved,
        oi.purchased,
        oi.purchase_store,
        oi.purchase_date,
        oi.arrived,
        oi.arrival_date,
        oi.printed,
        oi.shipped,
        oi.shipped_date,
        oi.tracking_no,
        oi.note
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.id
    ORDER BY o.order_date DESC, o.id DESC, oi.id DESC
    """
    df = run_query(sql)
    if df.empty:
        return df
    bool_cols = ["reserved", "purchased", "arrived", "printed", "shipped"]
    for c in bool_cols:
        df[c] = df[c].astype(int)
    return df


def fetch_dashboard_metrics(df: pd.DataFrame):
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
        "今日已发货": int(((df["shipped"] == 1) & (df["shipped_date"] == today)).sum()),
    }


# -----------------------------
# UI
# -----------------------------
def page_dashboard(df: pd.DataFrame):
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

    st.markdown("### 最近 10 条商品记录")
    preview_cols = [
        "order_no", "customer_name", "brand", "model", "color", "size",
        "purchase_store", "purchased", "arrived", "printed", "shipped"
    ]
    show = df[preview_cols].head(10).copy()
    st.dataframe(show, use_container_width=True, hide_index=True)


def page_order_entry():
    st.subheader("订单录入")

    with st.form("order_form", clear_on_submit=True):
        c1, c2, c3 = st.columns([1.2, 1, 1.2])
        order_date = c1.date_input("下单日期", value=date.today())
        customer_name = c2.text_input("客户姓名")
        source = c3.selectbox("来源", ["微信", "淘宝", "得物", "官网", "其他"])

        remark = st.text_input("订单备注")

        st.markdown("#### 商品列表")
        empty = pd.DataFrame(
            [
                {"品牌": "", "型号": "", "颜色": "", "尺寸": "", "数量": 1, "已预订": True, "备注": "", "采购店铺": ""}
            ]
        )
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
            valid_rows = []
            for _, row in items.iterrows():
                if str(row.get("型号") or "").strip():
                    valid_rows.append(row)
            if not valid_rows:
                st.error("至少填写一行商品型号。")
                return

            order_no = gen_order_no()
            conn = get_conn()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO orders (order_no, order_date, customer_name, source, remark) VALUES (?, ?, ?, ?, ?)",
                (order_no, order_date.isoformat(), customer_name.strip(), source, remark.strip()),
            )
            order_id = cur.lastrowid

            item_params = []
            for row in valid_rows:
                item_params.append(
                    (
                        order_id,
                        str(row.get("品牌") or "").strip(),
                        str(row.get("型号") or "").strip(),
                        str(row.get("颜色") or "").strip(),
                        str(row.get("尺寸") or "").strip(),
                        int(row.get("数量") or 1),
                        1 if bool(row.get("已预订")) else 0,
                        str(row.get("采购店铺") or "").strip(),
                        str(row.get("备注") or "").strip(),
                    )
                )
            cur.executemany(
                """
                INSERT INTO order_items
                (order_id, brand, model, color, size, qty, reserved, purchase_store, note)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                item_params,
            )
            conn.commit()
            conn.close()
            st.success(f"订单已保存：{order_no}")


def page_purchase(df: pd.DataFrame):
    st.subheader("采购清单")
    if df.empty:
        st.info("暂无数据。")
        return

    purchase_df = df[(df["reserved"] == 1) & (df["purchased"] == 0)].copy()
    if purchase_df.empty:
        st.success("当前没有待采购商品。")
        return

    c1, c2 = st.columns([1.2, 1])
    group_by = c1.radio("分组方式", ["按店铺", "按品牌", "不分组"], horizontal=True)
    store_filter = c2.text_input("筛选店铺")

    if store_filter.strip():
        purchase_df = purchase_df[purchase_df["purchase_store"].fillna("").str.contains(store_filter.strip(), case=False)]

    display_cols = ["item_id", "order_no", "customer_name", "brand", "model", "color", "size", "qty", "purchase_store"]
    purchase_df["选择"] = False
    editor_cols = ["选择"] + display_cols
    edited = st.data_editor(
        purchase_df[editor_cols],
        use_container_width=True,
        hide_index=True,
        disabled=display_cols,
        key="purchase_editor",
    )

    if group_by == "按店铺":
        st.markdown("#### 店铺分组概览")
        g = purchase_df.groupby("purchase_store", dropna=False).size().reset_index(name="件数").sort_values("件数", ascending=False)
        st.dataframe(g, use_container_width=True, hide_index=True)
    elif group_by == "按品牌":
        st.markdown("#### 品牌分组概览")
        g = purchase_df.groupby("brand", dropna=False).size().reset_index(name="件数").sort_values("件数", ascending=False)
        st.dataframe(g, use_container_width=True, hide_index=True)

    download_df(purchase_df[display_cols], "采购清单.csv", "下载采购清单 CSV")

    with st.form("mark_purchased_form"):
        purchase_date = st.date_input("采购日期", value=date.today())
        submitted = st.form_submit_button("标记为已采购", use_container_width=True)

        if submitted:
            selected_ids = edited.loc[edited["选择"] == True, "item_id"].tolist()
            if not selected_ids:
                st.warning("先勾选要处理的商品。")
                return
            conn = get_conn()
            cur = conn.cursor()
            cur.executemany(
                "UPDATE order_items SET purchased = 1, purchase_date = ? WHERE id = ?",
                [(purchase_date.isoformat(), int(i)) for i in selected_ids],
            )
            conn.commit()
            conn.close()
            st.success(f"已标记 {len(selected_ids)} 件商品为已采购。")
            st.rerun()


def page_arrival(df: pd.DataFrame):
    st.subheader("到货登记")
    if df.empty:
        st.info("暂无数据。")
        return

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
            conn = get_conn()
            cur = conn.cursor()
            cur.executemany(
                "UPDATE order_items SET arrived = 1, arrival_date = ? WHERE id = ?",
                [(arrival_date.isoformat(), int(i)) for i in selected_ids],
            )
            conn.commit()
            conn.close()
            st.success(f"已标记 {len(selected_ids)} 件商品为已到货。")
            st.rerun()


def page_labels(df: pd.DataFrame):
    st.subheader("标签打印")
    if df.empty:
        st.info("暂无数据。")
        return

    ready_df = df[(df["arrived"] == 1) & (df["shipped"] == 0)].copy()
    if ready_df.empty:
        st.success("当前没有可打印标签的商品。")
        return

    mode = st.radio("标签模式", ["同客户合并标签", "单件标签"], horizontal=True)

    if mode == "单件标签":
        ready_df["标签内容"] = ready_df.apply(
            lambda r: f"{r['customer_name']}\n{item_label_text(r)}",
            axis=1
        )
        show_cols = ["item_id", "order_no", "customer_name", "brand", "model", "color", "size", "标签内容"]
        st.dataframe(ready_df[show_cols], use_container_width=True, hide_index=True)

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
            execute("UPDATE order_items SET printed = 1 WHERE id = ?", (int(selected_item),))
            st.success("已标记为已打印。")
            st.rerun()

    else:
        grouped_records = []
        for customer_name, g in ready_df.groupby("customer_name", sort=False):
            grouped_records.append(
                {
                    "客户姓名": customer_name,
                    "商品数": len(g),
                    "标签内容": grouped_label_text(customer_name, g),
                    "item_ids": ",".join([str(x) for x in g["item_id"].tolist()]),
                }
            )
        grouped_df = pd.DataFrame(grouped_records)
        st.dataframe(grouped_df[["客户姓名", "商品数", "标签内容"]], use_container_width=True, hide_index=True)
        download_df(grouped_df[["客户姓名", "商品数", "标签内容"]], "合并标签内容.csv", "下载合并标签 CSV")

        selected_customer = st.selectbox("选择客户查看标签", grouped_df["客户姓名"].tolist())
        label_text = grouped_df.loc[grouped_df["客户姓名"] == selected_customer, "标签内容"].iloc[0]
        item_ids_str = grouped_df.loc[grouped_df["客户姓名"] == selected_customer, "item_ids"].iloc[0]

        st.text_area("标签内容", value=label_text, height=200)
        st.code(label_text)

        if st.button("标记该客户商品已打印", use_container_width=True):
            ids = [int(x) for x in item_ids_str.split(",") if x]
            conn = get_conn()
            cur = conn.cursor()
            cur.executemany("UPDATE order_items SET printed = 1 WHERE id = ?", [(i,) for i in ids])
            conn.commit()
            conn.close()
            st.success(f"已标记 {len(ids)} 件商品为已打印。")
            st.rerun()


def page_shipping(df: pd.DataFrame):
    st.subheader("发货登记")
    if df.empty:
        st.info("暂无数据。")
        return

    ship_df = df[(df["arrived"] == 1) & (df["shipped"] == 0)].copy()
    if ship_df.empty:
        st.success("当前没有待发货商品。")
    else:
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
                conn = get_conn()
                cur = conn.cursor()
                cur.executemany(
                    "UPDATE order_items SET shipped = 1, shipped_date = ?, tracking_no = ? WHERE id = ?",
                    [(shipped_date.isoformat(), tracking_no.strip(), int(i)) for i in selected_ids],
                )
                conn.commit()
                conn.close()
                st.success(f"已标记 {len(selected_ids)} 件商品为已发货。")
                st.rerun()

    st.markdown("### 历史发货记录")
    history = df[df["shipped"] == 1].copy()
    if history.empty:
        st.info("暂无历史发货记录。")
    else:
        show_cols = ["order_no", "customer_name", "brand", "model", "color", "size", "tracking_no", "shipped_date"]
        st.dataframe(history[show_cols], use_container_width=True, hide_index=True)
        download_df(history[show_cols], "发货记录.csv", "下载发货记录 CSV")


def page_data(df: pd.DataFrame):
    st.subheader("数据总览 / 维护")
    if df.empty:
        st.info("暂无数据。")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)
        download_df(df, "全部订单商品数据.csv", "下载全部数据 CSV")

    st.markdown("### 快速操作")
    if st.button("生成示例数据", use_container_width=True):
        conn = get_conn()
        cur = conn.cursor()

        base_order_no = gen_order_no()
        orders = [
            (base_order_no, today_str(), "黄导", "微信", "示例订单"),
            (base_order_no + "-B", today_str(), "蔡子立", "淘宝", "示例订单"),
        ]
        inserted_order_ids = []
        for order_no, order_date, customer, source, remark in orders:
            cur.execute(
                "INSERT INTO orders (order_no, order_date, customer_name, source, remark) VALUES (?, ?, ?, ?, ?)",
                (order_no, order_date, customer, source, remark),
            )
            inserted_order_ids.append(cur.lastrowid)

        sample_items = [
            (inserted_order_ids[0], "KUSHITANI", "K-2440", "黑黄", "XL", 1, 1, 1, "南海部品", today_str(), 1, today_str(), 0, 0, "", "示例"),
            (inserted_order_ids[0], "56design", "联名外套", "绿色", "LL", 1, 1, 1, "Webike", today_str(), 1, today_str(), 0, 0, "", "示例"),
            (inserted_order_ids[1], "KUSHITANI", "K-1366", "黑色", "32", 1, 1, 0, "南海部品", "", 0, "", 0, 0, "", "示例"),
        ]
        cur.executemany(
            """
            INSERT INTO order_items
            (order_id, brand, model, color, size, qty, reserved, purchased, purchase_store,
             purchase_date, arrived, arrival_date, printed, shipped, tracking_no, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            sample_items,
        )
        conn.commit()
        conn.close()
        st.success("示例数据已生成。")
        st.rerun()

    if st.button("清空全部数据（危险）", use_container_width=True, type="secondary"):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM order_items")
        cur.execute("DELETE FROM orders")
        conn.commit()
        conn.close()
        st.warning("已清空全部数据。")
        st.rerun()


def main():
    st.set_page_config(
        page_title="果熊采购发货系统",
        page_icon="📦",
        layout="wide",
    )

    init_db()

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

    page = st.sidebar.radio(
        "导航",
        ["首页", "订单录入", "采购清单", "到货登记", "标签打印", "发货登记", "数据总览"],
    )

    df = fetch_order_items()

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
    elif page == "数据总览":
        page_data(df)


if __name__ == "__main__":
    main()
