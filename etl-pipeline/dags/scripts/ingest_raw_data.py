import datetime
import time
import requests
import pandas as pd
from airflow.models import Variable
from airflow.hooks.base import BaseHook

RAW_PATH = "/opt/airflow/tmp"
DATE_FMT = "%d/%m/%Y"
TS_FMT = "%Y-%m-%d %H:%M:%S"

# =====================
# Utility
# =====================
def calculate_age(date_str):
    """Calculate days from date (dd/mm/yyyy) to now"""
    if not date_str:
        return None
    try:
        dt = datetime.datetime.strptime(date_str, DATE_FMT)
        return (datetime.datetime.now() - dt).days
    except ValueError:
        return None

def parse_printed_time(value):
    if not value:
        return None
    return datetime.datetime.strptime(value, "%d/%m/%Y, %H:%M")

def get_accurate_config():
    conn = BaseHook.get_connection("accurate-api")
    return {
        "TOKEN": Variable.get("accurate_access_token"),
        "SESSION_ID": Variable.get("accurate_db_session"),
        "DB_URL": conn.extra_dejson["db_url"]
    }


def build_headers(token, session_id):
    return {
        "Authorization": f"Bearer {token}",
        "X-Session-ID": session_id,
        "Content-Type": "application/json"
    }


# =====================
# Accurate API
# =====================
def fetch_invoice_list(db_url, headers, start_date, end_date, page_size=100):
    all_invoices = []
    page = 1

    while True:
        params = {
            "sp.pageSize": page_size,
            "sp.page": page,
            "filter.transDate.op": "BETWEEN",
            "filter.transDate.val[0]": start_date.strftime(DATE_FMT),
            "filter.transDate.val[1]": end_date.strftime(DATE_FMT),
        }

        print(f"Fetching invoice list page {page}")
        resp = requests.post(
            f"{db_url}/accurate/api/sales-invoice/list.do",
            headers=headers,
            params=params
        )

        data = resp.json().get("d", [])
        print(f"Status {resp.status_code} | Found {len(data)} invoices")

        if not data:
            break

        all_invoices.extend(data)
        page += 1
        time.sleep(1)

    return all_invoices


def fetch_invoice_detail(db_url, headers, invoices, batch_sleep=1.5):
    for idx, inv in enumerate(invoices, 1):
        invoice_id = inv.get("id")

        resp = requests.get(
            f"{db_url}/accurate/api/sales-invoice/detail.do",
            headers=headers,
            params={"id": invoice_id}
        )

        invoices[idx - 1] = resp.json().get("d", {})
        print(f"[{idx}/{len(invoices)}] fetched invoice {invoice_id}")

        if idx % 5 == 0:
            time.sleep(batch_sleep)

    return invoices


# =====================
# Transformation
# =====================
def transform_invoice_header(invoices):
    records = []

    for inv in invoices:
        records.append({
            # Primary Key
            'invoice_id': inv.get('id'),
            'invoice_number': inv.get('number'),

            # Dates
            'invoice_date': inv.get('transDate'),
            'due_date': inv.get('dueDate'),
            'ship_date': inv.get('shipDate'),

            # Customer
            'customer_id': inv.get('customerId'),
            'customer_name': inv.get('customer', {}).get('name') if inv.get('customer') else None,

            # Financial - Core
            'sub_total': float(inv.get('subTotal', 0)),
            'total_amount': float(inv.get('totalAmount', 0)),
            'outstanding_amount': float(inv.get('outstanding', 0)),

            # Status
            'status': inv.get('status'),
            'approval_status': inv.get('approvalStatus'),

            # Reference
            'po_number': inv.get('poNumber'),
            'sales_order_id': inv.get('salesOrderId'),
            'delivery_order_id': inv.get('deliveryOrderId'),

            # Payment Terms & Currency
            'payment_term_id': inv.get('paymentTermId'),
            'payment_term_name': inv.get('paymentTerm', {}).get('name') if inv.get('paymentTerm') else None,
            'currency_id': inv.get('currencyId'),
            'currency_code': inv.get('currency', {}).get('code') if inv.get('currency') else 'IDR',
            'exchange_rate': float(inv.get('rate', 1.0)),

            # Organization
            'branch_id': inv.get('branchId'),
            'branch_name': inv.get('branchName'),

            # Calculated
            'invoice_age_days': calculate_age(inv.get('transDate')),

            # Metadata
            'created_by': inv.get('createdBy'),
            'printed_time': parse_printed_time(inv.get('printedTime')),
            "extracted_at": datetime.datetime.now().strftime(TS_FMT)
        })

    return pd.DataFrame(records)


def transform_invoice_detail(invoices):
    records = []

    for inv in invoices:
        for d in inv.get("detailItem", []):
            item = d.get("item", {})
            unit = d.get("itemUnit", {})

            records.append({
                "detail_id": d.get("id"),
                "invoice_id": inv.get("id"),
                "invoice_number": inv.get("number"),

                "item_id": d.get("itemId"),
                "item_number": item.get("no"),
                "item_name": item.get("name"),
                "item_category_id": item.get("itemCategoryId"),

                "quantity": float(d.get("quantity", 0)),
                "unit_id": d.get("itemUnitId"),
                "unit_name": unit.get("name"),
                "unit_ratio": float(d.get("unitRatio", 1)),

                "unit_price": float(d.get("unitPrice", 0)),
                "gross_amount": float(d.get("grossAmount", 0)),
                "sales_amount": float(d.get("salesAmount", 0)),

                "warehouse_id": d.get("warehouseId"),
                "warehouse_name": d.get("warehouse", {}).get("name") if d.get('warehouse') else None,

                "sales_order_detail_id": d.get("salesOrderDetailId"),
                "delivery_order_detail_id": d.get("deliveryOrderDetailId"),

                "line_seq": d.get("seq"),
                "extracted_at": datetime.datetime.now().strftime(TS_FMT)
            })

    return pd.DataFrame(records)


# =====================
# Airflow Task
# =====================
def ingest_raw_data(data_interval_start, data_interval_end, **_):
    print(f"Interval: {data_interval_start} → {data_interval_end}")

    cfg = get_accurate_config()
    headers = build_headers(cfg["TOKEN"], cfg["SESSION_ID"])

    invoices = fetch_invoice_list(
        cfg["DB_URL"], headers,
        data_interval_start, data_interval_end
    )

    invoices = fetch_invoice_detail(cfg["DB_URL"], headers, invoices)

    df_header = transform_invoice_header(invoices)
    df_detail = transform_invoice_detail(invoices)

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    df_header.to_csv(f"{RAW_PATH}/sales_invoices_{data_interval_start.strftime('%Y%m%d')}_{data_interval_end.strftime('%Y%m%d')}.csv", index=False)
    df_detail.to_csv(f"{RAW_PATH}/sales_invoice_details_{data_interval_start.strftime('%Y%m%d')}_{data_interval_end.strftime('%Y%m%d')}.csv", index=False)

    print("Ingestion finished successfully")
