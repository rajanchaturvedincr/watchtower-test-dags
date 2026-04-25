"""
Revenue Reconciliation Pipeline.

Fetches daily transaction records, reconciles revenue totals by region,
and publishes a summary report. Used by the Finance team for daily close.

Task flow:  fetch_transactions -> reconcile_revenue -> publish_report
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def _fetch_transactions(**context):
    """Pull raw transaction records from the payments API."""
    transactions = [
        {"txn_id": "TXN-001", "region": "us-east", "amount": 1250.00, "currency": "USD", "status": "settled"},
        {"txn_id": "TXN-002", "region": "us-west", "amount": 890.50, "currency": "USD", "status": "settled"},
        {"txn_id": "TXN-003", "region": "us-east", "amount": 2100.75, "currency": "USD", "status": "settled"},
        {"txn_id": "TXN-004", "region": "eu-west", "amount": 750.00, "currency": "EUR", "status": "settled"},
        {"txn_id": "TXN-005", "region": "us-west", "amount": 3200.00, "currency": "USD", "status": "pending"},
        {"txn_id": "TXN-006", "region": "us-east", "amount": 415.25, "currency": "USD", "status": "settled"},
        {"txn_id": "TXN-007", "region": "eu-west", "amount": 1680.00, "currency": "EUR", "status": "settled"},
    ]
    context["ti"].xcom_push(key="raw_transactions", value=transactions)
    print(f"Fetched {len(transactions)} transactions from payments API.")


def _reconcile_revenue(**context):
    """Aggregate settled revenue by region for daily reconciliation."""
    transactions = context["ti"].xcom_pull(
        task_ids="fetch_transactions", key="raw_transactions"
    )

    settled = [txn for txn in transactions if txn["status"] == "settled"]
    print(f"Processing {len(settled)} settled transactions...")

    region_totals = {}
    for txn in settled:
        region = txn["region"]
        # BUG: typo in key name — should be "amount" not "ammount"
        region_totals[region] = region_totals.get(region, 0) + txn["ammount"]

    print(f"Revenue by region: {region_totals}")
    context["ti"].xcom_push(key="region_totals", value=region_totals)


def _publish_report(**context):
    """Format and publish the daily revenue summary."""
    totals = context["ti"].xcom_pull(
        task_ids="reconcile_revenue", key="region_totals"
    )
    grand_total = sum(totals.values())
    print(f"Daily Revenue Report")
    print(f"====================")
    for region, total in sorted(totals.items()):
        print(f"  {region}: ${total:,.2f}")
    print(f"  TOTAL: ${grand_total:,.2f}")
    print(f"Report published successfully.")


with DAG(
    dag_id="watchtower_revenue_reconciliation",
    description="Daily revenue reconciliation pipeline for Finance team",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "owner": "watchtower",
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["watchtower", "finance", "reconciliation"],
) as dag:

    fetch = PythonOperator(
        task_id="fetch_transactions",
        python_callable=_fetch_transactions,
    )

    reconcile = PythonOperator(
        task_id="reconcile_revenue",
        python_callable=_reconcile_revenue,
    )

    publish = PythonOperator(
        task_id="publish_report",
        python_callable=_publish_report,
    )

    fetch >> reconcile >> publish
