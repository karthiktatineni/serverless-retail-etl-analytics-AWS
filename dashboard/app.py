from __future__ import annotations

import hmac
import os
import time
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import streamlit as st

try:
    from dashboard.athena import (
        CACHE_TTL_SECONDS,
        AwsAccessError,
        cached_discover_partition_dates,
        cached_fetch_dashboard_athena_data,
        get_config,
        validate_date_range,
        verify_aws_identity,
    )
    from dashboard.insights import cached_fetch_latest_insights_report
    from dashboard.quality import cached_fetch_quality_summary
except ImportError:
    from athena import (
        CACHE_TTL_SECONDS,
        AwsAccessError,
        cached_discover_partition_dates,
        cached_fetch_dashboard_athena_data,
        get_config,
        validate_date_range,
        verify_aws_identity,
    )
    from insights import cached_fetch_latest_insights_report
    from quality import cached_fetch_quality_summary


st.set_page_config(
    page_title="Orders Analytics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    [data-testid="stMetricValue"] {
        font-size: clamp(1.25rem, 1.8vw, 1.85rem);
        white-space: normal;
        overflow-wrap: anywhere;
    }
    [data-testid="stMetricLabel"] p {
        white-space: normal;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def _format_utc_display(iso_ts: str | None) -> str:
    if not iso_ts:
        return "Unavailable"
    try:
        dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00")).astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except ValueError:
        return iso_ts


def _apply_chart_style(fig, yaxis_title: str, xaxis_title: str = "") -> None:
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=20, r=20, t=35, b=20),
        xaxis_title=xaxis_title,
        yaxis_title=yaxis_title,
        font=dict(color="#1f2937"),
        hovermode="x unified",
    )


def _enforce_passcode_gate() -> bool:
    expected = os.getenv("DASHBOARD_PASSCODE", "").strip()
    if not expected:
        return False

    if st.session_state.get("dashboard_authenticated", False):
        return True

    st.subheader("Access Verification")
    st.info("Enter the dashboard passcode to load AWS Athena and S3 analytics.")
    with st.form("passcode_form", clear_on_submit=True):
        entered = st.text_input("Passcode", type="password")
        submitted = st.form_submit_button("Unlock Dashboard")

    if submitted:
        if hmac.compare_digest(entered, expected):
            st.session_state["dashboard_authenticated"] = True
            st.rerun()
        else:
            st.error("Invalid passcode.")
    st.stop()


def main() -> None:
    cfg = get_config()

    st.title("Orders Analytics Dashboard")
    st.warning(
        "DATA IS SIMULATED — This dashboard uses synthetic order records generated for an ETL demonstration."
    )

    passcode_enabled = _enforce_passcode_gate()

    with st.sidebar:
        st.header("Controls & Filters")
        if st.button("Refresh Data", width="stretch"):
            now_ts = time.monotonic()
            last_refresh = st.session_state.get("last_manual_refresh_ts", 0.0)
            cooldown = 60.0
            if now_ts - last_refresh < cooldown:
                remaining = int(cooldown - (now_ts - last_refresh)) + 1
                st.warning(f"Please wait {remaining}s before forcing another refresh.")
            else:
                st.session_state["last_manual_refresh_ts"] = now_ts
                st.cache_data.clear()
                st.rerun()

        if passcode_enabled and st.button("Lock Dashboard", width="stretch"):
            st.session_state["dashboard_authenticated"] = False
            st.rerun()

        st.caption(f"Query cache TTL: {CACHE_TTL_SECONDS // 60} min")

    try:
        verify_aws_identity()
        available_dates = cached_discover_partition_dates()
    except AwsAccessError as exc:
        st.error(f"AWS connectivity error: {exc}")
        st.info(
            "Verify that your AWS CLI profile (`etl-project`) is signed in and has read access to STS, Glue, Athena, and S3."
        )
        st.stop()

    if not available_dates:
        st.warning(
            f"No Glue partitions (`order_date`) were discovered in `{cfg.athena_database}.{cfg.athena_table}`."
        )
        st.stop()

    earliest_date = available_dates[0]
    latest_date = available_dates[-1]

    with st.sidebar:
        st.caption(
            f"Glue partitions found: **{len(available_dates)}** "
            f"(`{earliest_date.isoformat()}` to `{latest_date.isoformat()}`)"
        )
        selected_range = st.date_input(
            "Select Order Date Range",
            value=(earliest_date, latest_date),
            min_value=earliest_date,
            max_value=latest_date,
            help="Bounds are discovered from AWS Glue order_date partitions.",
        )

    if isinstance(selected_range, tuple):
        if len(selected_range) != 2:
            st.info("Please select both a start date and an end date.")
            st.stop()
        start_date, end_date = selected_range
    else:
        start_date = selected_range
        end_date = selected_range

    try:
        start_str, end_str = validate_date_range(start_date, end_date)
    except ValueError as exc:
        st.error(f"Invalid date range selection: {exc}")
        st.stop()

    try:
        with st.spinner("Querying Athena and loading S3 quality and insights reports..."):
            athena_data = cached_fetch_dashboard_athena_data(start_str, end_str)
            quality_summary = cached_fetch_quality_summary(start_str, end_str)
            insights_report = cached_fetch_latest_insights_report(latest_date.isoformat())
    except AwsAccessError as exc:
        st.error(f"Failed to load pipeline data: {exc}")
        st.stop()
    except ValueError as exc:
        st.error(f"Validation error: {exc}")
        st.stop()

    kpis_df: pd.DataFrame = athena_data["kpis_df"]
    daily_df: pd.DataFrame = athena_data["daily_df"]
    category_df: pd.DataFrame = athena_data["category_df"]
    customers_df: pd.DataFrame = athena_data["customers_df"]

    total_orders = None
    total_revenue = None
    aov = None
    if not kpis_df.empty:
        row = kpis_df.iloc[0]
        raw_orders = row.get("total_orders")
        raw_rev = row.get("total_revenue")
        raw_aov = row.get("average_order_value")
        total_orders = int(raw_orders) if pd.notna(raw_orders) else 0
        total_revenue = float(raw_rev) if pd.notna(raw_rev) else None
        aov = float(raw_aov) if pd.notna(raw_aov) else None

    # Panel 1: KPI Cards
    st.subheader(f"Key Metrics ({start_str} to {end_str})")
    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)

    kpi_col1.metric(
        "Total Orders",
        f"{total_orders:,}" if total_orders is not None else "Unavailable",
        help="COUNT(DISTINCT order_id) from Athena curated table",
    )
    kpi_col2.metric(
        "Total Revenue",
        f"{total_revenue:,.2f}" if total_revenue is not None else "Unavailable",
        help="SUM(quantity * unit_price) across valid curated orders (unitless simulated currency)",
    )
    kpi_col3.metric(
        "Average Order Value",
        f"{aov:,.2f}" if aov is not None else "Unavailable",
        help="SUM(quantity * unit_price) / NULLIF(COUNT(DISTINCT order_id), 0)",
    )
    kpi_col4.metric(
        "Overall Rejection Rate",
        (
            f"{quality_summary.overall_reject_rate_pct:.2f}%"
            if quality_summary.overall_reject_rate_pct is not None
            else "Unavailable"
        ),
        help="SUM(rejected_rows) / SUM(total_rows) * 100 across valid S3 quality reports",
    )

    if quality_summary.missing_dates:
        st.warning(
            "Incomplete quality report coverage for selected range. "
            f"Missing or invalid reports for date(s): {', '.join(quality_summary.missing_dates)}. "
            "Missing reports are excluded rather than treated as 0% rejection."
        )

    if quality_summary.invalid_reports:
        with st.expander("Malformed or Failed Quality Reports Detected"):
            st.dataframe(pd.DataFrame(quality_summary.invalid_reports), width="stretch")

    if not total_orders:
        st.info("No curated order records were returned by Athena for the selected date range.")

    st.divider()

    # Panel 2: Daily Sales Performance
    st.subheader("Daily Sales Performance")
    if daily_df.empty:
        st.info("No daily Athena records available for the selected date range.")
    else:
        perf_col1, perf_col2 = st.columns(2)

        with perf_col1:
            fig_orders = px.line(
                daily_df,
                x="order_date",
                y="total_orders",
                markers=True,
                title="Daily Total Orders",
                color_discrete_sequence=["#1f4e79"],
            )
            fig_orders.update_xaxes(type="category")
            _apply_chart_style(fig_orders, yaxis_title="Total Orders", xaxis_title="Order Date")
            st.plotly_chart(fig_orders, width="stretch")

        with perf_col2:
            fig_rev = px.line(
                daily_df,
                x="order_date",
                y="total_revenue",
                markers=True,
                title="Daily Total Revenue",
                color_discrete_sequence=["#2e7d32"],
            )
            fig_rev.update_xaxes(type="category")
            _apply_chart_style(fig_rev, yaxis_title="Total Revenue", xaxis_title="Order Date")
            st.plotly_chart(fig_rev, width="stretch")

    # Panel 3 & Panel 4: Category Revenue and Top 10 Simulated Customers
    cat_col, cust_col = st.columns(2)

    with cat_col:
        st.subheader("Revenue by Category")
        if category_df.empty:
            st.info("No category records returned for the selected date range.")
        else:
            fig_cat = px.bar(
                category_df,
                x="category",
                y="revenue",
                text_auto=".2s",
                color_discrete_sequence=["#2b6cb0"],
            )
            _apply_chart_style(fig_cat, yaxis_title="Revenue", xaxis_title="Category")
            fig_cat.update_layout(hovermode="closest")
            st.plotly_chart(fig_cat, width="stretch")

    with cust_col:
        st.subheader("Top 10 Customers by Revenue")
        if customers_df.empty:
            st.info("No customer records returned for the selected date range.")
        else:
            cust_plot_df = customers_df.copy()
            cust_plot_df["customer_label"] = cust_plot_df["customer_id"].apply(
                lambda cid: f"Sim ID {int(cid)}" if pd.notna(cid) else "Unknown"
            )
            cust_plot_df = cust_plot_df.sort_values("revenue", ascending=True)
            fig_cust = px.bar(
                cust_plot_df,
                x="revenue",
                y="customer_label",
                orientation="h",
                hover_data=["total_orders", "customer_id"],
                color_discrete_sequence=["#4a5568"],
            )
            fig_cust.update_layout(
                template="plotly_white",
                margin=dict(l=20, r=20, t=35, b=20),
                xaxis_title="Revenue",
                yaxis_title="Simulated Customer ID",
                font=dict(color="#1f2937"),
            )
            st.plotly_chart(fig_cust, width="stretch")
            st.caption(
                "Identifiers shown are synthetic customer IDs. The dataset contains no product IDs or customer PII."
            )

    st.divider()

    # Panel 5 & Panel 6: Data Quality Trend and Validation-Failure Occurrences
    st.subheader("Data Quality & Validation Monitoring")
    dq_col1, dq_col2 = st.columns(2)

    with dq_col1:
        st.markdown("**Daily Data Quality Trend**")
        if quality_summary.daily_df.empty:
            st.info("No valid S3 quality reports found for the selected date range.")
        else:
            fig_dq = px.line(
                quality_summary.daily_df,
                x="report_date",
                y="reject_rate_pct",
                markers=True,
                hover_data=["total_rows", "valid_rows", "rejected_rows"],
                color_discrete_sequence=["#b45309"],
            )
            fig_dq.update_xaxes(type="category")
            _apply_chart_style(
                fig_dq,
                yaxis_title="Rejection Rate (%)",
                xaxis_title="Report Date",
            )
            st.plotly_chart(fig_dq, width="stretch")

    with dq_col2:
        st.markdown("**Validation-Failure Occurrences by Reason**")
        if quality_summary.daily_df.empty:
            st.info("No quality report reason counts available for the selected date range.")
        else:
            reason_df = pd.DataFrame(
                [
                    {"reason": reason, "occurrences": count}
                    for reason, count in quality_summary.reason_counts.items()
                ]
            ).sort_values("occurrences", ascending=False)
            fig_reasons = px.bar(
                reason_df,
                x="reason",
                y="occurrences",
                text_auto=True,
                color_discrete_sequence=["#c53030"],
            )
            _apply_chart_style(
                fig_reasons,
                yaxis_title="Validation-Failure Occurrences",
                xaxis_title="Validation Rule",
            )
            fig_reasons.update_layout(hovermode="closest")
            st.plotly_chart(fig_reasons, width="stretch")
            st.caption(
                "Note: Validation rules are evaluated independently per row, so a single rejected row with multiple defects increments multiple reason counts."
            )

    # Panel 7: Quality Report Coverage
    st.subheader("Quality Report Coverage")
    cov_cols = st.columns(6)
    cov_cols[0].metric("Selected Dates", f"{quality_summary.selected_dates_count:,}")
    cov_cols[1].metric("Reports Found", f"{quality_summary.reports_found_count:,}")
    cov_cols[2].metric("Missing Reports", f"{len(quality_summary.missing_dates):,}")
    cov_cols[3].metric(
        "Total Input Rows",
        f"{quality_summary.total_input_rows:,}"
        if quality_summary.reports_found_count > 0
        else "Unavailable",
    )
    cov_cols[4].metric(
        "Total Valid Rows",
        f"{quality_summary.total_valid_rows:,}"
        if quality_summary.reports_found_count > 0
        else "Unavailable",
    )
    cov_cols[5].metric(
        "Total Rejected Rows",
        f"{quality_summary.total_rejected_rows:,}"
        if quality_summary.reports_found_count > 0
        else "Unavailable",
    )

    if not quality_summary.daily_df.empty:
        with st.expander("Daily Quality Report Details (from S3)"):
            st.dataframe(quality_summary.daily_df, width="stretch")

    st.divider()

    # Panel 8: AI-Generated Business Insights (read-only from S3 reports/insights/)
    st.subheader("AI-Generated Business Insights")
    if not insights_report.found:
        if insights_report.error_message:
            st.warning(
                f"Latest AI insights report could not be loaded: {insights_report.error_message}"
            )
        else:
            st.info(
                "No AI-generated insights reports were found in S3 yet. Run `orders-etl-insights` to generate a report."
            )
    else:
        if insights_report.is_stale and insights_report.stale_reason:
            st.warning(f"Stale AI insights report notice: {insights_report.stale_reason}")

        meta_col1, meta_col2, meta_col3 = st.columns(3)
        meta_col1.metric("Insights Report Date", insights_report.report_date or "Unavailable")
        meta_col2.metric("Gemini Model", insights_report.gemini_model or "Unavailable")
        meta_col3.metric("Generated At (UTC)", _format_utc_display(insights_report.generated_at))

        if insights_report.report_date and (
            insights_report.report_date < start_str or insights_report.report_date > end_str
        ):
            st.info(
                f"Showing the latest available AI insights report (`{insights_report.report_date}`), "
                f"which falls outside the currently selected filter range (`{start_str}` to `{end_str}`)."
            )

        st.markdown("**Executive Summary**")
        st.write(insights_report.executive_summary)

        obs_col, rec_col = st.columns(2)
        with obs_col:
            st.markdown("**Important Observations**")
            if insights_report.observations:
                for item in insights_report.observations:
                    st.markdown(f"- {item}")
            else:
                st.caption("No observations recorded in the report.")

        with rec_col:
            st.markdown("**Actionable Recommendations**")
            if insights_report.recommendations:
                for item in insights_report.recommendations:
                    st.markdown(f"- {item}")
            else:
                st.caption("No recommendations recorded in the report.")

            if insights_report.opportunities:
                st.markdown("**Opportunities**")
                for item in insights_report.opportunities:
                    st.markdown(f"- {item}")

        if insights_report.caveats:
            with st.expander("Data Limitations & Caveats"):
                for item in insights_report.caveats:
                    st.markdown(f"- {item}")


if __name__ == "__main__":
    main()

