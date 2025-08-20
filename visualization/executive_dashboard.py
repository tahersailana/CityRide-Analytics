import streamlit as st
import snowflake.connector
import pandas as pd
import numpy as np

# Custom CSS for better styling
def load_css():
    st.markdown("""
    <style>
        .metric-card {
            background-color: #f0f2f6;
            padding: 1rem;
            border-radius: 0.5rem;
            border-left: 4px solid #1f77b4;
            margin-bottom: 1rem;
        }
        .positive-growth {
            color: #28a745;
            font-weight: bold;
            font-size: 1.2em;
        }
        .negative-growth {
            color: #dc3545;
            font-weight: bold;
            font-size: 1.2em;
        }
        .neutral-growth {
            color: #6c757d;
            font-weight: bold;
            font-size: 1.2em;
        }
        .big-number {
            font-size: 2em;
            font-weight: bold;
            color: #1f77b4;
        }
        .growth-indicator {
            padding: 0.5rem;
            border-radius: 0.25rem;
            margin: 0.5rem 0;
            text-align: center;
        }
        .growth-positive {
            background-color: #d4edda;
            border: 1px solid #c3e6cb;
            color: #155724;
        }
        .growth-negative {
            background-color: #f8d7da;
            border: 1px solid #f5c6cb;
            color: #721c24;
        }
        .section-header {
            background: linear-gradient(90deg, #1f77b4, #17a2b8);
            color: white;
            padding: 1rem;
            border-radius: 0.5rem;
            margin: 1rem 0;
            text-align: center;
            font-size: 1.2em;
            font-weight: bold;
        }
    </style>
    """, unsafe_allow_html=True)

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_executive_data():
    """Load executive KPI data from Snowflake"""
    try:
        conn = snowflake.connector.connect(
            user='AIRFLOW_USER',
            password='StrongPassword123!',
            account='ekorbhk-no98289',
            warehouse='AIRFLOW_WH',
            database='CITYRIDE_ANALYTICS',
            schema='CITYRIDE_ANALYTICS',
            role='AIRFLOW_ROLE'
        )
        
        query = "SELECT * FROM vw_executive_kpis"
        df = pd.read_sql(query, conn)
        conn.close()
        
        return df
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {str(e)}")
        return None

def format_number(num, format_type="number"):
    """Format numbers for display"""
    if pd.isna(num):
        return "N/A"
    
    if format_type == "currency":
        if num >= 1_000_000:
            return f"${num/1_000_000:.1f}M"
        elif num >= 1_000:
            return f"${num/1_000:.0f}K"
        else:
            return f"${num:.2f}"
    elif format_type == "percentage":
        return f"{num:+.1f}%"
    elif format_type == "number":
        if num >= 1_000_000:
            return f"{num/1_000_000:.1f}M"
        elif num >= 1_000:
            return f"{num/1_000:.0f}K"
        else:
            return f"{num:,.0f}"
    elif format_type == "decimal":
        return f"{num:.2f}"
    elif format_type == "time":
        return f"{num:.1f} min"
    else:
        return str(num)

def get_growth_status(growth_pct):
    """Get status and emoji for growth"""
    if pd.isna(growth_pct):
        return "neutral", "➖", "No Change"
    elif growth_pct > 0:
        return "positive", "📈", "Growth"
    elif growth_pct < 0:
        return "negative", "📉", "Decline"
    else:
        return "neutral", "➖", "Stable"

def show_executive_dashboard():
    """Main function to display the executive dashboard"""
    
    # Load CSS
    load_css()
    
    # Header
    st.markdown('<div class="section-header">📊 Executive KPIs Dashboard</div>', unsafe_allow_html=True)
    
    # Load data
    with st.spinner("Loading executive data from Snowflake..."):
        df = load_executive_data()
    
    if df is None or df.empty:
        st.error("No data available. Please check your database connection.")
        return
    
    # Extract metrics from the dataframe
    row = df.iloc[0]  # Since we expect only one row
    
    total_trips = row['TOTAL_TRIPS']
    total_revenue = row['TOTAL_REVENUE']
    avg_trip_value = row['AVG_TRIP_VALUE']
    total_passengers = row['TOTAL_PASSENGERS']
    avg_distance = row['AVG_DISTANCE']
    avg_duration = row['AVG_DURATION']
    trips_growth_pct = row['TRIPS_GROWTH_PCT']
    revenue_growth_pct = row['REVENUE_GROWTH_PCT']
    
    # Auto-refresh indicator (subtle)
    st.caption("Data auto-refreshes every 5 minutes")
    
    st.markdown("---")
    
    # Key Performance Indicators
    st.markdown('<div class="section-header">📊 Key Performance Indicators</div>', unsafe_allow_html=True)
    
    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
    
    with kpi_col1:
        st.markdown(f'<div class="big-number">{format_number(total_trips)}</div>', unsafe_allow_html=True)
        st.write("**Total Trips**")
        
        status, emoji, text = get_growth_status(trips_growth_pct)
        growth_class = f"growth-{status}" if status != "neutral" else ""
        if not pd.isna(trips_growth_pct):
            st.markdown(f'<div class="growth-indicator {growth_class}">{emoji} {trips_growth_pct:+.1f}% vs 2023</div>', unsafe_allow_html=True)
    
    with kpi_col2:
        st.markdown(f'<div class="big-number">{format_number(total_revenue, "currency")}</div>', unsafe_allow_html=True)
        st.write("**Total Revenue**")
        
        status, emoji, text = get_growth_status(revenue_growth_pct)
        growth_class = f"growth-{status}" if status != "neutral" else ""
        if not pd.isna(revenue_growth_pct):
            st.markdown(f'<div class="growth-indicator {growth_class}">{emoji} {revenue_growth_pct:+.1f}% vs 2023</div>', unsafe_allow_html=True)
    
    with kpi_col3:
        st.markdown(f'<div class="big-number">{format_number(avg_trip_value, "currency")}</div>', unsafe_allow_html=True)
        st.write("**Average Trip Value**")
        st.info("💡 Revenue per trip")
    
    with kpi_col4:
        st.markdown(f'<div class="big-number">{format_number(total_passengers)}</div>', unsafe_allow_html=True)
        st.write("**Total Passengers**")
        avg_passengers_per_trip = total_passengers / total_trips if total_trips > 0 else 0
        st.info(f"👥 {avg_passengers_per_trip:.1f} passengers/trip")
    
    st.markdown("---")
    
    # Operational Metrics
    st.markdown('<div class="section-header">🎯 Operational Metrics</div>', unsafe_allow_html=True)
    
    ops_col1, ops_col2, ops_col3 = st.columns(3)
    
    with ops_col1:
        st.metric(
            label="🛣️ Average Distance",
            value=f"{avg_distance:.2f} miles",
            delta=None,
            help="Average distance per trip"
        )
    
    with ops_col2:
        st.metric(
            label="⏱️ Average Duration",
            value=f"{avg_duration:.1f} minutes",
            delta=None,
            help="Average time per trip"
        )
    
    with ops_col3:
        # Calculate speed
        avg_speed = (avg_distance / (avg_duration / 60)) if avg_duration > 0 else 0
        st.metric(
            label="🚗 Average Speed",
            value=f"{avg_speed:.1f} mph",
            delta=None,
            help="Calculated from distance and duration"
        )
    
    st.markdown("---")
    
    # Growth Analysis
    st.markdown('<div class="section-header">📈 Growth Analysis</div>', unsafe_allow_html=True)
    
    growth_col1, growth_col2 = st.columns(2)
    
    with growth_col1:
        st.subheader("Trip Performance")
        
        # Trip growth visualization
        if not pd.isna(trips_growth_pct):
            if trips_growth_pct >= 0:
                st.success(f"📈 Trips grew by {trips_growth_pct:.1f}%")
                st.progress(min(trips_growth_pct / 20, 1.0))  # Assuming 20% is max for visualization
            else:
                st.error(f"📉 Trips declined by {abs(trips_growth_pct):.1f}%")
                st.progress(0)
        else:
            st.info("No growth data available")
        
        st.write("**Trip Volume Analysis:**")
        st.write(f"• 2024: {format_number(total_trips)} trips")
        if not pd.isna(trips_growth_pct):
            previous_trips = total_trips / (1 + trips_growth_pct/100)
            st.write(f"• 2023: {format_number(previous_trips)} trips")
    
    with growth_col2:
        st.subheader("Revenue Performance")
        
        # Revenue growth visualization
        if not pd.isna(revenue_growth_pct):
            if revenue_growth_pct >= 0:
                st.success(f"📈 Revenue grew by {revenue_growth_pct:.1f}%")
                st.progress(min(revenue_growth_pct / 20, 1.0))  # Assuming 20% is max for visualization
            else:
                st.error(f"📉 Revenue declined by {abs(revenue_growth_pct):.1f}%")
                st.progress(0)
        else:
            st.info("No growth data available")
        
        st.write("**Revenue Analysis:**")
        st.write(f"• 2024: {format_number(total_revenue, 'currency')}")
        if not pd.isna(revenue_growth_pct):
            previous_revenue = total_revenue / (1 + revenue_growth_pct/100)
            st.write(f"• 2023: {format_number(previous_revenue, 'currency')}")
    
    st.markdown("---")
    
    # Key Insights
    st.markdown('<div class="section-header">💡 Key Insights</div>', unsafe_allow_html=True)
    
    insights_col1, insights_col2 = st.columns(2)
    
    with insights_col1:
        st.subheader("Performance Summary")
        
        # Generate insights based on data
        insights = []
        
        if not pd.isna(trips_growth_pct) and not pd.isna(revenue_growth_pct):
            if trips_growth_pct > 0 and revenue_growth_pct > 0:
                insights.append("🟢 Both trips and revenue are growing - strong performance")
            elif trips_growth_pct > 0 and revenue_growth_pct < 0:
                insights.append("🟡 Trip volume up but revenue down - check pricing strategy")
            elif trips_growth_pct < 0 and revenue_growth_pct > 0:
                insights.append("🟡 Fewer trips but higher revenue - premium strategy working")
            else:
                insights.append("🔴 Both trips and revenue declining - requires attention")
        
        if avg_trip_value > 25:
            insights.append("💰 Above-average trip value indicates premium service")
        elif avg_trip_value < 15:
            insights.append("💡 Low trip value - opportunity for upselling")
        
        avg_passengers_per_trip = total_passengers / total_trips if total_trips > 0 else 0
        if avg_passengers_per_trip > 1.5:
            insights.append("👥 Good passenger density - efficient service")
        
        if avg_speed > 15:
            insights.append("🚗 Good average speed - efficient routes")
        elif avg_speed < 10:
            insights.append("⚠️ Low average speed - traffic or route optimization needed")
        
        for insight in insights:
            st.write(f"• {insight}")
    
    with insights_col2:
        st.subheader("Key Ratios")
        
        # Calculate and display key ratios
        revenue_per_passenger = total_revenue / total_passengers if total_passengers > 0 else 0
        st.metric("Revenue per Passenger", format_number(revenue_per_passenger, "currency"))
        
        revenue_per_mile = total_revenue / (total_trips * avg_distance) if (total_trips * avg_distance) > 0 else 0
        st.metric("Revenue per Mile", format_number(revenue_per_mile, "currency"))
        
        trips_per_day = total_trips / 90  # Assuming 3-month period (90 days)
        st.metric("Average Trips per Day", format_number(trips_per_day, "number"))
    
    st.markdown("---")
    
    # Detailed Data Table
    with st.expander("📋 Detailed Raw Data", expanded=False):
        st.subheader("Complete Dataset")
        
        # Create a more detailed breakdown
        st.write("**Current Period Metrics (2024):**")
        
        data_dict = {
            'Metric': [
                'Total Trips',
                'Total Revenue',
                'Average Trip Value',
                'Total Passengers',
                'Average Distance',
                'Average Duration',
                'Trips Growth %',
                'Revenue Growth %'
            ],
            'Value': [
                format_number(total_trips),
                format_number(total_revenue, "currency"),
                format_number(avg_trip_value, "currency"),
                format_number(total_passengers),
                f"{avg_distance:.2f} miles",
                f"{avg_duration:.1f} minutes",
                f"{trips_growth_pct:+.1f}%" if not pd.isna(trips_growth_pct) else "N/A",
                f"{revenue_growth_pct:+.1f}%" if not pd.isna(revenue_growth_pct) else "N/A"
            ]
        }
        
        summary_df = pd.DataFrame(data_dict)
        st.dataframe(summary_df, use_container_width=True, hide_index=True)
        
        st.write("**Original Data:**")
        st.dataframe(df, use_container_width=True)