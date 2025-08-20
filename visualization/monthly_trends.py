import streamlit as st
import pandas as pd
from datetime import datetime

from snowflake_connector import load_data_from_source
# Custom CSS for monthly trends styling
def load_trends_css():
    st.markdown("""
    <style>
        .trends-card {
            background-color: #f8f9fa;
            padding: 1.5rem;
            border-radius: 0.8rem;
            border-left: 4px solid #6f42c1;
            margin-bottom: 1.5rem;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            transition: transform 0.2s;
        }
        .trends-card:hover {
            transform: translateY(-2px);
        }
        .month-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 1rem;
            border-radius: 0.5rem;
            margin: 0.5rem 0;
            text-align: center;
        }
        .growth-positive {
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
            color: white;
            padding: 0.75rem;
            border-radius: 0.5rem;
            margin: 0.5rem;
            text-align: center;
        }
        .growth-negative {
            background: linear-gradient(135deg, #fc4a1a 0%, #f7b733 100%);
            color: white;
            padding: 0.75rem;
            border-radius: 0.5rem;
            margin: 0.5rem;
            text-align: center;
        }
        .growth-neutral {
            background: linear-gradient(135deg, #bdc3c7 0%, #95a5a6 100%);
            color: white;
            padding: 0.75rem;
            border-radius: 0.5rem;
            margin: 0.5rem;
            text-align: center;
        }
        .trend-metric {
            font-size: 1.8em;
            font-weight: bold;
            color: #2c3e50;
            text-align: center;
        }
        .service-header {
            background: linear-gradient(90deg, #6f42c1, #8b5fbf);
            color: white;
            padding: 1rem;
            border-radius: 0.5rem;
            margin: 1rem 0;
            text-align: center;
            font-size: 1.2em;
            font-weight: bold;
        }
        .month-name {
            font-size: 1.1em;
            font-weight: bold;
            color: #495057;
        }
        .trend-arrow {
            font-size: 1.5em;
            margin: 0 0.5rem;
        }
        .summary-box {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 1.5rem;
            border-radius: 0.8rem;
            margin: 1rem 0;
            text-align: center;
        }
        .chart-container {
            background-color: white;
            padding: 1rem;
            border-radius: 0.5rem;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin: 1rem 0;
        }
    </style>
    """, unsafe_allow_html=True)

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_monthly_data():
    """Load monthly trends data from Snowflake"""
    try:        
        query = "SELECT * FROM vw_monthly_trends ORDER BY year, month, service_name"
        df = load_data_from_source(query)
        
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
    else:
        return str(num)

def get_growth_class_and_arrow(growth_pct):
    """Get CSS class and arrow for growth display"""
    if pd.isna(growth_pct):
        return "growth-neutral", "➖", "No Data"
    elif growth_pct > 0:
        return "growth-positive", "📈", f"+{growth_pct:.1f}%"
    elif growth_pct < 0:
        return "growth-negative", "📉", f"{growth_pct:.1f}%"
    else:
        return "growth-neutral", "➖", "0.0%"

def create_trend_chart_data(df, metric_col, service_name):
    """Create data for simple trend visualization"""
    service_data = df[df['SERVICE_NAME'] == service_name].sort_values(['YEAR', 'MONTH'])
    if service_data.empty:
        return None
    
    months = []
    values = []
    for _, row in service_data.iterrows():
        month_label = f"{row['MONTH_NAME'][:3]} {row['YEAR']}"
        months.append(month_label)
        values.append(row[metric_col])
    
    return months, values

def show_monthly_trends():
    """Main function to display the monthly trends dashboard"""
    
    # Load CSS
    load_trends_css()
    
    # Header
    st.markdown('<div class="service-header">📈 Monthly Trends Dashboard</div>', unsafe_allow_html=True)
    
    # Load data
    with st.spinner("Loading monthly trends data from Snowflake..."):
        df = load_monthly_data()
    
    if df is None or df.empty:
        st.error("No monthly trends data available. Please check your database connection.")
        return
    
    # Auto-refresh indicator
    st.caption("Monthly trends data auto-refreshes every 5 minutes")
    st.markdown("---")
    
    # Summary Statistics
    st.markdown('<div class="service-header">📊 Overall Trends Summary</div>', unsafe_allow_html=True)
    
    # Calculate overall statistics
    total_months = len(df)
    unique_services = df['SERVICE_NAME'].nunique()
    date_range = f"{df['YEAR'].min()} - {df['YEAR'].max()}"
    
    # Calculate average growth rates (excluding NaN values)
    avg_trip_growth = df['TRIPS_MOM_GROWTH'].dropna().mean()
    avg_revenue_growth = df['REVENUE_MOM_GROWTH'].dropna().mean()
    
    summary_col1, summary_col2, summary_col3, summary_col4 = st.columns(4)
    
    with summary_col1:
        st.markdown(f"""
        <div class="summary-box">
            <div class="trend-metric">{total_months}</div>
            <div>Total Data Points</div>
        </div>
        """, unsafe_allow_html=True)
    
    with summary_col2:
        st.markdown(f"""
        <div class="summary-box">
            <div class="trend-metric">{unique_services}</div>
            <div>Services Tracked</div>
        </div>
        """, unsafe_allow_html=True)
    
    with summary_col3:
        growth_class, growth_arrow, growth_text = get_growth_class_and_arrow(avg_trip_growth)
        st.markdown(f"""
        <div class="summary-box">
            <div class="trend-metric">{growth_arrow} {avg_trip_growth:.1f}%</div>
            <div>Avg Trip Growth</div>
        </div>
        """, unsafe_allow_html=True)
    
    with summary_col4:
        rev_growth_class, rev_growth_arrow, rev_growth_text = get_growth_class_and_arrow(avg_revenue_growth)
        st.markdown(f"""
        <div class="summary-box">
            <div class="trend-metric">{rev_growth_arrow} {avg_revenue_growth:.1f}%</div>
            <div>Avg Revenue Growth</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Service selector
    st.markdown('<div class="service-header">🎯 Service-Specific Analysis</div>', unsafe_allow_html=True)
    
    services = sorted(df['SERVICE_NAME'].unique())
    selected_service = st.selectbox("Select a service for detailed analysis:", services, key="service_selector")
    
    if selected_service:
        service_df = df[df['SERVICE_NAME'] == selected_service].sort_values(['YEAR', 'MONTH'])
        
        st.markdown(f"### 📊 {selected_service} - Monthly Performance")
        
        # Create columns for each month's data
        for _, row in service_df.iterrows():
            st.markdown(f"""
            <div class="trends-card">
                <div class="month-card">
                    <strong>{row['MONTH_NAME']} {row['YEAR']}</strong>
                </div>
            """, unsafe_allow_html=True)
            
            # Metrics for this month
            month_col1, month_col2, month_col3, month_col4 = st.columns(4)
            
            with month_col1:
                st.metric("Trips", format_number(row['TRIP_COUNT'], "number"))
                
                # Trip growth
                if not pd.isna(row['TRIPS_MOM_GROWTH']):
                    trip_class, trip_arrow, trip_text = get_growth_class_and_arrow(row['TRIPS_MOM_GROWTH'])
                    st.markdown(f'<div class="{trip_class}">MoM: {trip_arrow} {trip_text}</div>', unsafe_allow_html=True)
            
            with month_col2:
                st.metric("Revenue", format_number(row['TOTAL_REVENUE'], "currency"))
                
                # Revenue growth
                if not pd.isna(row['REVENUE_MOM_GROWTH']):
                    rev_class, rev_arrow, rev_text = get_growth_class_and_arrow(row['REVENUE_MOM_GROWTH'])
                    st.markdown(f'<div class="{rev_class}">MoM: {rev_arrow} {rev_text}</div>', unsafe_allow_html=True)
            
            with month_col3:
                st.metric("Avg Trip Value", format_number(row['AVG_REVENUE_PER_TRIP'], "currency"))
                st.caption("Revenue per trip")
            
            with month_col4:
                st.metric("Passengers", format_number(row['TOTAL_PASSENGERS'], "number"))
                st.metric("Avg Distance", f"{row['AVG_DISTANCE']:.1f} mi")
            
            st.markdown("</div>", unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Growth Trends Analysis
    st.markdown('<div class="service-header">📈 Growth Trends Analysis</div>', unsafe_allow_html=True)
    
    # Best and worst performing months
    growth_df = df.dropna(subset=['TRIPS_MOM_GROWTH', 'REVENUE_MOM_GROWTH'])
    
    if not growth_df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🏆 Top Performers")
            
            # Best trip growth
            best_trip_growth = growth_df.loc[growth_df['TRIPS_MOM_GROWTH'].idxmax()]
            st.markdown(f"""
            **Best Trip Growth:**
            - {best_trip_growth['SERVICE_NAME']}
            - {best_trip_growth['MONTH_NAME']} {best_trip_growth['YEAR']}
            - 📈 +{best_trip_growth['TRIPS_MOM_GROWTH']:.1f}% MoM
            """)
            
            # Best revenue growth
            best_revenue_growth = growth_df.loc[growth_df['REVENUE_MOM_GROWTH'].idxmax()]
            st.markdown(f"""
            **Best Revenue Growth:**
            - {best_revenue_growth['SERVICE_NAME']}
            - {best_revenue_growth['MONTH_NAME']} {best_revenue_growth['YEAR']}
            - 📈 +{best_revenue_growth['REVENUE_MOM_GROWTH']:.1f}% MoM
            """)
        
        with col2:
            st.subheader("⚠️ Areas for Attention")
            
            # Worst trip growth
            worst_trip_growth = growth_df.loc[growth_df['TRIPS_MOM_GROWTH'].idxmin()]
            st.markdown(f"""
            **Lowest Trip Growth:**
            - {worst_trip_growth['SERVICE_NAME']}
            - {worst_trip_growth['MONTH_NAME']} {worst_trip_growth['YEAR']}
            - 📉 {worst_trip_growth['TRIPS_MOM_GROWTH']:.1f}% MoM
            """)
            
            # Worst revenue growth
            worst_revenue_growth = growth_df.loc[growth_df['REVENUE_MOM_GROWTH'].idxmin()]
            st.markdown(f"""
            **Lowest Revenue Growth:**
            - {worst_revenue_growth['SERVICE_NAME']}
            - {worst_revenue_growth['MONTH_NAME']} {worst_revenue_growth['YEAR']}
            - 📉 {worst_revenue_growth['REVENUE_MOM_GROWTH']:.1f}% MoM
            """)
    
    st.markdown("---")
    
    # Service Performance Comparison
    st.markdown('<div class="service-header">🔄 Service Performance Comparison</div>', unsafe_allow_html=True)
    
    # Calculate average performance by service
    service_performance = df.groupby('SERVICE_NAME').agg({
        'TRIP_COUNT': 'mean',
        'TOTAL_REVENUE': 'mean',
        'AVG_REVENUE_PER_TRIP': 'mean',
        'TRIPS_MOM_GROWTH': 'mean',
        'REVENUE_MOM_GROWTH': 'mean'
    }).round(2)
    
    # Sort by average revenue
    service_performance = service_performance.sort_values('TOTAL_REVENUE', ascending=False)
    
    for service_name, row in service_performance.iterrows():
        st.markdown(f"""
        <div class="trends-card">
            <h4>🚗 {service_name}</h4>
        </div>
        """, unsafe_allow_html=True)
        
        perf_col1, perf_col2, perf_col3, perf_col4 = st.columns(4)
        
        with perf_col1:
            st.metric("Avg Monthly Trips", format_number(row['TRIP_COUNT'], "number"))
        
        with perf_col2:
            st.metric("Avg Monthly Revenue", format_number(row['TOTAL_REVENUE'], "currency"))
        
        with perf_col3:
            st.metric("Avg Trip Value", format_number(row['AVG_REVENUE_PER_TRIP'], "currency"))
        
        with perf_col4:
            # Show average growth rates
            avg_trip_growth = row['TRIPS_MOM_GROWTH']
            avg_rev_growth = row['REVENUE_MOM_GROWTH']
            
            if not pd.isna(avg_trip_growth):
                st.metric("Avg Trip Growth", f"{avg_trip_growth:+.1f}%")
            if not pd.isna(avg_rev_growth):
                st.metric("Avg Revenue Growth", f"{avg_rev_growth:+.1f}%")
    
    st.markdown("---")
    
    # Detailed Data Table
    with st.expander("📋 Complete Monthly Trends Data", expanded=False):
        st.subheader("Full Dataset")
        
        # Format dataframe for display
        display_df = df.copy()
        
        # Format columns
        display_df['TRIP_COUNT'] = display_df['TRIP_COUNT'].apply(lambda x: f"{x:,}")
        display_df['TOTAL_REVENUE'] = display_df['TOTAL_REVENUE'].apply(lambda x: f"${x:,.2f}")
        display_df['AVG_REVENUE_PER_TRIP'] = display_df['AVG_REVENUE_PER_TRIP'].apply(lambda x: f"${x:.2f}")
        display_df['TOTAL_PASSENGERS'] = display_df['TOTAL_PASSENGERS'].apply(lambda x: f"{x:,}")
        display_df['AVG_DISTANCE'] = display_df['AVG_DISTANCE'].apply(lambda x: f"{x:.2f} mi")
        display_df['TRIPS_MOM_GROWTH'] = display_df['TRIPS_MOM_GROWTH'].apply(lambda x: f"{x:+.1f}%" if not pd.isna(x) else "N/A")
        display_df['REVENUE_MOM_GROWTH'] = display_df['REVENUE_MOM_GROWTH'].apply(lambda x: f"{x:+.1f}%" if not pd.isna(x) else "N/A")
        
        # Select columns for display
        display_columns = [
            'YEAR', 'MONTH_NAME', 'SERVICE_NAME', 'TRIP_COUNT', 'TOTAL_REVENUE',
            'AVG_REVENUE_PER_TRIP', 'TOTAL_PASSENGERS', 'AVG_DISTANCE',
            'TRIPS_MOM_GROWTH', 'REVENUE_MOM_GROWTH'
        ]
        
        st.dataframe(display_df[display_columns], use_container_width=True)
        
        # Download option
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download Full Data as CSV",
            data=csv,
            file_name=f"monthly_trends_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )