import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime

from snowflake_connector import load_data_from_source

# Custom CSS for airport traffic styling
def load_airport_css():
    st.markdown("""
    <style>
        .airport-card {
            background-color: #f8f9fa;
            padding: 1.5rem;
            border-radius: 0.8rem;
            border-left: 4px solid #17a2b8;
            margin-bottom: 1.5rem;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            transition: transform 0.2s;
        }
        .airport-card:hover {
            transform: translateY(-2px);
        }
        .airport-header {
            background: linear-gradient(90deg, #17a2b8, #20c997);
            color: white;
            padding: 1rem;
            border-radius: 0.5rem;
            margin: 1rem 0;
            text-align: center;
            font-size: 1.2em;
            font-weight: bold;
        }
        .trip-type-card {
            background: linear-gradient(135deg, #fd7e14 0%, #ffc107 100%);
            color: white;
            padding: 1rem;
            border-radius: 0.5rem;
            margin: 0.5rem 0;
            text-align: center;
        }
        .from-airport {
            background: linear-gradient(135deg, #28a745 0%, #20c997 100%);
            color: white;
            padding: 1rem;
            border-radius: 0.5rem;
            margin: 0.5rem;
            text-align: center;
        }
        .to-airport {
            background: linear-gradient(135deg, #007bff 0%, #6f42c1 100%);
            color: white;
            padding: 1rem;
            border-radius: 0.5rem;
            margin: 0.5rem;
            text-align: center;
        }
        .airport-metric {
            font-size: 1.8em;
            font-weight: bold;
            color: #2c3e50;
            text-align: center;
        }
        .summary-box {
            background: linear-gradient(135deg, #17a2b8 0%, #20c997 100%);
            color: white;
            padding: 1.5rem;
            border-radius: 0.8rem;
            margin: 1rem 0;
            text-align: center;
        }
        .time-slot {
            background: linear-gradient(135deg, #6f42c1 0%, #e83e8c 100%);
            color: white;
            padding: 0.75rem;
            border-radius: 0.5rem;
            margin: 0.25rem;
            text-align: center;
            font-size: 0.9em;
        }
        .airport-name {
            font-size: 1.1em;
            font-weight: bold;
            color: #495057;
            text-align: center;
            padding: 0.5rem;
            background-color: rgba(23, 162, 184, 0.1);
            border-radius: 0.3rem;
            margin: 0.5rem 0;
        }
        .performance-indicator {
            display: inline-block;
            padding: 0.5rem 1rem;
            border-radius: 2rem;
            color: white;
            font-weight: bold;
            margin: 0.25rem;
        }
        .high-traffic {
            background: linear-gradient(135deg, #dc3545 0%, #fd7e14 100%);
        }
        .medium-traffic {
            background: linear-gradient(135deg, #ffc107 0%, #fd7e14 100%);
        }
        .low-traffic {
            background: linear-gradient(135deg, #28a745 0%, #20c997 100%);
        }
        .service-card h4 {
            color: #2c3e50 !important;
        }
    </style>
    """, unsafe_allow_html=True)

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_airport_data():
    """Load airport traffic data from Snowflake"""
    try:        
        query = "SELECT * FROM vw_airport_traffic ORDER BY year DESC, month_name, trip_type, airport_name"
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
    elif format_type == "number":
        if num >= 1_000_000:
            return f"{num/1_000_000:.1f}M"
        elif num >= 1_000:
            return f"{num/1_000:.0f}K"
        else:
            return f"{num:,.0f}"
    elif format_type == "decimal":
        return f"{num:.2f}"
    elif format_type == "duration":
        return f"{num:.0f} min"
    elif format_type == "distance":
        return f"{num:.1f} mi"
    else:
        return str(num)

def get_traffic_level(trip_count):
    """Determine traffic level and return appropriate styling"""
    if trip_count >= 10000:
        return "high-traffic", "🔴 High Traffic"
    elif trip_count >= 5000:
        return "medium-traffic", "🟡 Medium Traffic"
    else:
        return "low-traffic", "🟢 Low Traffic"

def create_airport_comparison_chart(df):
    """Create airport comparison chart"""
    airport_summary = df[df['TRIP_TYPE'] != 'Non-Airport'].groupby(['AIRPORT_NAME', 'TRIP_TYPE']).agg({
        'TRIP_COUNT': 'sum',
        'TOTAL_REVENUE': 'sum',
        'AVG_FARE': 'mean'
    }).reset_index()
    
    fig = px.bar(
        airport_summary,
        x='AIRPORT_NAME',
        y='TRIP_COUNT',
        color='TRIP_TYPE',
        title='Airport Traffic Volume by Direction',
        labels={'TRIP_COUNT': 'Total Trips', 'AIRPORT_NAME': 'Airport'},
        color_discrete_map={'From Airport': '#28a745', 'To Airport': '#007bff'}
    )
    
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#2c3e50'),
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    return fig

def create_time_analysis_chart(df):
    """Create time period analysis chart"""
    time_analysis = df.groupby(['TIME_PERIOD', 'TRIP_TYPE']).agg({
        'TRIP_COUNT': 'sum',
        'AVG_FARE': 'mean'
    }).reset_index()
    
    # Filter out Non-Airport trips for cleaner visualization
    time_analysis = time_analysis[time_analysis['TRIP_TYPE'] != 'Non-Airport']
    
    fig = px.line(
        time_analysis,
        x='TIME_PERIOD',
        y='TRIP_COUNT',
        color='TRIP_TYPE',
        title='Airport Traffic Patterns by Time of Day',
        labels={'TRIP_COUNT': 'Number of Trips', 'TIME_PERIOD': 'Time Period'},
        markers=True,
        color_discrete_map={'From Airport': '#28a745', 'To Airport': '#007bff'}
    )
    
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#2c3e50'),
        showlegend=True,
        xaxis_tickangle=-45
    )
    
    return fig

def show_airport_traffic():
    """Main function to display the airport traffic dashboard"""
    
    # Load CSS
    load_airport_css()
    
    # Header
    st.markdown('<div class="airport-header">✈️ Airport Traffic Dashboard</div>', unsafe_allow_html=True)
    
    # Load data
    with st.spinner("Loading airport traffic data from Snowflake..."):
        df = load_airport_data()
    
    if df is None or df.empty:
        st.error("No airport traffic data available. Please check your database connection.")
        return
    
    # Auto-refresh indicator
    st.caption("Airport traffic data auto-refreshes every 5 minutes")
    st.markdown("---")
    
    # Overall Summary Statistics
    st.markdown('<div class="airport-header">📊 Airport Traffic Overview</div>', unsafe_allow_html=True)
    
    # Calculate key metrics
    total_airport_trips = df[df['TRIP_TYPE'] != 'Non-Airport']['TRIP_COUNT'].sum()
    total_airport_revenue = df[df['TRIP_TYPE'] != 'Non-Airport']['TOTAL_REVENUE'].sum()
    avg_airport_fare = df[df['TRIP_TYPE'] != 'Non-Airport']['AVG_FARE'].mean()
    unique_airports = df[df['TRIP_TYPE'] != 'Non-Airport']['AIRPORT_NAME'].nunique()
    
    # From vs To Airport breakdown
    from_airport_trips = df[df['TRIP_TYPE'] == 'From Airport']['TRIP_COUNT'].sum()
    to_airport_trips = df[df['TRIP_TYPE'] == 'To Airport']['TRIP_COUNT'].sum()
    
    summary_col1, summary_col2, summary_col3, summary_col4 = st.columns(4)
    
    with summary_col1:
        st.markdown(f"""
        <div class="summary-box">
            <div class="airport-metric">{format_number(total_airport_trips)}</div>
            <div>Total Airport Trips</div>
        </div>
        """, unsafe_allow_html=True)
    
    with summary_col2:
        st.markdown(f"""
        <div class="summary-box">
            <div class="airport-metric">{format_number(total_airport_revenue, 'currency')}</div>
            <div>Total Revenue</div>
        </div>
        """, unsafe_allow_html=True)
    
    with summary_col3:
        st.markdown(f"""
        <div class="summary-box">
            <div class="airport-metric">{format_number(avg_airport_fare, 'currency')}</div>
            <div>Average Fare</div>
        </div>
        """, unsafe_allow_html=True)
    
    with summary_col4:
        st.markdown(f"""
        <div class="summary-box">
            <div class="airport-metric">{unique_airports}</div>
            <div>Airports Served</div>
        </div>
        """, unsafe_allow_html=True)
    
    # Traffic Direction Analysis
    st.markdown("### 🔄 Traffic Direction Analysis")
    direction_col1, direction_col2 = st.columns(2)
    
    with direction_col1:
        st.markdown(f"""
        <div class="from-airport">
            <h4>✈️ From Airport</h4>
            <div class="airport-metric">{format_number(from_airport_trips)}</div>
            <div>trips ({from_airport_trips/(from_airport_trips+to_airport_trips)*100:.1f}%)</div>
        </div>
        """, unsafe_allow_html=True)
    
    with direction_col2:
        st.markdown(f"""
        <div class="to-airport">
            <h4>🛬 To Airport</h4>
            <div class="airport-metric">{format_number(to_airport_trips)}</div>
            <div>trips ({to_airport_trips/(from_airport_trips+to_airport_trips)*100:.1f}%)</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Airport Performance Analysis
    st.markdown('<div class="airport-header">🏆 Airport Performance Analysis</div>', unsafe_allow_html=True)
    
    # Group by airport and calculate totals
    airport_performance = df[df['TRIP_TYPE'] != 'Non-Airport'].groupby('AIRPORT_NAME').agg({
        'TRIP_COUNT': 'sum',
        'TOTAL_REVENUE': 'sum',
        'AVG_FARE': 'mean',
        'AVG_DISTANCE': 'mean',
        'AVG_DURATION': 'mean'
    }).round(2).sort_values('TRIP_COUNT', ascending=False)
    
    for airport_name, row in airport_performance.iterrows():
        traffic_class, traffic_label = get_traffic_level(row['TRIP_COUNT'])
        
        st.markdown(f"""
        <div class="airport-card">
            <div class="airport-name">✈️ {airport_name}</div>
            <div class="performance-indicator {traffic_class}">{traffic_label}</div>
        </div>
        """, unsafe_allow_html=True)
        
        airport_col1, airport_col2, airport_col3, airport_col4 = st.columns(4)
        
        with airport_col1:
            st.metric("Total Trips", format_number(row['TRIP_COUNT'], "number"))
        
        with airport_col2:
            st.metric("Total Revenue", format_number(row['TOTAL_REVENUE'], "currency"))
        
        with airport_col3:
            st.metric("Avg Fare", format_number(row['AVG_FARE'], "currency"))
            st.caption(f"Avg Distance: {format_number(row['AVG_DISTANCE'], 'distance')}")
        
        with airport_col4:
            st.metric("Avg Duration", format_number(row['AVG_DURATION'], "duration"))
            
            # Show trip direction breakdown for this airport
            airport_directions = df[(df['AIRPORT_NAME'] == airport_name) & (df['TRIP_TYPE'] != 'Non-Airport')].groupby('TRIP_TYPE')['TRIP_COUNT'].sum()
            if len(airport_directions) > 0:
                direction_text = " | ".join([f"{direction}: {format_number(count)}" for direction, count in airport_directions.items()])
                st.caption(f"Breakdown: {direction_text}")
    
    st.markdown("---")
    
    # Time Analysis
    st.markdown('<div class="airport-header">⏰ Time Pattern Analysis</div>', unsafe_allow_html=True)
    
    # Time period analysis
    time_patterns = df[df['TRIP_TYPE'] != 'Non-Airport'].groupby(['TIME_PERIOD', 'TRIP_TYPE']).agg({
        'TRIP_COUNT': 'sum',
        'AVG_FARE': 'mean'
    }).reset_index()
    
    # Show time period breakdown
    unique_time_periods = sorted(df['TIME_PERIOD'].unique())
    
    st.markdown("### 🕒 Peak Hours Analysis")
    
    time_cols = st.columns(len(unique_time_periods))
    for i, time_period in enumerate(unique_time_periods):
        with time_cols[i]:
            period_data = df[(df['TIME_PERIOD'] == time_period) & (df['TRIP_TYPE'] != 'Non-Airport')]
            total_trips = period_data['TRIP_COUNT'].sum()
            avg_fare = period_data['AVG_FARE'].mean()
            
            st.markdown(f"""
            <div class="time-slot">
                <strong>{time_period}</strong><br>
                {format_number(total_trips)} trips<br>
                Avg: {format_number(avg_fare, 'currency')}
            </div>
            """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Service Analysis
    st.markdown('<div class="airport-header">🚗 Service Type Analysis</div>', unsafe_allow_html=True)
    
    # Group by service name
    service_analysis = df[df['TRIP_TYPE'] != 'Non-Airport'].groupby('SERVICE_NAME').agg({
        'TRIP_COUNT': 'sum',
        'TOTAL_REVENUE': 'sum',
        'AVG_FARE': 'mean',
        'AVG_DISTANCE': 'mean'
    }).round(2).sort_values('TRIP_COUNT', ascending=False)
    
    for service_name, row in service_analysis.iterrows():
        st.markdown(f"""
        <div class="airport-card service-card">
            <h4>🚕 {service_name} - Airport Service</h4>
        </div>
        """, unsafe_allow_html=True)
        
        service_col1, service_col2, service_col3, service_col4 = st.columns(4)
        
        with service_col1:
            st.metric("Airport Trips", format_number(row['TRIP_COUNT'], "number"))
        
        with service_col2:
            st.metric("Airport Revenue", format_number(row['TOTAL_REVENUE'], "currency"))
        
        with service_col3:
            st.metric("Avg Airport Fare", format_number(row['AVG_FARE'], "currency"))
        
        with service_col4:
            st.metric("Avg Distance", format_number(row['AVG_DISTANCE'], "distance"))
            
            # Show percentage of total airport trips
            pct_of_total = (row['TRIP_COUNT'] / total_airport_trips) * 100
            st.caption(f"{pct_of_total:.1f}% of airport trips")
    
    st.markdown("---")
    
    # Monthly Trends
    st.markdown('<div class="airport-header">📅 Monthly Trends</div>', unsafe_allow_html=True)
    
    # Monthly analysis
    monthly_trends = df[df['TRIP_TYPE'] != 'Non-Airport'].groupby(['YEAR', 'MONTH_NAME']).agg({
        'TRIP_COUNT': 'sum',
        'TOTAL_REVENUE': 'sum',
        'AVG_FARE': 'mean'
    }).reset_index().sort_values(['YEAR', 'MONTH_NAME'])
    
    if not monthly_trends.empty:
        # Create month-year column for better display
        monthly_trends['PERIOD'] = monthly_trends['YEAR'].astype(str) + ' - ' + monthly_trends['MONTH_NAME']
        
        # Show recent months
        recent_months = monthly_trends.tail(6)  # Last 6 months
        
        for _, row in recent_months.iterrows():
            st.markdown(f"""
            <div class="airport-card">
                <div class="trip-type-card">
                    <strong>📅 {row['PERIOD']}</strong>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            monthly_col1, monthly_col2, monthly_col3 = st.columns(3)
            
            with monthly_col1:
                st.metric("Airport Trips", format_number(row['TRIP_COUNT'], "number"))
            
            with monthly_col2:
                st.metric("Airport Revenue", format_number(row['TOTAL_REVENUE'], "currency"))
            
            with monthly_col3:
                st.metric("Avg Fare", format_number(row['AVG_FARE'], "currency"))
    
    st.markdown("---")
    
    # Charts Section
    st.markdown('<div class="airport-header">📊 Visual Analytics</div>', unsafe_allow_html=True)
    
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        # Airport comparison chart
        airport_chart = create_airport_comparison_chart(df)
        st.plotly_chart(airport_chart, use_container_width=True)
    
    with chart_col2:
        # Time analysis chart
        time_chart = create_time_analysis_chart(df)
        st.plotly_chart(time_chart, use_container_width=True)
    
    st.markdown("---")
    
    # Detailed Data Table
    with st.expander("📋 Complete Airport Traffic Data", expanded=False):
        st.subheader("Full Dataset")
        
        # Format dataframe for display
        display_df = df.copy()
        
        # Format columns for better display
        display_df['TRIP_COUNT'] = display_df['TRIP_COUNT'].apply(lambda x: f"{x:,}")
        display_df['TOTAL_REVENUE'] = display_df['TOTAL_REVENUE'].apply(lambda x: f"${x:,.2f}")
        display_df['AVG_FARE'] = display_df['AVG_FARE'].apply(lambda x: f"${x:.2f}")
        display_df['AVG_DISTANCE'] = display_df['AVG_DISTANCE'].apply(lambda x: f"{x:.2f} mi")
        display_df['AVG_DURATION'] = display_df['AVG_DURATION'].apply(lambda x: f"{x:.0f} min")
        
        # Select and reorder columns for display
        display_columns = [
            'YEAR', 'MONTH_NAME', 'TRIP_TYPE', 'AIRPORT_NAME', 'SERVICE_NAME',
            'TIME_PERIOD', 'TRIP_COUNT', 'TOTAL_REVENUE', 'AVG_FARE',
            'AVG_DISTANCE', 'AVG_DURATION'
        ]
        
        st.dataframe(display_df[display_columns], use_container_width=True)
        
        # Download option
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download Full Airport Data as CSV",
            data=csv,
            file_name=f"airport_traffic_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )