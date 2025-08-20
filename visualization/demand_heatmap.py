import streamlit as st
import snowflake.connector
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.figure_factory as ff

# Custom CSS for demand heatmap styling
def load_heatmap_css():
    st.markdown("""
    <style>
        .heatmap-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 1.5rem;
            border-radius: 0.75rem;
            margin-bottom: 1rem;
            box-shadow: 0 4px 15px rgba(0,0,0,0.2);
        }
        .demand-header {
            background: linear-gradient(90deg, #667eea, #764ba2);
            color: white;
            padding: 1rem;
            border-radius: 0.5rem;
            margin: 1rem 0;
            text-align: center;
            font-size: 1.2em;
            font-weight: bold;
        }
        .page-selector {
            background-color: #f8f9fa !important;
            color: #2c3e50 !important;
            padding: 1rem;
            border-radius: 0.5rem;
            border-left: 4px solid #667eea;
            margin-bottom: 1rem;
        }
        .peak-indicator {
            display: inline-block;
            padding: 0.5rem 1rem;
            border-radius: 2rem;
            font-weight: bold;
            margin: 0.25rem;
            font-size: 0.9em;
        }
        .peak-high {
            background: linear-gradient(135deg, #ff6b6b, #ee5a5a);
            color: white;
            box-shadow: 0 2px 10px rgba(255,107,107,0.3);
        }
        .peak-medium {
            background: linear-gradient(135deg, #feca57, #ff9ff3);
            color: #2c3e50;
            box-shadow: 0 2px 10px rgba(254,202,87,0.3);
        }
        .peak-low {
            background: linear-gradient(135deg, #48dbfb, #0abde3);
            color: white;
            box-shadow: 0 2px 10px rgba(72,219,251,0.3);
        }
        .time-slot {
            background-color: #f8f9fa;
            color: #2c3e50;
            padding: 0.75rem;
            border-radius: 0.5rem;
            border-left: 4px solid #667eea;
            margin: 0.5rem 0;
            transition: all 0.3s ease;
        }
        .time-slot:hover {
            transform: translateX(5px);
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .insight-box {
            background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%);
            padding: 1rem;
            border-radius: 0.5rem;
            margin: 1rem 0;
            border-left: 4px solid #667eea;
        }
        .service-tab {
            background: linear-gradient(135deg, #d299c2 0%, #fef9d7 100%);
            padding: 0.5rem 1rem;
            border-radius: 1rem;
            margin: 0.25rem;
            font-weight: bold;
            display: inline-block;
            cursor: pointer;
            transition: all 0.3s ease;
        }
        .service-tab:hover {
            transform: scale(1.05);
        }
    </style>
    """, unsafe_allow_html=True)

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_demand_data():
    """Load hourly demand heatmap data from Snowflake"""
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
        
        query = "SELECT * FROM vw_hourly_demand_heatmap ORDER BY day_of_week, hour"
        df = pd.read_sql(query, conn)
        # Normalize day names (map abbreviations to full names)
        day_mapping = {
            "Sun": "Sunday",
            "Mon": "Monday",
            "Tue": "Tuesday",
            "Wed": "Wednesday",
            "Thu": "Thursday",
            "Fri": "Friday",
            "Sat": "Saturday"
        }
        if 'DAY_NAME' in df.columns:
            df['DAY_NAME'] = df['DAY_NAME'].map(day_mapping).fillna(df['DAY_NAME'])
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
        return f"${num:.2f}"
    elif format_type == "percentage":
        return f"{num:.1f}%"
    elif format_type == "number":
        if num >= 1_000:
            return f"{num/1_000:.1f}K"
        else:
            return f"{num:,.0f}"
    else:
        return str(num)

def get_peak_indicator(intensity):
    """Get peak demand indicator based on intensity"""
    if intensity >= 80:
        return "peak-high", "🔥 Peak Hours"
    elif intensity >= 50:
        return "peak-medium", "⚡ High Demand"
    else:
        return "peak-low", "💤 Low Demand"

def create_overall_heatmap(df):
    """Create overall demand heatmap across all services"""
    # Aggregate data across all services
    agg_df = df.groupby(['DAY_NAME', 'HOUR']).agg({
        'TRIP_COUNT': 'sum',
        'DEMAND_INTENSITY': 'mean'
    }).reset_index()

    # Create pivot table for heatmap
    heatmap_data = agg_df.pivot(index='DAY_NAME', columns='HOUR', values='DEMAND_INTENSITY')
    # Fill NaN values with 0 for better visualization
    heatmap_data = heatmap_data.fillna(0)
    # Debugging: Show heatmap data after pivot
    # Reorder days
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    heatmap_data = heatmap_data.reindex(day_order)
    fig = px.imshow(
        heatmap_data,
        labels=dict(x="Hour of Day", y="Day of Week", color="Demand Intensity"),
        x=[f"{hour:02d}:00" for hour in range(24)],
        y=day_order,
        color_continuous_scale="Viridis",
        title="Overall Demand Intensity Heatmap",
        aspect="auto",
        zmin=0,
        zmax=100
    )

    fig.update_layout(
        height=400,
        title_font_size=16,
        xaxis_title="Hour of Day",
        yaxis_title="Day of Week",
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='black')
    )

    return fig

def create_service_heatmap(df, service_name):
    """Create heatmap for a specific service"""
    service_df = df[df['SERVICE_NAME'] == service_name]

    # Create pivot table
    heatmap_data = service_df.pivot(index='DAY_NAME', columns='HOUR', values='DEMAND_INTENSITY')

    # Reorder days
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    heatmap_data = heatmap_data.reindex(day_order)

    # Fill NaN values with 0 for better visualization
    heatmap_data = heatmap_data.fillna(0)
    # Debugging: Show heatmap data after pivot
    fig = px.imshow(
        heatmap_data,
        labels=dict(x="Hour of Day", y="Day of Week", color="Demand Intensity"),
        x=[f"{hour:02d}:00" for hour in range(24)],
        y=day_order,
        color_continuous_scale="RdYlBu_r",
        title=f"Demand Heatmap: {service_name}",
        aspect="auto",
        zmin=0,
        zmax=100
    )

    fig.update_layout(
        height=350,
        title_font_size=14,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='black')
    )

    return fig

def create_hourly_pattern_chart(df):
    """Create hourly demand pattern chart"""
    hourly_df = df.groupby(['HOUR', 'SERVICE_NAME']).agg({
        'TRIP_COUNT': 'sum',
        'AVG_REVENUE': 'mean'
    }).reset_index()
    
    fig = px.line(
        hourly_df,
        x='HOUR',
        y='TRIP_COUNT',
        color='SERVICE_NAME',
        title="Hourly Demand Patterns by Service",
        labels={'HOUR': 'Hour of Day', 'TRIP_COUNT': 'Total Trips'},
        markers=True
    )
    
    fig.update_layout(
        height=400,
        xaxis=dict(tickmode='linear', dtick=2),
        hovermode='x unified',
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='black')
    )
    
    return fig

def create_daily_pattern_chart(df):
    """Create daily demand pattern chart"""
    daily_df = df.groupby(['DAY_NAME', 'DAY_OF_WEEK', 'SERVICE_NAME']).agg({
        'TRIP_COUNT': 'sum',
        'AVG_REVENUE': 'mean'
    }).reset_index()
    
    # Sort by day of week
    daily_df = daily_df.sort_values('DAY_OF_WEEK')
    
    fig = px.bar(
        daily_df,
        x='DAY_NAME',
        y='TRIP_COUNT',
        color='SERVICE_NAME',
        title="Daily Demand Distribution by Service",
        labels={'DAY_NAME': 'Day of Week', 'TRIP_COUNT': 'Total Trips'},
        barmode='group'
    )
    
    fig.update_layout(
        height=400,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='black')
    )
    
    return fig

def create_revenue_intensity_scatter(df):
    """Create scatter plot of revenue vs demand intensity"""
    fig = px.scatter(
        df,
        x='DEMAND_INTENSITY',
        y='AVG_REVENUE',
        color='SERVICE_NAME',
        size='TRIP_COUNT',
        hover_data=['DAY_NAME', 'HOUR'],
        title="Revenue vs Demand Intensity",
        labels={'DEMAND_INTENSITY': 'Demand Intensity (%)', 'AVG_REVENUE': 'Average Revenue ($)'}
    )
    
    fig.update_layout(
        height=400,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='black')
    )
    
    return fig

def analyze_peak_hours(df):
    """Analyze peak hours and return insights"""
    insights = {}
    
    # Overall peak hours
    hourly_total = df.groupby('HOUR')['TRIP_COUNT'].sum().reset_index()
    peak_hour = hourly_total.loc[hourly_total['TRIP_COUNT'].idxmax(), 'HOUR']
    peak_trips = hourly_total.loc[hourly_total['TRIP_COUNT'].idxmax(), 'TRIP_COUNT']
    
    insights['overall_peak'] = {
        'hour': peak_hour,
        'trips': peak_trips
    }
    
    # Peak day
    daily_total = df.groupby('DAY_NAME')['TRIP_COUNT'].sum().reset_index()
    peak_day = daily_total.loc[daily_total['TRIP_COUNT'].idxmax(), 'DAY_NAME']
    peak_day_trips = daily_total.loc[daily_total['TRIP_COUNT'].idxmax(), 'TRIP_COUNT']
    
    insights['overall_peak_day'] = {
        'day': peak_day,
        'trips': peak_day_trips
    }
    
    # Service-specific peaks
    service_peaks = {}
    for service in df['SERVICE_NAME'].unique():
        service_data = df[df['SERVICE_NAME'] == service]
        service_hourly = service_data.groupby('HOUR')['TRIP_COUNT'].sum().reset_index()
        if not service_hourly.empty:
            peak_hour_service = service_hourly.loc[service_hourly['TRIP_COUNT'].idxmax(), 'HOUR']
            service_peaks[service] = peak_hour_service
    
    insights['service_peaks'] = service_peaks
    
    return insights

def show_demand_heatmap():
    """Main function to display the demand heatmap dashboard"""
    
    # Load CSS
    load_heatmap_css()
    
    # Header
    st.markdown('<div class="demand-header">🔥 Hourly Demand Heatmap Dashboard</div>', unsafe_allow_html=True)
    
    # Load data
    with st.spinner("Loading demand pattern data from Snowflake..."):
        df = load_demand_data()
    
    if df is None or df.empty:
        st.error("No demand heatmap data available. Please check your database connection.")
        return
    
    # Auto-refresh indicator
    st.caption("Demand data auto-refreshes every 5 minutes")
    st.markdown("---")
    
    # Quick insights section
    insights = analyze_peak_hours(df)
    
    st.markdown('<div class="demand-header">⚡ Quick Insights</div>', unsafe_allow_html=True)
    
    insight_col1, insight_col2, insight_col3, insight_col4 = st.columns(4)
    
    with insight_col1:
        st.metric(
            "Peak Hour", 
            f"{insights['overall_peak']['hour']:02d}:00",
            f"{format_number(insights['overall_peak']['trips'])} trips"
        )
    
    with insight_col2:
        st.metric(
            "Busiest Day", 
            insights['overall_peak_day']['day'],
            f"{format_number(insights['overall_peak_day']['trips'])} trips"
        )
    
    with insight_col3:
        total_hours_analyzed = len(df['HOUR'].unique())
        total_days_analyzed = len(df['DAY_NAME'].unique())
        st.metric("Time Coverage", f"{total_hours_analyzed}H × {total_days_analyzed}D", "Full Week")
    
    with insight_col4:
        avg_intensity = df['DEMAND_INTENSITY'].mean()
        st.metric("Avg Intensity", f"{avg_intensity:.1f}%", "Demand Score")
    
    st.markdown("---")
    
    # Main heatmap visualization
    st.markdown('<div class="demand-header">🗺️ Overall Demand Heatmap</div>', unsafe_allow_html=True)
    
    st.plotly_chart(create_overall_heatmap(df), use_container_width=True)
    
    # Service-specific analysis
    st.markdown('<div class="demand-header">🚗 Service-Specific Heatmaps</div>', unsafe_allow_html=True)
    
    services = df['SERVICE_NAME'].unique()
    service_tabs = st.tabs([f"🎯 {service}" for service in services])
    
    for i, service in enumerate(services):
        with service_tabs[i]:
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.plotly_chart(create_service_heatmap(df, service), use_container_width=True)
            
            with col2:
                service_data = df[df['SERVICE_NAME'] == service]
                
                # Service peak analysis
                service_peak_hour = insights['service_peaks'].get(service, 'N/A')
                service_total_trips = service_data['TRIP_COUNT'].sum()
                service_avg_revenue = service_data['AVG_REVENUE'].mean()
                service_max_intensity = service_data['DEMAND_INTENSITY'].max()
                
                st.markdown(f"""
                <div class="heatmap-card">
                    <h4>📊 {service} Stats</h4>
                    <p><strong>Peak Hour:</strong> {service_peak_hour:02d}:00</p>
                    <p><strong>Total Trips:</strong> {format_number(service_total_trips)}</p>
                    <p><strong>Avg Revenue:</strong> {format_number(service_avg_revenue, 'currency')}</p>
                    <p><strong>Max Intensity:</strong> {service_max_intensity:.1f}%</p>
                </div>
                """, unsafe_allow_html=True)
                
                # Peak hours for this service
                service_hourly = service_data.groupby('HOUR')['DEMAND_INTENSITY'].mean().sort_values(ascending=False)
                
                st.markdown("**🔥 Top Peak Hours:**")
                for hour in service_hourly.head(3).index:
                    intensity = service_hourly[hour]
                    peak_class, peak_label = get_peak_indicator(intensity)
                    st.markdown(f'<span class="peak-indicator {peak_class}">{hour:02d}:00 - {intensity:.1f}%</span>', 
                               unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Pattern analysis charts
    st.markdown('<div class="demand-header">📈 Demand Pattern Analysis</div>', unsafe_allow_html=True)
    
    pattern_tab1, pattern_tab2, pattern_tab3 = st.tabs(["Hourly Patterns", "Daily Patterns", "Revenue Analysis"])
    
    with pattern_tab1:
        st.plotly_chart(create_hourly_pattern_chart(df), use_container_width=True)
        
        # Hourly insights
        st.markdown("**💡 Hourly Insights:**")
        hourly_summary = df.groupby('HOUR')['TRIP_COUNT'].sum().sort_values(ascending=False)
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**🌅 Morning Rush (6-10 AM):**")
            morning_trips = hourly_summary[6:11].sum()
            st.info(f"Total trips: {format_number(morning_trips)}")
        
        with col2:
            st.markdown("**🌆 Evening Rush (5-9 PM):**")
            evening_trips = hourly_summary[17:22].sum()
            st.info(f"Total trips: {format_number(evening_trips)}")
    
    with pattern_tab2:
        st.plotly_chart(create_daily_pattern_chart(df), use_container_width=True)
        
        # Daily insights
        st.markdown("**💡 Daily Insights:**")
        daily_summary = df.groupby(['DAY_NAME', 'DAY_OF_WEEK'])['TRIP_COUNT'].sum().sort_values(ascending=False)
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**📅 Weekdays vs Weekends:**")
            weekday_trips = df[df['DAY_OF_WEEK'] <= 5]['TRIP_COUNT'].sum()
            weekend_trips = df[df['DAY_OF_WEEK'] > 5]['TRIP_COUNT'].sum()
            st.info(f"Weekdays: {format_number(weekday_trips)} | Weekends: {format_number(weekend_trips)}")
        
        with col2:
            top_day = daily_summary.index[0][0]
            top_day_trips = daily_summary.iloc[0]
            st.markdown("**🏆 Peak Day:**")
            st.success(f"{top_day}: {format_number(top_day_trips)} trips")
    
    with pattern_tab3:
        st.plotly_chart(create_revenue_intensity_scatter(df), use_container_width=True)
        
        # Revenue insights
        st.markdown("**💰 Revenue Insights:**")
        high_revenue_hours = df[df['AVG_REVENUE'] > df['AVG_REVENUE'].quantile(0.75)]
        high_intensity_hours = df[df['DEMAND_INTENSITY'] > 70]
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**💵 High Revenue Hours:**")
            avg_high_revenue = high_revenue_hours['AVG_REVENUE'].mean()
            st.info(f"Average: {format_number(avg_high_revenue, 'currency')}")
        
        with col2:
            st.markdown("**🎯 High Demand Hours:**")
            high_demand_revenue = high_intensity_hours['AVG_REVENUE'].mean()
            st.info(f"Revenue: {format_number(high_demand_revenue, 'currency')}")
    
    # Detailed time slot analysis
    st.markdown("---")
    st.markdown('<div class="demand-header">⏰ Time Slot Deep Dive</div>', unsafe_allow_html=True)
    
    # Group by time periods
    def get_time_period(hour):
        if 5 <= hour < 12:
            return "🌅 Morning (5-12)"
        elif 12 <= hour < 17:
            return "☀️ Afternoon (12-17)"
        elif 17 <= hour < 22:
            return "🌆 Evening (17-22)"
        else:
            return "🌙 Night (22-5)"
    
    df['TIME_PERIOD'] = df['HOUR'].apply(get_time_period)
    
    period_analysis = df.groupby('TIME_PERIOD').agg({
        'TRIP_COUNT': 'sum',
        'AVG_REVENUE': 'mean',
        'DEMAND_INTENSITY': 'mean'
    }).reset_index()
    
    for _, period in period_analysis.iterrows():
        peak_class, _ = get_peak_indicator(period['DEMAND_INTENSITY'])
        
        st.markdown(f"""
        <div class="time-slot">
            <strong>{period['TIME_PERIOD']}</strong>
            <div style="margin-top: 0.5rem;">
                <span style="margin-right: 2rem;">🚗 Trips: {format_number(period['TRIP_COUNT'])}</span>
                <span style="margin-right: 2rem;">💰 Avg Revenue: {format_number(period['AVG_REVENUE'], 'currency')}</span>
                <span class="peak-indicator {peak_class}">Intensity: {period['DEMAND_INTENSITY']:.1f}%</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # Export and refresh section
    st.markdown("---")
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col2:
        csv = df.to_csv(index=False)
        st.download_button(
            label="📊 Download Heatmap Data",
            data=csv,
            file_name=f"demand_heatmap_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True
        )
    
    with col3:
        st.button("🔄 Refresh Data", 
                 help="Clear cache and reload data from Snowflake",
                 use_container_width=True,
                 on_click=lambda: st.cache_data.clear())