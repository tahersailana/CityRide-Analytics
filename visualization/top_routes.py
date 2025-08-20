import streamlit as st
import snowflake.connector
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.figure_factory as ff

from snowflake_connector import load_data_from_source

# Custom CSS for top routes styling
def load_routes_css():
    st.markdown("""
    <style>
        .routes-card {
            background: linear-gradient(135deg, #ff9a9e 0%, #fecfef 50%, #fecfef 100%);
            color: #2c3e50;
            padding: 1.5rem;
            border-radius: 0.75rem;
            margin-bottom: 1rem;
            box-shadow: 0 4px 15px rgba(0,0,0,0.2);
        }
        .routes-header {
            background: linear-gradient(90deg, #ff9a9e, #fecfef);
            color: #2c3e50;
            padding: 1rem;
            border-radius: 0.5rem;
            margin: 1rem 0;
            text-align: center;
            font-size: 1.2em;
            font-weight: bold;
        }
        .route-item {
            background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%);
            color: #2c3e50;
            padding: 1rem;
            border-radius: 0.5rem;
            margin: 0.5rem 0;
            border-left: 4px solid #ff9a9e;
            transition: all 0.3s ease;
        }
        .route-item:hover {
            transform: translateX(5px);
            box-shadow: 0 4px 15px rgba(255,154,158,0.3);
        }
        .route-rank {
            display: inline-block;
            padding: 0.3rem 0.8rem;
            border-radius: 1rem;
            font-weight: bold;
            margin: 0.25rem;
            font-size: 0.85em;
            min-width: 2rem;
            text-align: center;
        }
        .rank-1 {
            background: linear-gradient(135deg, #ffd700, #ffed4e);
            color: #2c3e50;
            box-shadow: 0 2px 10px rgba(255,215,0,0.4);
        }
        .rank-2 {
            background: linear-gradient(135deg, #c0c0c0, #e8e8e8);
            color: #2c3e50;
            box-shadow: 0 2px 10px rgba(192,192,192,0.4);
        }
        .rank-3 {
            background: linear-gradient(135deg, #cd7f32, #daa520);
            color: white;
            box-shadow: 0 2px 10px rgba(205,127,50,0.4);
        }
        .rank-other {
            background: linear-gradient(135deg, #667eea, #764ba2);
            color: white;
            box-shadow: 0 2px 10px rgba(102,126,234,0.3);
        }
        .borough-tag {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 0.3rem 0.8rem;
            border-radius: 1rem;
            font-size: 0.8em;
            font-weight: bold;
            margin: 0.2rem;
            display: inline-block;
        }
        .service-badge {
            background: linear-gradient(135deg, #ff6b6b, #ee5a5a);
            color: white;
            padding: 0.4rem 1rem;
            border-radius: 1.5rem;
            font-weight: bold;
            margin: 0.25rem;
            display: inline-block;
            font-size: 0.9em;
        }
        .metric-box {
            background: rgba(255,255,255,0.9);
            padding: 1rem;
            border-radius: 0.5rem;
            text-align: center;
            border: 2px solid #ff9a9e;
            margin: 0.5rem 0;
        }
        .route-stats {
            background: linear-gradient(135deg, #ffeaa7 0%, #fab1a0 100%);
            padding: 1rem;
            border-radius: 0.5rem;
            margin: 0.5rem 0;
            border-left: 4px solid #ff9a9e;
        }
        .top-route-highlight {
            background: linear-gradient(135deg, #fd79a8 0%, #fdcb6e 100%);
            padding: 1.5rem;
            border-radius: 0.75rem;
            margin: 1rem 0;
            box-shadow: 0 6px 20px rgba(253,121,168,0.3);
            border: 2px solid #ff9a9e;
        }
        .borough-flow {
            background: rgba(255,255,255,0.95);
            padding: 1rem;
            border-radius: 0.5rem;
            margin: 0.5rem 0;
            border-left: 4px solid #667eea;
        }
    </style>
    """, unsafe_allow_html=True)

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_routes_data():
    """Load top routes data from Snowflake"""
    try:        
        query = "SELECT * FROM vw_top_routes ORDER BY trip_count DESC"
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
        return f"${num:.2f}"
    elif format_type == "percentage":
        return f"{num:.1f}%"
    elif format_type == "number":
        if num >= 1_000:
            return f"{num/1_000:.1f}K"
        else:
            return f"{num:,.0f}"
    elif format_type == "distance":
        return f"{num:.1f} mi"
    elif format_type == "duration":
        return f"{num:.0f} min"
    else:
        return str(num)

def get_rank_class(rank):
    """Get CSS class for route ranking"""
    if rank == 1:
        return "rank-1", "🥇"
    elif rank == 2:
        return "rank-2", "🥈"
    elif rank == 3:
        return "rank-3", "🥉"
    else:
        return "rank-other", f"#{rank}"

def create_top_routes_chart(df, metric='TRIP_COUNT', top_n=15):
    """Create bar chart for top routes"""
    top_routes = df.nlargest(top_n, metric)
    
    # Create route labels
    top_routes['route_label'] = (
        top_routes['PICKUP_ZONE'] + " → " + top_routes['DROPOFF_ZONE']
    )
    
    metric_labels = {
        'TRIP_COUNT': 'Trip Count',
        'TOTAL_REVENUE': 'Total Revenue ($)',
        'AVG_REVENUE': 'Average Revenue ($)',
        'AVG_DISTANCE': 'Average Distance (mi)',
        'AVG_DURATION': 'Average Duration (min)'
    }
    
    fig = px.bar(
        top_routes,
        x=metric,
        y='route_label',
        color='SERVICE_NAME',
        orientation='h',
        title=f"Top {top_n} Routes by {metric_labels.get(metric, metric)}",
        labels={'route_label': 'Route', metric: metric_labels.get(metric, metric)},
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    
    fig.update_layout(
        height=600,
        yaxis={'categoryorder': 'total ascending'},
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='black'),
        xaxis_title=metric_labels.get(metric, metric),
        yaxis_title=""
    )
    
    return fig

def create_borough_flow_chart(df):
    """Create Sankey diagram for borough flows"""
    # Aggregate by boroughs
    borough_flows = df.groupby(['PICKUP_BOROUGH', 'DROPOFF_BOROUGH']).agg({
        'TRIP_COUNT': 'sum',
        'TOTAL_REVENUE': 'sum'
    }).reset_index()
    
    # Get unique boroughs
    all_boroughs = list(set(borough_flows['PICKUP_BOROUGH'].unique()) | 
                       set(borough_flows['DROPOFF_BOROUGH'].unique()))
    
    # Create node indices
    borough_to_index = {borough: i for i, borough in enumerate(all_boroughs)}
    
    # Prepare data for Sankey
    sources = [borough_to_index[borough] for borough in borough_flows['PICKUP_BOROUGH']]
    targets = [borough_to_index[borough] for borough in borough_flows['DROPOFF_BOROUGH']]
    values = borough_flows['TRIP_COUNT'].tolist()
    
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=all_boroughs,
            color="rgba(255,154,158,0.8)"
        ),
        link=dict(
            source=sources,
            target=targets,
            value=values,
            color="rgba(255,154,158,0.4)"
        )
    )])
    
    fig.update_layout(
        title_text="Borough Flow Analysis",
        font_size=12,
        height=400,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig

def create_service_comparison_chart(df):
    """Create comparison chart by service"""
    service_stats = df.groupby('SERVICE_NAME').agg({
        'TRIP_COUNT': 'sum',
        'TOTAL_REVENUE': 'sum',
        'AVG_REVENUE': 'mean',
        'AVG_DISTANCE': 'mean',
        'AVG_DURATION': 'mean'
    }).reset_index()
    
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=['Trip Volume', 'Total Revenue', 'Avg Distance', 'Avg Duration'],
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": False}, {"secondary_y": False}]]
    )
    
    colors = px.colors.qualitative.Set2
    
    # Trip volume
    fig.add_trace(
        go.Bar(x=service_stats['SERVICE_NAME'], y=service_stats['TRIP_COUNT'],
               name='Trip Count', marker_color=colors[0]),
        row=1, col=1
    )
    
    # Total revenue
    fig.add_trace(
        go.Bar(x=service_stats['SERVICE_NAME'], y=service_stats['TOTAL_REVENUE'],
               name='Total Revenue', marker_color=colors[1]),
        row=1, col=2
    )
    
    # Average distance
    fig.add_trace(
        go.Bar(x=service_stats['SERVICE_NAME'], y=service_stats['AVG_DISTANCE'],
               name='Avg Distance', marker_color=colors[2]),
        row=2, col=1
    )
    
    # Average duration
    fig.add_trace(
        go.Bar(x=service_stats['SERVICE_NAME'], y=service_stats['AVG_DURATION'],
               name='Avg Duration', marker_color=colors[3]),
        row=2, col=2
    )
    
    fig.update_layout(
        height=500,
        showlegend=False,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='black')
    )
    
    return fig

def create_route_efficiency_scatter(df):
    """Create scatter plot for route efficiency analysis"""
    fig = px.scatter(
        df,
        x='AVG_DISTANCE',
        y='AVG_REVENUE',
        color='SERVICE_NAME',
        size='TRIP_COUNT',
        hover_data=['PICKUP_ZONE', 'DROPOFF_ZONE', 'AVG_DURATION'],
        title="Route Efficiency: Distance vs Revenue",
        labels={'AVG_DISTANCE': 'Average Distance (mi)', 'AVG_REVENUE': 'Average Revenue ($)'}
    )
    
    fig.update_layout(
        height=500,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='black')
    )
    
    return fig

def analyze_route_patterns(df):
    """Analyze route patterns and return insights"""
    insights = {}
    
    # Top route overall
    top_route = df.iloc[0]
    insights['top_route'] = {
        'pickup': top_route['PICKUP_ZONE'],
        'dropoff': top_route['DROPOFF_ZONE'],
        'trips': top_route['TRIP_COUNT'],
        'revenue': top_route['TOTAL_REVENUE'],
        'service': top_route['SERVICE_NAME']
    }
    
    # Most profitable route
    most_profitable = df.loc[df['TOTAL_REVENUE'].idxmax()]
    insights['most_profitable'] = {
        'pickup': most_profitable['PICKUP_ZONE'],
        'dropoff': most_profitable['DROPOFF_ZONE'],
        'revenue': most_profitable['TOTAL_REVENUE'],
        'trips': most_profitable['TRIP_COUNT']
    }
    
    # Borough analysis
    borough_stats = df.groupby('PICKUP_BOROUGH')['TRIP_COUNT'].sum().sort_values(ascending=False)
    insights['top_pickup_borough'] = {
        'borough': borough_stats.index[0],
        'trips': borough_stats.iloc[0]
    }
    
    # Service dominance
    service_routes = df.groupby('SERVICE_NAME').size().sort_values(ascending=False)
    insights['dominant_service'] = {
        'service': service_routes.index[0],
        'routes': service_routes.iloc[0]
    }
    
    return insights

def show_top_routes():
    """Main function to display the top routes dashboard"""
    
    # Load CSS
    load_routes_css()
    
    # Header
    st.markdown('<div class="routes-header">🗺️ Top Routes Dashboard</div>', unsafe_allow_html=True)
    
    # Load data
    with st.spinner("Loading top routes data from Snowflake..."):
        df = load_routes_data()
    
    if df is None or df.empty:
        st.error("No routes data available. Please check your database connection.")
        return
    
    # Auto-refresh indicator
    st.caption("Routes data auto-refreshes every 5 minutes")
    st.markdown("---")
    
    # Quick insights section
    insights = analyze_route_patterns(df)
    
    st.markdown('<div class="routes-header">🏆 Route Champions</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(f"""
        <div class="top-route-highlight">
            <h4>🥇 Busiest Route</h4>
            <p><strong>From:</strong> {insights['top_route']['pickup']}</p>
            <p><strong>To:</strong> {insights['top_route']['dropoff']}</p>
            <p><strong>Service:</strong> <span class="service-badge">{insights['top_route']['service']}</span></p>
            <p><strong>Trips:</strong> {format_number(insights['top_route']['trips'])}</p>
            <p><strong>Revenue:</strong> {format_number(insights['top_route']['revenue'], 'currency')}</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="top-route-highlight">
            <h4>💰 Most Profitable Route</h4>
            <p><strong>From:</strong> {insights['most_profitable']['pickup']}</p>
            <p><strong>To:</strong> {insights['most_profitable']['dropoff']}</p>
            <p><strong>Revenue:</strong> {format_number(insights['most_profitable']['revenue'], 'currency')}</p>
            <p><strong>Trips:</strong> {format_number(insights['most_profitable']['trips'])}</p>
            <p><strong>Avg per Trip:</strong> {format_number(insights['most_profitable']['revenue']/insights['most_profitable']['trips'], 'currency')}</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Key metrics
    metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
    
    with metric_col1:
        total_routes = len(df)
        st.metric("Total Top Routes", format_number(total_routes), "Active")
    
    with metric_col2:
        total_trips = df['TRIP_COUNT'].sum()
        st.metric("Total Trips", format_number(total_trips), "All Services")
    
    with metric_col3:
        total_revenue = df['TOTAL_REVENUE'].sum()
        st.metric("Total Revenue", format_number(total_revenue, 'currency'), "Combined")
    
    with metric_col4:
        avg_route_revenue = df['TOTAL_REVENUE'].mean()
        st.metric("Avg Route Revenue", format_number(avg_route_revenue, 'currency'), "Per Route")
    
    st.markdown("---")
    
    # Route analysis tabs
    st.markdown('<div class="routes-header">📊 Route Analysis</div>', unsafe_allow_html=True)
    
    analysis_tab1, analysis_tab2, analysis_tab3, analysis_tab4 = st.tabs([
        "🔥 Top Routes", "🌉 Borough Flow", "🚗 Service Analysis", "⚡ Efficiency"
    ])
    
    with analysis_tab1:
        # Route ranking controls
        col1, col2 = st.columns([1, 3])
        
        with col1:
            metric_choice = st.selectbox(
                "Rank by:",
                ['TRIP_COUNT', 'TOTAL_REVENUE', 'AVG_REVENUE', 'AVG_DISTANCE', 'AVG_DURATION'],
                format_func=lambda x: {
                    'TRIP_COUNT': '🚗 Trip Volume',
                    'TOTAL_REVENUE': '💰 Total Revenue',
                    'AVG_REVENUE': '💵 Avg Revenue',
                    'AVG_DISTANCE': '📏 Avg Distance',
                    'AVG_DURATION': '⏱️ Avg Duration'
                }.get(x, x)
            )
            
            top_n = st.slider("Show top N routes:", 10, 50, 15)
        
        with col2:
            st.plotly_chart(create_top_routes_chart(df, metric_choice, top_n), use_container_width=True)
        
        # Detailed route breakdown
        st.markdown("**Route Details:**")
        
        top_routes_display = df.nlargest(10, metric_choice)
        
        for idx, route in top_routes_display.iterrows():
            rank_by_volume = route['ROUTE_RANK_BY_VOLUME']
            rank_by_revenue = route['ROUTE_RANK_BY_REVENUE']
            
            volume_class, volume_emoji = get_rank_class(rank_by_volume)
            revenue_class, revenue_emoji = get_rank_class(rank_by_revenue)
            
            st.markdown(f"""
            <div class="route-item">
                <div style="display: flex; justify-content: between; align-items: center; margin-bottom: 0.5rem;">
                    <strong>{route['PICKUP_ZONE']} → {route['DROPOFF_ZONE']}</strong>
                    <div>
                        <span class="route-rank {volume_class}">Vol: {volume_emoji}</span>
                        <span class="route-rank {revenue_class}">Rev: {revenue_emoji}</span>
                    </div>
                </div>
                <div style="display: flex; flex-wrap: wrap; gap: 1rem; align-items: center;">
                    <span class="borough-tag">📍 {route['PICKUP_BOROUGH']}</span>
                    <span class="borough-tag">📍 {route['DROPOFF_BOROUGH']}</span>
                    <span class="service-badge">{route['SERVICE_NAME']}</span>
                </div>
                <div style="margin-top: 0.5rem; display: flex; flex-wrap: wrap; gap: 1.5rem;">
                    <span>🚗 <strong>{format_number(route['TRIP_COUNT'])} trips</strong></span>
                    <span>💰 <strong>{format_number(route['TOTAL_REVENUE'], 'currency')}</strong></span>
                    <span>📏 <strong>{format_number(route['AVG_DISTANCE'], 'distance')}</strong></span>
                    <span>⏱️ <strong>{format_number(route['AVG_DURATION'], 'duration')}</strong></span>
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    with analysis_tab2:
        st.plotly_chart(create_borough_flow_chart(df), use_container_width=True)
        
        # Borough statistics
        st.markdown("**🏙️ Borough Performance:**")
        
        borough_pickup = df.groupby('PICKUP_BOROUGH').agg({
            'TRIP_COUNT': 'sum',
            'TOTAL_REVENUE': 'sum'
        }).sort_values('TRIP_COUNT', ascending=False)
        
        for borough, stats in borough_pickup.iterrows():
            st.markdown(f"""
            <div class="borough-flow">
                <strong>📍 {borough}</strong>
                <div style="margin-top: 0.5rem;">
                    <span style="margin-right: 2rem;">🚗 Pickups: {format_number(stats['TRIP_COUNT'])}</span>
                    <span>💰 Revenue: {format_number(stats['TOTAL_REVENUE'], 'currency')}</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    with analysis_tab3:
        st.plotly_chart(create_service_comparison_chart(df), use_container_width=True)
        
        # Service insights
        st.markdown("**🚗 Service Insights:**")
        
        service_analysis = df.groupby('SERVICE_NAME').agg({
            'TRIP_COUNT': 'sum',
            'TOTAL_REVENUE': 'sum',
            'AVG_REVENUE': 'mean',
            'AVG_DISTANCE': 'mean',
            'AVG_DURATION': 'mean'
        }).round(2)
        
        for service, stats in service_analysis.iterrows():
            st.markdown(f"""
            <div class="route-stats">
                <h5>{service}</h5>
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; margin-top: 0.5rem;">
                    <div><strong>Trips:</strong> {format_number(stats['TRIP_COUNT'])}</div>
                    <div><strong>Revenue:</strong> {format_number(stats['TOTAL_REVENUE'], 'currency')}</div>
                    <div><strong>Avg Revenue:</strong> {format_number(stats['AVG_REVENUE'], 'currency')}</div>
                    <div><strong>Avg Distance:</strong> {format_number(stats['AVG_DISTANCE'], 'distance')}</div>
                    <div><strong>Avg Duration:</strong> {format_number(stats['AVG_DURATION'], 'duration')}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    with analysis_tab4:
        st.plotly_chart(create_route_efficiency_scatter(df), use_container_width=True)
        
        # Efficiency insights
        st.markdown("**⚡ Efficiency Insights:**")
        
        # Calculate revenue per mile and per minute
        df_efficiency = df.copy()
        df_efficiency['revenue_per_mile'] = df_efficiency['AVG_REVENUE'] / df_efficiency['AVG_DISTANCE']
        df_efficiency['revenue_per_minute'] = df_efficiency['AVG_REVENUE'] / df_efficiency['AVG_DURATION']
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**💰 Most Efficient Routes ($/mile):**")
            top_efficiency_mile = df_efficiency.nlargest(5, 'revenue_per_mile')
            for _, route in top_efficiency_mile.iterrows():
                st.markdown(f"""
                <div class="metric-box">
                    <strong>{route['PICKUP_ZONE'][:20]}... → {route['DROPOFF_ZONE'][:20]}...</strong><br>
                    <span style="color: #ff6b6b; font-weight: bold;">${route['revenue_per_mile']:.2f}/mile</span>
                </div>
                """, unsafe_allow_html=True)
        
        with col2:
            st.markdown("**⏱️ Most Efficient Routes ($/min):**")
            top_efficiency_time = df_efficiency.nlargest(5, 'revenue_per_minute')
            for _, route in top_efficiency_time.iterrows():
                st.markdown(f"""
                <div class="metric-box">
                    <strong>{route['PICKUP_ZONE'][:20]}... → {route['DROPOFF_ZONE'][:20]}...</strong><br>
                    <span style="color: #667eea; font-weight: bold;">${route['revenue_per_minute']:.2f}/min</span>
                </div>
                """, unsafe_allow_html=True)
    
    # Export and refresh section
    st.markdown("---")
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col2:
        csv = df.to_csv(index=False)
        st.download_button(
            label="📊 Download Routes Data",
            data=csv,
            file_name=f"top_routes_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True
        )
    
    with col3:
        st.button("🔄 Refresh Data", 
                 help="Clear cache and reload data from Snowflake",
                 use_container_width=True,
                 on_click=lambda: st.cache_data.clear())