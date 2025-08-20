import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from snowflake_connector import load_data_from_source

# Custom CSS for service comparison styling
def load_service_css():
    st.markdown("""
    <style>
        .service-card {
            background-color: #f8f9fa;
            padding: 1.5rem;
            border-radius: 0.5rem;
            border-left: 4px solid #28a745;
            margin-bottom: 1rem;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .premium-service {
            border-left-color: #ffc107;
            background: linear-gradient(135deg, #fff9e6, #f8f9fa);
        }
        .standard-service {
            border-left-color: #17a2b8;
            background: linear-gradient(135deg, #e6f7ff, #f8f9fa);
        }
        .economy-service {
            border-left-color: #28a745;
            background: linear-gradient(135deg, #e6f7e6, #f8f9fa);
        }
        .service-metric {
            font-size: 1.5em;
            font-weight: bold;
            color: #2c3e50;
        }
        .service-name {
            font-size: 1.2em;
            font-weight: bold;
            color: #1f77b4;
            margin-bottom: 0.5rem;
        }
        .comparison-header {
            background: linear-gradient(90deg, #28a745, #20c997);
            color: white;
            padding: 1rem;
            border-radius: 0.5rem;
            margin: 1rem 0;
            text-align: center;
            font-size: 1.2em;
            font-weight: bold;
        }
        .payment-indicator {
            display: inline-block;
            padding: 0.25rem 0.5rem;
            border-radius: 1rem;
            font-size: 0.8em;
            font-weight: bold;
            margin: 0.25rem;
        }
        .cashless-high {
            background-color: #d1ecf1;
            color: #0c5460;
            border: 1px solid #bee5eb;
        }
        .cashless-medium {
            background-color: #fff3cd;
            color: #856404;
            border: 1px solid #ffeaa7;
        }
        .cashless-low {
            background-color: #f8d7da;
            color: #721c24;
            border: 1px solid #f5c6cb;
        }
    </style>
    """, unsafe_allow_html=True)

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_service_data():
    """Load service comparison data from Snowflake"""
    try:        
        query = "SELECT * FROM vw_service_comparison ORDER BY total_revenue DESC"
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
        return f"{num:.1f}%"
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

def get_service_class(category):
    """Determine service class for styling"""
    category_lower = str(category).lower()
    if any(word in category_lower for word in ['premium', 'luxury', 'executive']):
        return "premium-service"
    elif any(word in category_lower for word in ['standard', 'regular', 'normal']):
        return "standard-service"
    else:
        return "economy-service"

def get_cashless_indicator(percentage):
    """Get cashless payment indicator"""
    if percentage >= 80:
        return "cashless-high", "💳 High Digital"
    elif percentage >= 50:
        return "cashless-medium", "💰 Mixed Payment"
    else:
        return "cashless-low", "💵 Cash Heavy"

def create_service_revenue_chart(df):
    """Create service revenue comparison chart"""
    fig = px.bar(
        df.sort_values('TOTAL_REVENUE', ascending=True),
        x='TOTAL_REVENUE',
        y='SERVICE_NAME',
        orientation='h',
        title="Revenue by Service Type",
        labels={'TOTAL_REVENUE': 'Total Revenue ($)', 'SERVICE_NAME': 'Service'},
        color='SERVICE_CATEGORY',
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    
    fig.update_layout(
        height=400,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    return fig

def create_trip_metrics_chart(df):
    """Create trip metrics comparison chart"""
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Average Trip Distance', 'Average Trip Duration', 
                       'Average Passengers', 'Average Trip Value'),
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": False}, {"secondary_y": False}]]
    )
    
    services = df['SERVICE_NAME'].tolist()
    colors = px.colors.qualitative.Set3[:len(services)]
    
    # Average Trip Distance
    fig.add_trace(
        go.Bar(x=services, y=df['AVG_TRIP_DISTANCE'], name='Distance', 
               marker_color=colors, showlegend=False),
        row=1, col=1
    )
    
    # Average Trip Duration
    fig.add_trace(
        go.Bar(x=services, y=df['AVG_TRIP_DURATION'], name='Duration',
               marker_color=colors, showlegend=False),
        row=1, col=2
    )
    
    # Average Passengers
    fig.add_trace(
        go.Bar(x=services, y=df['AVG_PASSENGERS'], name='Passengers',
               marker_color=colors, showlegend=False),
        row=2, col=1
    )
    
    # Average Trip Value
    fig.add_trace(
        go.Bar(x=services, y=df['AVG_REVENUE_PER_TRIP'], name='Revenue',
               marker_color=colors, showlegend=False),
        row=2, col=2
    )
    
    fig.update_layout(height=600, title_text="Service Performance Metrics")
    fig.update_xaxes(tickangle=45)
    
    return fig

def create_payment_analysis_chart(df):
    """Create payment method analysis chart"""
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('Cashless Payment Percentage', 'Tip Percentage by Service'),
        specs=[[{"type": "bar"}, {"type": "bar"}]]
    )
    
    # Cashless percentage
    fig.add_trace(
        go.Bar(
            x=df['SERVICE_NAME'],
            y=df['CASHLESS_PERCENTAGE'],
            name='Cashless %',
            marker_color='lightblue',
            showlegend=False
        ),
        row=1, col=1
    )
    
    # Tip percentage
    fig.add_trace(
        go.Bar(
            x=df['SERVICE_NAME'],
            y=df['AVG_TIP_PERCENTAGE'],
            name='Tip %',
            marker_color='lightgreen',
            showlegend=False
        ),
        row=1, col=2
    )
    
    fig.update_layout(height=400, title_text="Payment & Tipping Analysis")
    fig.update_xaxes(tickangle=45)
    fig.update_yaxes(title_text="Percentage (%)")
    
    return fig

def create_market_share_chart(df):
    """Create market share pie chart"""
    fig = px.pie(
        df, 
        values='TOTAL_REVENUE', 
        names='SERVICE_NAME',
        title="Market Share by Revenue",
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    
    fig.update_traces(
        textposition='inside', 
        textinfo='percent+label',
        hovertemplate='<b>%{label}</b><br>Revenue: $%{value:,.0f}<br>Share: %{percent}<extra></extra>'
    )
    
    fig.update_layout(height=500)
    
    return fig

def show_service_comparison():
    """Main function to display the service comparison dashboard"""
    
    # Load CSS
    load_service_css()
    
    # Header
    st.markdown('<div class="comparison-header">🔄 Service Comparison Dashboard</div>', unsafe_allow_html=True)
    
    # Load data
    with st.spinner("Loading service comparison data from Snowflake..."):
        df = load_service_data()
    
    if df is None or df.empty:
        st.error("No service comparison data available. Please check your database connection.")
        return
    
    # Auto-refresh indicator
    st.caption("Service data auto-refreshes every 5 minutes")
    st.markdown("---")
    
    # Summary metrics
    st.markdown('<div class="comparison-header">📊 Service Overview</div>', unsafe_allow_html=True)
    
    # Calculate totals
    total_services = len(df)
    total_revenue_all = df['TOTAL_REVENUE'].sum()
    total_trips_all = df['TOTAL_TRIPS'].sum()
    avg_tip_all = df['AVG_TIP_PERCENTAGE'].mean()
    
    summary_col1, summary_col2, summary_col3, summary_col4 = st.columns(4)
    
    with summary_col1:
        st.metric("Total Services", total_services)
    
    with summary_col2:
        st.metric("Combined Revenue", format_number(total_revenue_all, "currency"))
    
    with summary_col3:
        st.metric("Combined Trips", format_number(total_trips_all, "number"))
    
    with summary_col4:
        st.metric("Average Tip Rate", f"{avg_tip_all:.1f}%")
    
    st.markdown("---")
    
    # Service performance cards
    st.markdown('<div class="comparison-header">🏆 Service Performance Breakdown</div>', unsafe_allow_html=True)
    
    # Sort by revenue for display
    df_sorted = df.sort_values('TOTAL_REVENUE', ascending=False)
    
    for index, row in df_sorted.iterrows():
        service_class = get_service_class(row['SERVICE_CATEGORY'])
        cashless_class, cashless_label = get_cashless_indicator(row['CASHLESS_PERCENTAGE'])
        
        # Calculate market share
        revenue_share = (row['TOTAL_REVENUE'] / total_revenue_all) * 100
        trip_share = (row['TOTAL_TRIPS'] / total_trips_all) * 100
        
        st.markdown(f"""
        <div class="service-card {service_class}">
            <div class="service-name">
                🚗 {row['SERVICE_NAME']} 
                <span style="font-size: 0.9em; color: #6c757d;">({row['SERVICE_CATEGORY']})</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Service metrics in columns
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Revenue", format_number(row['TOTAL_REVENUE'], "currency"))
            st.caption(f"📊 {revenue_share:.1f}% market share")
        
        with col2:
            st.metric("Total Trips", format_number(row['TOTAL_TRIPS'], "number"))
            st.caption(f"🎯 {trip_share:.1f}% of all trips")
        
        with col3:
            st.metric("Avg Trip Value", format_number(row['AVG_REVENUE_PER_TRIP'], "currency"))
            st.caption(f"📏 {row['AVG_TRIP_DISTANCE']:.1f} avg distance")
        
        with col4:
            st.metric("Tip Rate", f"{row['AVG_TIP_PERCENTAGE']:.1f}%")
            st.markdown(f'<span class="payment-indicator {cashless_class}">{cashless_label}</span>', 
                       unsafe_allow_html=True)
        
        # Additional metrics row
        detail_col1, detail_col2, detail_col3, detail_col4 = st.columns(4)
        
        with detail_col1:
            st.caption(f"⏱️ Avg Duration: {row['AVG_TRIP_DURATION']:.0f} min")
        
        with detail_col2:
            st.caption(f"👥 Avg Passengers: {row['AVG_PASSENGERS']:.1f}")
        
        with detail_col3:
            st.caption(f"📅 Operating Days: {row['OPERATING_DAYS']}")
        
        with detail_col4:
            st.caption(f"🏢 Active Vendors: {row['ACTIVE_VENDORS']}")
        
        st.markdown("---")
    
    # Visualization section
    st.markdown('<div class="comparison-header">📈 Service Analytics & Charts</div>', unsafe_allow_html=True)
    
    # Chart selection tabs
    chart_tab1, chart_tab2, chart_tab3, chart_tab4 = st.tabs(["Revenue Comparison", "Performance Metrics", "Payment Analysis", "Market Share"])
    
    with chart_tab1:
        st.plotly_chart(create_service_revenue_chart(df), use_container_width=True)
    
    with chart_tab2:
        st.plotly_chart(create_trip_metrics_chart(df), use_container_width=True)
    
    with chart_tab3:
        st.plotly_chart(create_payment_analysis_chart(df), use_container_width=True)
    
    with chart_tab4:
        st.plotly_chart(create_market_share_chart(df), use_container_width=True)
    
    # Detailed data table
    st.markdown('<div class="comparison-header">📋 Detailed Service Data</div>', unsafe_allow_html=True)
    
    # Format the dataframe for display
    display_df = df.copy()
    
    # Round numerical columns for better display
    numeric_columns = ['TOTAL_REVENUE', 'AVG_REVENUE_PER_TRIP', 'AVG_TRIP_DISTANCE', 
                      'AVG_TRIP_DURATION', 'AVG_PASSENGERS', 'AVG_TIP_PERCENTAGE', 'CASHLESS_PERCENTAGE']
    
    for col in numeric_columns:
        if col in display_df.columns:
            display_df[col] = display_df[col].round(2)
    
    # Display with search and sort functionality
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "TOTAL_REVENUE": st.column_config.NumberColumn(
                "Total Revenue",
                format="$%.0f"
            ),
            "AVG_REVENUE_PER_TRIP": st.column_config.NumberColumn(
                "Avg Trip Revenue",
                format="$%.2f"
            ),
            "CASHLESS_PERCENTAGE": st.column_config.NumberColumn(
                "Cashless %",
                format="%.1f%%"
            ),
            "AVG_TIP_PERCENTAGE": st.column_config.NumberColumn(
                "Avg Tip %",
                format="%.1f%%"
            )
        }
    )
    
    # Export functionality
    st.markdown("---")
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col2:
        csv = df.to_csv(index=False)
        st.download_button(
            label="📊 Download CSV",
            data=csv,
            file_name=f"service_comparison_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True
        )
    
    with col3:
        st.button("🔄 Refresh Data", 
                 help="Clear cache and reload data from Snowflake",
                 use_container_width=True,
                 on_click=lambda: st.cache_data.clear())