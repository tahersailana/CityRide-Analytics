import streamlit as st
from executive_dashboard import show_executive_dashboard
from service_comparison import show_service_comparison
from demand_heatmap import show_demand_heatmap
from top_routes import show_top_routes
from monthly_trends import show_monthly_trends
from airport_traffic import show_airport_traffic

# Page configuration
st.set_page_config(
    page_title="CityRide Analytics Platform",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for navigation
st.markdown("""
<style>
    .main-nav {
        background: linear-gradient(90deg, #1f77b4, #17a2b8);
        color: white;
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 2rem;
        text-align: center;
    }
    .nav-title {
        font-size: 2em;
        font-weight: bold;
        margin-bottom: 0.5rem;
    }
    .nav-subtitle {
        font-size: 1.1em;
        opacity: 0.9;
    }
    .page-selector {
        background-color: #f8f9fa !important;
        color: #2c3e50 !important;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
        margin-bottom: 1rem;
    }
    .page-selector strong {
        color: #1f77b4 !important;
    }
    .nav-button {
        margin-bottom: 0.5rem;
    }
    .sidebar-section {
        margin: 1rem 0;
        padding: 1rem;
        background-color: rgba(255,255,255,0.1);
        border-radius: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)

def main():
    # Main navigation header
    st.markdown("""
    <div class="main-nav">
        <div class="nav-title">🚗 CityRide Analytics Platform</div>
        <div class="nav-subtitle">Comprehensive Business Intelligence Dashboard</div>
    </div>
    """, unsafe_allow_html=True)
    
    # Initialize session state for page navigation
    if 'current_page' not in st.session_state:
        st.session_state.current_page = 'executive'
    
    # Sidebar navigation
    with st.sidebar:
        st.markdown("## 📊 Navigation")
        
        # Page selection buttons
        if st.button("🎯 Executive KPIs", use_container_width=True, 
                    type="primary" if st.session_state.current_page == 'executive' else "secondary",
                    key="nav_executive"):
            st.session_state.current_page = 'executive'
            st.rerun()
        
        if st.button("🔄 Service Comparison", use_container_width=True,
                    type="primary" if st.session_state.current_page == 'service' else "secondary",
                    key="nav_service"):
            st.session_state.current_page = 'service'
            st.rerun()
        
        if st.button("📈 Monthly Trends", use_container_width=True,
                    type="primary" if st.session_state.current_page == 'trends' else "secondary",
                    key="nav_trends"):
            st.session_state.current_page = 'trends'
            st.rerun()
        
        if st.button("🔥 Demand Heatmap", use_container_width=True,
                    type="primary" if st.session_state.current_page == 'heatmap' else "secondary",
                    key="nav_heatmap"):
            st.session_state.current_page = 'heatmap'
            st.rerun()
        
        if st.button("🗺️ Top Routes", use_container_width=True,
                    type="primary" if st.session_state.current_page == 'routes' else "secondary",
                    key="nav_routes"):
            st.session_state.current_page = 'routes'
            st.rerun()

        if st.button("✈️ Airport Traffic", use_container_width=True,
                    type="primary" if st.session_state.current_page == 'airport' else "secondary",
                    key="nav_airport"):
            st.session_state.current_page = 'airport'
            st.rerun()
        
        st.markdown("---")
        
        # Page descriptions
        if st.session_state.current_page == 'executive':
            st.markdown("""
            <div class="sidebar-section">
            <strong>📊 Executive KPIs</strong>
            <ul>
            <li>Overall business performance</li>
            <li>Trip and revenue metrics</li>
            <li>Year-over-year comparisons</li>
            <li>Growth analytics</li>
            </ul>
            </div>
            """, unsafe_allow_html=True)
        elif st.session_state.current_page == 'service':
            st.markdown("""
            <div class="sidebar-section">
            <strong>🔄 Service Comparison</strong>
            <ul>
            <li>Service type analysis</li>
            <li>Performance by category</li>
            <li>Payment methods</li>
            <li>Operational insights</li>
            </ul>
            </div>
            """, unsafe_allow_html=True)
        elif st.session_state.current_page == 'trends':
            st.markdown("""
            <div class="sidebar-section">
            <strong>📈 Monthly Trends</strong>
            <ul>
            <li>Month-over-month growth</li>
            <li>Time series analysis</li>
            <li>Service trend comparison</li>
            <li>Performance patterns</li>
            </ul>
            </div>
            """, unsafe_allow_html=True)
        elif st.session_state.current_page == 'heatmap':
            st.markdown("""
            <div class="sidebar-section">
            <strong>🔥 Demand Heatmap</strong>
            <ul>
            <li>Hourly demand patterns</li>
            <li>Peak time analysis</li>
            <li>Service-specific heatmaps</li>
            <li>Time slot insights</li>
            </ul>
            </div>
            """, unsafe_allow_html=True)
        elif st.session_state.current_page == 'airport':
            st.markdown("""
            <div class="sidebar-section">
            <strong>✈️ Airport Traffic</strong>
            <ul>
            <li>Airport passenger flows</li>
            <li>Flight and ground transport integration</li>
            <li>Peak travel times</li>
            <li>Traffic forecasts</li>
            </ul>
            </div>
            """, unsafe_allow_html=True)
        else:  # routes
            st.markdown("""
            <div class="sidebar-section">
            <strong>🗺️ Top Routes</strong>
            <ul>
            <li>Most popular routes</li>
            <li>Borough flow analysis</li>
            <li>Route efficiency metrics</li>
            <li>Service-specific routes</li>
            </ul>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Quick navigation info
        st.markdown("**🚀 Quick Stats:**")
        st.info("Real-time data from Snowflake")
        st.success("Auto-refresh: 5 minutes")
        
        # Dashboard info
        st.markdown("---")
        st.markdown("**💡 Dashboard Info:**")
        page_count = 6  # Updated count
        st.metric("Total Views", page_count, "Analytics Pages")
        
        current_page_names = {
            'executive': 'Executive KPIs',
            'service': 'Service Comparison',
            'trends': 'Monthly Trends',
            'heatmap': 'Demand Heatmap',
            'routes': 'Top Routes',
            'airport': 'Airport Traffic'
        }
        current_name = current_page_names.get(st.session_state.current_page, 'Unknown')
        st.caption(f"Currently viewing: **{current_name}**")
    
    # Main content area with page selection indicator
    if st.session_state.current_page == 'executive':
        st.markdown("""
        <div class="page-selector">
            <strong>📊 Currently Viewing:</strong> Executive KPIs Dashboard
        </div>
        """, unsafe_allow_html=True)
        show_executive_dashboard()
    
    elif st.session_state.current_page == 'service':
        st.markdown("""
        <div class="page-selector">
            <strong>🔄 Currently Viewing:</strong> Service Comparison Dashboard
        </div>
        """, unsafe_allow_html=True)
        show_service_comparison()
    
    elif st.session_state.current_page == 'trends':
        st.markdown("""
        <div class="page-selector">
            <strong>📈 Currently Viewing:</strong> Monthly Trends Dashboard
        </div>
        """, unsafe_allow_html=True)
        show_monthly_trends()
    
    elif st.session_state.current_page == 'heatmap':
        st.markdown("""
        <div class="page-selector">
            <strong>🔥 Currently Viewing:</strong> Demand Heatmap Dashboard
        </div>
        """, unsafe_allow_html=True)
        show_demand_heatmap()
    
    elif st.session_state.current_page == 'routes':
        st.markdown("""
        <div class="page-selector">
            <strong>🗺️ Currently Viewing:</strong> Top Routes Dashboard
        </div>
        """, unsafe_allow_html=True)
        show_top_routes()

    elif st.session_state.current_page == 'airport':
        st.markdown("""
        <div class="page-selector">
            <strong>✈️ Currently Viewing:</strong> Airport Traffic Dashboard
        </div>
        """, unsafe_allow_html=True)
        show_airport_traffic()
    
    # Footer navigation (alternative to sidebar)
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col1:
        # Previous page navigation
        if st.session_state.current_page == 'service':
            if st.button("⬅️ Executive KPIs", use_container_width=True, key="footer_prev_service"):
                st.session_state.current_page = 'executive'
                st.rerun()
        elif st.session_state.current_page == 'trends':
            if st.button("⬅️ Service Comparison", use_container_width=True, key="footer_prev_trends"):
                st.session_state.current_page = 'service'
                st.rerun()
        elif st.session_state.current_page == 'heatmap':
            if st.button("⬅️ Monthly Trends", use_container_width=True, key="footer_prev_heatmap"):
                st.session_state.current_page = 'trends'
                st.rerun()
        elif st.session_state.current_page == 'routes':
            if st.button("⬅️ Demand Heatmap", use_container_width=True, key="footer_prev_routes"):
                st.session_state.current_page = 'heatmap'
                st.rerun()
        elif st.session_state.current_page == 'airport':
            if st.button("⬅️ Top Routes", use_container_width=True, key="footer_prev_airport"):
                st.session_state.current_page = 'routes'
                st.rerun()
    
    with col2:
        # Current page indicator with navigation breadcrumb
        page_names = {
            'executive': '📊 Executive KPIs',
            'service': '🔄 Service Comparison',
            'trends': '📈 Monthly Trends',
            'heatmap': '🔥 Demand Heatmap',
            'routes': '🗺️ Top Routes',
            'airport': '✈️ Airport Traffic'
        }
        current_name = page_names.get(st.session_state.current_page, st.session_state.current_page.title())
        
        # Show page position
        page_order = ['executive', 'service', 'trends', 'heatmap', 'routes', 'airport']
        current_position = page_order.index(st.session_state.current_page) + 1
        total_pages = len(page_order)
        
        st.markdown(f"""
        <div style='text-align: center; padding: 1rem;'>
            <strong>Current:</strong> {current_name}<br>
            <small>Page {current_position} of {total_pages}</small>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        # Next page navigation
        if st.session_state.current_page == 'executive':
            if st.button("Service Comparison ➡️", use_container_width=True, key="footer_next_executive"):
                st.session_state.current_page = 'service'
                st.rerun()
        elif st.session_state.current_page == 'service':
            if st.button("Monthly Trends ➡️", use_container_width=True, key="footer_next_service"):
                st.session_state.current_page = 'trends'
                st.rerun()
        elif st.session_state.current_page == 'trends':
            if st.button("Demand Heatmap ➡️", use_container_width=True, key="footer_next_trends"):
                st.session_state.current_page = 'heatmap'
                st.rerun()
        elif st.session_state.current_page == 'heatmap':
            if st.button("Top Routes ➡️", use_container_width=True, key="footer_next_heatmap"):
                st.session_state.current_page = 'routes'
                st.rerun()
        elif st.session_state.current_page == 'routes':
            if st.button("Airport Traffic ➡️", use_container_width=True, key="footer_next_routes"):
                st.session_state.current_page = 'airport'
                st.rerun()
    
    # Additional footer info
    st.markdown("---")
    footer_col1, footer_col2, footer_col3 = st.columns(3)
    
    with footer_col1:
        st.markdown("**🏢 CityRide Analytics**")
        st.caption("Business Intelligence Platform")
    
    with footer_col2:
        st.markdown("**📊 Data Source**")
        st.caption("Snowflake Data Warehouse")
    
    with footer_col3:
        st.markdown("**🔄 Last Update**")
        st.caption("Real-time (5min refresh)")

if __name__ == "__main__":
    main()