import streamlit as st
from executive_dashboard import show_executive_dashboard
from service_comparison import show_service_comparison
from demand_heatmap import show_demand_heatmap

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
        
        if st.button("🔥 Demand Heatmap", use_container_width=True,
                    type="primary" if st.session_state.current_page == 'heatmap' else "secondary",
                    key="nav_heatmap"):
            st.session_state.current_page = 'heatmap'
            st.rerun()
        
        st.markdown("---")
        
        # Page descriptions
        if st.session_state.current_page == 'executive':
            st.markdown("""
            **📊 Executive KPIs**
            - Overall business performance
            - Trip and revenue metrics
            - Year-over-year comparisons
            - Growth analytics
            """)
        elif st.session_state.current_page == 'service':
            st.markdown("""
            **🔄 Service Comparison**
            - Service type analysis
            - Performance by category
            - Payment methods
            - Operational insights
            """)
        else:  # heatmap
            st.markdown("""
            **🔥 Demand Heatmap**
            - Hourly demand patterns
            - Peak time analysis
            - Service-specific heatmaps
            - Time slot insights
            """)
        
        st.markdown("---")
        st.markdown("**Quick Stats:**")
        st.info("Real-time data from Snowflake")
        st.success("Auto-refresh: 5 minutes")
    
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
    
    elif st.session_state.current_page == 'heatmap':
        st.markdown("""
        <div class="page-selector">
            <strong>🔥 Currently Viewing:</strong> Demand Heatmap Dashboard
        </div>
        """, unsafe_allow_html=True)
        show_demand_heatmap()
    
    # Footer navigation (alternative to sidebar)
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col1:
        # Previous page navigation
        if st.session_state.current_page == 'service':
            if st.button("⬅️ Executive KPIs", use_container_width=True, key="footer_prev_service"):
                st.session_state.current_page = 'executive'
                st.rerun()
        elif st.session_state.current_page == 'heatmap':
            if st.button("⬅️ Service Comparison", use_container_width=True, key="footer_prev_heatmap"):
                st.session_state.current_page = 'service'
                st.rerun()
    
    with col2:
        # Current page indicator
        page_names = {
            'executive': 'Executive KPIs',
            'service': 'Service Comparison', 
            'heatmap': 'Demand Heatmap'
        }
        current_name = page_names.get(st.session_state.current_page, st.session_state.current_page.title())
        st.markdown(f"<div style='text-align: center; padding: 1rem;'><strong>Page:</strong> {current_name}</div>", 
                   unsafe_allow_html=True)
    
    with col3:
        # Next page navigation
        if st.session_state.current_page == 'executive':
            if st.button("Service Comparison ➡️", use_container_width=True, key="footer_next_executive"):
                st.session_state.current_page = 'service'
                st.rerun()
        elif st.session_state.current_page == 'service':
            if st.button("Demand Heatmap ➡️", use_container_width=True, key="footer_next_service"):
                st.session_state.current_page = 'heatmap'
                st.rerun()

if __name__ == "__main__":
    main()