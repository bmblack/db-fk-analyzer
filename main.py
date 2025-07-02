"""
Main Streamlit application for AI-powered database foreign key analysis and remediation.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import traceback

# Import our modules
from utils.database import get_database_manager, DatabaseManager
from utils.logging_config import setup_logging, streamlit_handler
from crew import create_database_crew, get_agent_description
from dotenv import load_dotenv

# Configure logging
setup_logging(level="INFO")
logger = logging.getLogger(__name__)

# Add Streamlit log handler
logger.addHandler(streamlit_handler)

# Page configuration
st.set_page_config(
    page_title="DB Foreign Key Analyzer",
    page_icon="🔗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .agent-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
        margin: 0.5rem 0;
    }
    .status-success {
        color: #28a745;
        font-weight: bold;
    }
    .status-error {
        color: #dc3545;
        font-weight: bold;
    }
    .status-running {
        color: #ffc107;
        font-weight: bold;
    }
    .metric-card {
        background-color: #ffffff;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)


class StreamlitApp:
    """Main Streamlit application class."""
    
    def __init__(self):
        """Initialize the application."""
        self.db_manager: Optional[DatabaseManager] = None
        self.crew = None
        self.initialize_session_state()
    
    def initialize_session_state(self):
        """Initialize Streamlit session state variables."""
        if 'db_connected' not in st.session_state:
            st.session_state.db_connected = False
        if 'crew_results' not in st.session_state:
            st.session_state.crew_results = {}
        if 'agent_status' not in st.session_state:
            st.session_state.agent_status = {}
        if 'analysis_running' not in st.session_state:
            st.session_state.analysis_running = False
    
    def connect_to_database(self) -> bool:
        """Attempt to connect to the database."""
        try:
            with st.spinner("Connecting to database..."):
                load_dotenv('config/settings.env')
                self.db_manager = get_database_manager()
                
                # Test connection
                if self.db_manager.test_connection():
                    self.crew = create_database_crew(self.db_manager)
                    st.session_state.db_connected = True
                    st.success("✅ Database connection successful!")
                    return True
                else:
                    st.error("❌ Database connection failed!")
                    return False
                    
        except Exception as e:
            st.error(f"❌ Database connection error: {str(e)}")
            logger.error(f"Database connection failed: {e}")
            return False
    
    def render_header(self):
        """Render the application header."""
        st.markdown('<h1 class="main-header">🔗 AI-Powered DB Foreign Key Analyzer</h1>', 
                   unsafe_allow_html=True)
        
        st.markdown("""
        This application uses AI agents to analyze your SQL Server database and provide comprehensive 
        recommendations for foreign key relationships, data integrity improvements, and performance optimizations.
        """)
    
    def render_sidebar(self):
        """Render the sidebar with connection status and controls."""
        st.sidebar.header("🔧 Database Connection")
        
        if not st.session_state.db_connected:
            if st.sidebar.button("Connect to Database", type="primary"):
                self.connect_to_database()
        else:
            st.sidebar.success("✅ Connected to Database")
            
            # Database stats
            if self.db_manager:
                stats = self.db_manager.get_database_stats()
                st.sidebar.metric("Tables", stats.get('table_count', 0))
                st.sidebar.metric("Foreign Keys", stats.get('foreign_key_count', 0))
                st.sidebar.metric("DB Size (MB)", stats.get('database_size_mb', 0))
            
            if st.sidebar.button("Disconnect"):
                st.session_state.db_connected = False
                self.db_manager = None
                self.crew = None
                st.rerun()
        
        st.sidebar.header("🤖 AI Agents")
        
        # Agent status display
        agent_names = ['schema_analysis', 'data_integrity', 'constraint_recommendation', 
                      'query_performance', 'change_impact']
        
        for agent_name in agent_names:
            status = st.session_state.agent_status.get(agent_name, 'not_run')
            if status == 'success':
                st.sidebar.markdown(f"✅ {agent_name.replace('_', ' ').title()}")
            elif status == 'error':
                st.sidebar.markdown(f"❌ {agent_name.replace('_', ' ').title()}")
            elif status == 'running':
                st.sidebar.markdown(f"🔄 {agent_name.replace('_', ' ').title()}")
            else:
                st.sidebar.markdown(f"⏸️ {agent_name.replace('_', ' ').title()}")
    
    def render_main_content(self):
        """Render the main content area."""
        if not st.session_state.db_connected:
            self.render_connection_page()
        else:
            self.render_analysis_page()
    
    def render_connection_page(self):
        """Render the database connection page."""
        st.info("👆 Please connect to your database using the sidebar to begin analysis.")
        
        # Show connection instructions
        with st.expander("📋 Connection Setup Instructions"):
            st.markdown("""
            ### Database Connection Setup
            
            1. **Ensure SQL Server is running** with the AdventureWorks2016-backup database
            2. **Update your configuration** in `config/settings.env`:
               - Set your `DB_CONNECTION_STRING`
               - Configure your `OPENAI_API_KEY`
            3. **Click "Connect to Database"** in the sidebar
            
            ### Example Connection String Format:
            ```
            DB_CONNECTION_STRING=mssql+pyodbc:///?odbc_connect=DRIVER%3D%7BODBC+Driver+17+for+SQL+Server%7D%3BSERVER%3D127.0.0.1%2C1433%3BDATABASE%3DAdventureWorks2016-backup%3BUID%3Dsa%3BPWD%3Dyour_password%3BTrustServerCertificate%3Dyes%3BEncrypt%3Dno
            ```
            """)
    
    def render_analysis_page(self):
        """Render the main analysis page."""
        # Analysis controls
        col1, col2, col3 = st.columns([2, 2, 1])
        
        with col1:
            if st.button("🚀 Run All Agents", type="primary", disabled=st.session_state.analysis_running):
                self.run_all_agents()
        
        with col2:
            if st.button("🔄 Clear Results", disabled=st.session_state.analysis_running):
                self.clear_results()
        
        with col3:
            if st.button("📊 Refresh"):
                st.rerun()
        
        # Individual agent controls
        st.subheader("🎯 Individual Agent Controls")
        
        agent_cols = st.columns(5)
        agent_names = ['schema_analysis', 'data_integrity', 'constraint_recommendation', 
                      'query_performance', 'change_impact']
        
        for i, agent_name in enumerate(agent_names):
            with agent_cols[i]:
                display_name = agent_name.replace('_', ' ').title()
                if st.button(f"Run {display_name}", key=f"run_{agent_name}", 
                           disabled=st.session_state.analysis_running):
                    self.run_individual_agent(agent_name)
        
        # Results display
        if st.session_state.crew_results:
            self.render_results()
        else:
            st.info("🔍 Run agents to see analysis results here.")
    
    def run_all_agents(self):
        """Run all agents in sequence."""
        if not self.crew:
            st.error("No database connection available.")
            return
        
        st.session_state.analysis_running = True
        
        # Progress tracking
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        def progress_callback(current: int, total: int, message: str):
            progress = current / total
            progress_bar.progress(progress)
            status_text.text(f"Progress: {current}/{total} - {message}")
        
        try:
            with st.spinner("Running comprehensive database analysis..."):
                results = self.crew.run_all_agents(progress_callback)
                st.session_state.crew_results = results
                st.session_state.agent_status = self.crew.get_execution_status()
                
                if results.get('status') == 'success':
                    st.success("✅ Analysis completed successfully!")
                else:
                    st.warning("⚠️ Analysis completed with some errors.")
                    
        except Exception as e:
            st.error(f"❌ Analysis failed: {str(e)}")
            logger.error(f"Full analysis failed: {e}")
            
        finally:
            st.session_state.analysis_running = False
            progress_bar.empty()
            status_text.empty()
            st.rerun()
    
    def run_individual_agent(self, agent_name: str):
        """Run a single agent."""
        if not self.crew:
            st.error("No database connection available.")
            return
        
        st.session_state.analysis_running = True
        st.session_state.agent_status[agent_name] = 'running'
        
        try:
            with st.spinner(f"Running {agent_name.replace('_', ' ').title()} agent..."):
                result = self.crew.run_individual_agent(agent_name)
                
                # Update session state
                if 'agent_results' not in st.session_state.crew_results:
                    st.session_state.crew_results['agent_results'] = {}
                
                st.session_state.crew_results['agent_results'][agent_name] = result
                st.session_state.agent_status[agent_name] = result.get('status', 'error')
                
                if result.get('status') == 'success':
                    st.success(f"✅ {agent_name.replace('_', ' ').title()} completed successfully!")
                else:
                    st.error(f"❌ {agent_name.replace('_', ' ').title()} failed: {result.get('error', 'Unknown error')}")
                    
        except Exception as e:
            st.error(f"❌ Agent {agent_name} failed: {str(e)}")
            st.session_state.agent_status[agent_name] = 'error'
            logger.error(f"Agent {agent_name} failed: {e}")
            
        finally:
            st.session_state.analysis_running = False
            st.rerun()
    
    def clear_results(self):
        """Clear all analysis results."""
        st.session_state.crew_results = {}
        st.session_state.agent_status = {}
        if self.crew:
            self.crew.clear_results()
        st.success("🗑️ Results cleared!")
        st.rerun()
    
    def render_results(self):
        """Render analysis results."""
        st.header("📊 Analysis Results")
        
        results = st.session_state.crew_results
        
        # Summary metrics
        if 'summary' in results:
            self.render_summary_metrics(results['summary'])
        
        # Agent results tabs
        if 'agent_results' in results:
            self.render_agent_results_tabs(results['agent_results'])
    
    def render_summary_metrics(self, summary: Dict[str, Any]):
        """Render summary metrics."""
        st.subheader("📈 Analysis Summary")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Agents Executed", summary.get('agents_executed', 0))
        with col2:
            st.metric("Successful", summary.get('successful_agents', 0))
        with col3:
            st.metric("Failed", summary.get('failed_agents', 0))
        with col4:
            status = summary.get('overall_status', 'unknown')
            st.metric("Overall Status", status.upper())
        
        # Key findings
        if 'key_findings' in summary:
            st.subheader("🔍 Key Findings")
            
            findings = summary['key_findings']
            
            if 'schema_analysis' in findings:
                schema_findings = findings['schema_analysis']
                st.info(f"**Schema Analysis:** Found {schema_findings.get('missing_foreign_keys', 0)} missing foreign keys with {schema_findings.get('high_confidence_recommendations', 0)} high-confidence recommendations")
            
            if 'data_integrity' in findings:
                integrity_findings = findings['data_integrity']
                st.warning(f"**Data Integrity:** {integrity_findings.get('orphaned_record_issues', 0)} orphaned record issues and {integrity_findings.get('duplicate_issues', 0)} duplicate data issues found")
            
            if 'query_performance' in findings:
                performance_findings = findings['query_performance']
                st.info(f"**Performance:** {performance_findings.get('missing_indexes', 0)} missing indexes identified with {performance_findings.get('optimization_opportunities', 0)} optimization opportunities")
    
    def render_agent_results_tabs(self, agent_results: Dict[str, Any]):
        """Render agent results in tabs."""
        st.subheader("🤖 Detailed Agent Results")
        
        # Create tabs for each agent
        agent_names = list(agent_results.keys())
        if not agent_names:
            st.info("No agent results available.")
            return
        
        tabs = st.tabs([name.replace('_', ' ').title() for name in agent_names])
        
        for i, agent_name in enumerate(agent_names):
            with tabs[i]:
                self.render_individual_agent_results(agent_name, agent_results[agent_name])
    
    def render_individual_agent_results(self, agent_name: str, results: Dict[str, Any]):
        """Render results for an individual agent."""
        if results.get('status') == 'error':
            st.error(f"❌ Agent failed: {results.get('error', 'Unknown error')}")
            return
        
        # Agent-specific result rendering
        if agent_name == 'schema_analysis':
            self.render_schema_analysis_results(results)
        elif agent_name == 'data_integrity':
            self.render_data_integrity_results(results)
        elif agent_name == 'constraint_recommendation':
            self.render_constraint_recommendation_results(results)
        elif agent_name == 'query_performance':
            self.render_query_performance_results(results)
        elif agent_name == 'change_impact':
            self.render_change_impact_results(results)
        else:
            # Generic result display
            st.json(results)
    
    def render_schema_analysis_results(self, results: Dict[str, Any]):
        """Render schema analysis results."""
        st.write("**Schema Analysis Results**")
        
        # Summary
        if 'summary' in results:
            summary = results['summary']
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Recommendations", summary.get('total_recommendations', 0))
            with col2:
                st.metric("High Confidence", summary.get('high_confidence', 0))
            with col3:
                st.metric("Safe to Implement", summary.get('safe_to_implement', 0))
        
        # Recommendations table
        if 'recommendations' in results and results['recommendations']:
            st.subheader("🎯 Foreign Key Recommendations")
            
            recommendations_df = pd.DataFrame(results['recommendations'])
            st.dataframe(recommendations_df, use_container_width=True)
            
            # Download SQL scripts
            if 'sql_statements' in results and results['sql_statements']:
                sql_content = '\n\n'.join(results['sql_statements'])
                st.download_button(
                    label="📥 Download SQL Scripts",
                    data=sql_content,
                    file_name=f"foreign_key_scripts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
                    mime="text/sql"
                )
    
    def render_data_integrity_results(self, results: Dict[str, Any]):
        """Render data integrity audit results."""
        st.write("**Data Integrity Audit Results**")
        
        # Audit summary
        if 'audit_summary' in results:
            summary = results['audit_summary']
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("FK Violations", summary.get('foreign_key_violations', 0))
            with col2:
                st.metric("Orphaned Records", summary.get('orphaned_record_issues', 0))
            with col3:
                st.metric("Duplicate Issues", summary.get('duplicate_issues', 0))
            with col4:
                st.metric("NULL Issues", summary.get('null_value_issues', 0))
        
        # Recommendations
        if 'recommendations' in results and results['recommendations']:
            st.subheader("🔧 Remediation Recommendations")
            
            recommendations_df = pd.DataFrame(results['recommendations'])
            st.dataframe(recommendations_df, use_container_width=True)
            
            # Cleanup scripts
            if 'cleanup_scripts' in results and results['cleanup_scripts']:
                cleanup_content = '\n\n'.join(results['cleanup_scripts'])
                st.download_button(
                    label="📥 Download Cleanup Scripts",
                    data=cleanup_content,
                    file_name=f"data_cleanup_scripts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
                    mime="text/sql"
                )
    
    def render_constraint_recommendation_results(self, results: Dict[str, Any]):
        """Render constraint recommendation results."""
        st.write("**Constraint Implementation Plan**")
        
        # Summary
        if 'summary' in results:
            summary = results['summary']
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Constraints", summary.get('total_constraints', 0))
            with col2:
                st.metric("High Priority", summary.get('high_priority', 0))
            with col3:
                st.metric("Low Risk", summary.get('low_risk', 0))
        
        # Implementation order
        if 'implementation_order' in results:
            st.subheader("📋 Implementation Order")
            order_df = pd.DataFrame(results['implementation_order'])
            st.dataframe(order_df, use_container_width=True)
        
        # DDL Scripts
        if 'ddl_scripts' in results and results['ddl_scripts']:
            ddl_content = '\n\n'.join(results['ddl_scripts'])
            st.download_button(
                label="📥 Download DDL Scripts",
                data=ddl_content,
                file_name=f"constraint_ddl_scripts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
                mime="text/sql"
            )
    
    def render_query_performance_results(self, results: Dict[str, Any]):
        """Render query performance analysis results."""
        st.write("**Query Performance Analysis**")
        
        # Analysis summary
        if 'analysis_summary' in results:
            summary = results['analysis_summary']
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Missing Indexes", summary.get('missing_indexes_found', 0))
            with col2:
                st.metric("Performance Queries", summary.get('performance_queries_analyzed', 0))
            with col3:
                st.metric("Optimizations", summary.get('optimization_opportunities', 0))
        
        # Recommendations
        if 'recommendations' in results and results['recommendations']:
            st.subheader("⚡ Performance Recommendations")
            
            for rec in results['recommendations']:
                with st.expander(f"{rec.get('title', 'Recommendation')} - Priority: {rec.get('priority', 'MEDIUM')}"):
                    st.write(f"**Category:** {rec.get('category', 'N/A')}")
                    st.write(f"**Description:** {rec.get('description', 'N/A')}")
                    st.write(f"**Impact:** {rec.get('impact', 'N/A')}")
                    if 'action_items' in rec:
                        st.write("**Action Items:**")
                        for item in rec['action_items']:
                            st.write(f"- {item}")
    
    def render_change_impact_results(self, results: Dict[str, Any]):
        """Render change impact analysis results."""
        st.write("**Change Impact Analysis**")
        
        # Executive summary
        if 'executive_summary' in results:
            summary = results['executive_summary']
            
            st.subheader("📋 Executive Summary")
            
            if 'project_overview' in summary:
                overview = summary['project_overview']
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Changes", overview.get('scope', 'N/A'))
                with col2:
                    st.metric("Duration", overview.get('duration', 'N/A'))
                with col3:
                    st.metric("Risk Level", overview.get('risk_level', 'N/A'))
            
            if 'key_benefits' in summary:
                st.subheader("✅ Key Benefits")
                for benefit in summary['key_benefits']:
                    st.write(f"- {benefit}")
            
            if 'recommendation' in summary:
                rec = summary['recommendation']
                st.subheader("🎯 Final Recommendation")
                
                recommendation_type = rec.get('recommendation', 'UNKNOWN')
                if recommendation_type == 'GO':
                    st.success(f"✅ **{recommendation_type}**: {rec.get('reasoning', '')}")
                elif recommendation_type == 'CONDITIONAL GO':
                    st.warning(f"⚠️ **{recommendation_type}**: {rec.get('reasoning', '')}")
                else:
                    st.error(f"❌ **{recommendation_type}**: {rec.get('reasoning', '')}")
        
        # Implementation timeline
        if 'implementation_timeline' in results:
            timeline = results['implementation_timeline']
            st.subheader("📅 Implementation Timeline")
            
            if 'phases' in timeline:
                for phase in timeline['phases']:
                    with st.expander(f"Phase {phase.get('phase', 'N/A')}: {phase.get('name', 'N/A')} - {phase.get('duration', 'N/A')}"):
                        st.write(f"**Risk Level:** {phase.get('risk_level', 'N/A')}")
                        st.write("**Activities:**")
                        for activity in phase.get('activities', []):
                            st.write(f"- {activity}")
    
    def render_logs(self):
        """Render application logs."""
        st.subheader("📝 Application Logs")
        
        logs = streamlit_handler.get_logs()
        if logs:
            # Filter logs by level
            log_level = st.selectbox("Log Level", ["ALL", "INFO", "WARNING", "ERROR"])
            
            filtered_logs = logs if log_level == "ALL" else streamlit_handler.get_logs(log_level)
            
            # Display logs
            for log in reversed(filtered_logs[-50:]):  # Show last 50 logs
                timestamp = datetime.fromtimestamp(log['timestamp']).strftime('%H:%M:%S')
                level = log['level']
                message = log['message']
                
                if level == 'ERROR':
                    st.error(f"[{timestamp}] {message}")
                elif level == 'WARNING':
                    st.warning(f"[{timestamp}] {message}")
                else:
                    st.info(f"[{timestamp}] {message}")
        else:
            st.info("No logs available.")
    
    def run(self):
        """Run the Streamlit application."""
        try:
            self.render_header()
            self.render_sidebar()
            self.render_main_content()
            
            # Logs section (collapsible)
            with st.expander("📝 View Application Logs"):
                self.render_logs()
                
        except Exception as e:
            st.error(f"Application error: {str(e)}")
            st.error("Full traceback:")
            st.code(traceback.format_exc())
            logger.error(f"Application error: {e}")


def main():
    """Main application entry point."""
    app = StreamlitApp()
    app.run()


if __name__ == "__main__":
    main()
