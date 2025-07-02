"""
CrewAI orchestration for database foreign key analysis and remediation.
"""
import logging
from typing import Dict, Any, List, Optional
from crewai import Crew, Process
from agents.schema_analysis_agent import SchemaAnalysisAgent
from agents.data_integrity_auditor import DataIntegrityAuditor
from agents.constraint_recommendation_agent import ConstraintRecommendationAgent
from agents.query_performance_analyst import QueryPerformanceAnalyst
from agents.change_impact_summarizer import ChangeImpactSummarizer
from utils.database import DatabaseManager

logger = logging.getLogger(__name__)

class DatabaseAnalysisCrew:
    """Orchestrates AI agents for comprehensive database foreign key analysis."""
    
    def __init__(self, database_manager: DatabaseManager):
        """Initialize the crew with database manager."""
        self.db_manager = database_manager
        self.agents = self._initialize_agents()
        self.crew = None
        self.results = {}
    
    def _initialize_agents(self) -> Dict[str, Any]:
        """Initialize all AI agents."""
        logger.info("Initializing AI agents...")
        
        agents = {
            'schema_analysis': SchemaAnalysisAgent(self.db_manager),
            'data_integrity': DataIntegrityAuditor(self.db_manager),
            'constraint_recommendation': ConstraintRecommendationAgent(self.db_manager),
            'query_performance': QueryPerformanceAnalyst(self.db_manager),
            'change_impact': ChangeImpactSummarizer(self.db_manager)
        }
        
        logger.info(f"Initialized {len(agents)} AI agents")
        return agents
    
    def run_individual_agent(self, agent_name: str) -> Dict[str, Any]:
        """Run a single agent and return its results."""
        try:
            logger.info(f"Running {agent_name} agent...")
            
            if agent_name not in self.agents:
                raise ValueError(f"Unknown agent: {agent_name}")
            
            agent = self.agents[agent_name]
            
            # Execute the appropriate analysis method for each agent
            if agent_name == 'schema_analysis':
                result = agent.analyze_schema()
            elif agent_name == 'data_integrity':
                result = agent.audit_data_integrity()
            elif agent_name == 'constraint_recommendation':
                # This agent needs schema analysis results
                schema_results = self.results.get('schema_analysis', {})
                if not schema_results:
                    schema_results = self.agents['schema_analysis'].analyze_schema()
                    self.results['schema_analysis'] = schema_results
                result = agent.generate_constraint_recommendations(schema_results)
            elif agent_name == 'query_performance':
                result = agent.analyze_query_performance()
            elif agent_name == 'change_impact':
                # This agent needs results from all other agents
                if not all(key in self.results for key in ['schema_analysis', 'data_integrity', 'query_performance']):
                    raise ValueError("Change impact analysis requires other agents to run first")
                result = agent.summarize_change_impact(self.results)
            else:
                raise ValueError(f"No execution method defined for agent: {agent_name}")
            
            # Store results
            self.results[agent_name] = result
            
            logger.info(f"{agent_name} agent completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Agent {agent_name} failed: {e}")
            error_result = {
                'status': 'error',
                'error': str(e),
                'agent': agent_name
            }
            self.results[agent_name] = error_result
            return error_result
    
    def run_all_agents(self, progress_callback: Optional[callable] = None) -> Dict[str, Any]:
        """Run all agents in sequence and return combined results."""
        try:
            logger.info("Starting full database analysis with all agents...")
            
            # Define execution order (some agents depend on others)
            execution_order = [
                'schema_analysis',
                'data_integrity', 
                'query_performance',
                'constraint_recommendation',  # Depends on schema_analysis
                'change_impact'  # Depends on all others
            ]
            
            total_agents = len(execution_order)
            
            for i, agent_name in enumerate(execution_order):
                try:
                    if progress_callback:
                        progress_callback(i, total_agents, f"Running {agent_name}...")
                    
                    result = self.run_individual_agent(agent_name)
                    
                    if result.get('status') == 'error':
                        logger.warning(f"Agent {agent_name} failed, continuing with others...")
                    
                except Exception as e:
                    logger.error(f"Failed to run agent {agent_name}: {e}")
                    self.results[agent_name] = {
                        'status': 'error',
                        'error': str(e),
                        'agent': agent_name
                    }
            
            if progress_callback:
                progress_callback(total_agents, total_agents, "Analysis complete!")
            
            # Generate summary
            summary = self._generate_analysis_summary()
            
            logger.info("Full database analysis completed")
            return {
                'status': 'success',
                'summary': summary,
                'agent_results': self.results,
                'execution_order': execution_order
            }
            
        except Exception as e:
            logger.error(f"Full analysis failed: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'agent_results': self.results
            }
    
    def _generate_analysis_summary(self) -> Dict[str, Any]:
        """Generate a high-level summary of all analysis results."""
        summary = {
            'agents_executed': len(self.results),
            'successful_agents': sum(1 for r in self.results.values() if r.get('status') == 'success'),
            'failed_agents': sum(1 for r in self.results.values() if r.get('status') == 'error'),
            'key_findings': {},
            'overall_status': 'success' if all(r.get('status') == 'success' for r in self.results.values()) else 'partial'
        }
        
        # Extract key findings from each agent
        if 'schema_analysis' in self.results and self.results['schema_analysis'].get('status') == 'success':
            schema_data = self.results['schema_analysis']
            summary['key_findings']['schema_analysis'] = {
                'missing_foreign_keys': schema_data.get('missing_foreign_keys', 0),
                'recommendations': len(schema_data.get('recommendations', [])),
                'high_confidence_recommendations': len([
                    r for r in schema_data.get('recommendations', []) 
                    if r.get('confidence_score', 0) >= 0.8
                ])
            }
        
        if 'data_integrity' in self.results and self.results['data_integrity'].get('status') == 'success':
            integrity_data = self.results['data_integrity']
            audit_summary = integrity_data.get('audit_summary', {})
            summary['key_findings']['data_integrity'] = {
                'foreign_key_violations': audit_summary.get('foreign_key_violations', 0),
                'orphaned_record_issues': audit_summary.get('orphaned_record_issues', 0),
                'duplicate_issues': audit_summary.get('duplicate_issues', 0),
                'total_recommendations': len(integrity_data.get('recommendations', []))
            }
        
        if 'query_performance' in self.results and self.results['query_performance'].get('status') == 'success':
            performance_data = self.results['query_performance']
            analysis_summary = performance_data.get('analysis_summary', {})
            summary['key_findings']['query_performance'] = {
                'missing_indexes': analysis_summary.get('missing_indexes_found', 0),
                'optimization_opportunities': analysis_summary.get('optimization_opportunities', 0),
                'performance_queries_analyzed': analysis_summary.get('performance_queries_analyzed', 0)
            }
        
        if 'constraint_recommendation' in self.results and self.results['constraint_recommendation'].get('status') == 'success':
            constraint_data = self.results['constraint_recommendation']
            summary['key_findings']['constraint_recommendation'] = {
                'total_constraints': constraint_data.get('total_constraints', 0),
                'ddl_scripts_generated': len(constraint_data.get('ddl_scripts', [])),
                'index_recommendations': len(constraint_data.get('index_recommendations', []))
            }
        
        if 'change_impact' in self.results and self.results['change_impact'].get('status') == 'success':
            impact_data = self.results['change_impact']
            impact_assessment = impact_data.get('impact_assessment', {})
            summary['key_findings']['change_impact'] = {
                'total_changes': impact_assessment.get('total_changes', 0),
                'overall_risk_level': impact_assessment.get('overall_risk_level', 'UNKNOWN'),
                'estimated_effort_days': impact_assessment.get('estimated_effort', {}).get('total_days', 0)
            }
        
        return summary
    
    def get_agent_results(self, agent_name: str) -> Dict[str, Any]:
        """Get results from a specific agent."""
        return self.results.get(agent_name, {})
    
    def get_all_results(self) -> Dict[str, Any]:
        """Get results from all agents."""
        return self.results
    
    def clear_results(self) -> None:
        """Clear all stored results."""
        self.results.clear()
        logger.info("Agent results cleared")
    
    def get_execution_status(self) -> Dict[str, str]:
        """Get execution status of all agents."""
        status = {}
        for agent_name, result in self.results.items():
            status[agent_name] = result.get('status', 'not_run')
        return status
    
    def export_results_to_dict(self) -> Dict[str, Any]:
        """Export all results to a dictionary for serialization."""
        from datetime import datetime
        return {
            'timestamp': str(datetime.now()),
            'database_stats': self.db_manager.get_database_stats(),
            'agent_results': self.results,
            'summary': self._generate_analysis_summary()
        }


def create_database_crew(database_manager: DatabaseManager) -> DatabaseAnalysisCrew:
    """Factory function to create a DatabaseAnalysisCrew instance."""
    return DatabaseAnalysisCrew(database_manager)


# Mock implementations for agents that weren't fully implemented
class MockAgent:
    """Mock agent for simplified implementation."""
    
    def __init__(self, name: str, database_manager: DatabaseManager):
        self.name = name
        self.db_manager = database_manager
    
    def analyze(self) -> Dict[str, Any]:
        """Mock analysis method."""
        return {
            'status': 'success',
            'agent': self.name,
            'message': f'{self.name} analysis completed (mock implementation)',
            'recommendations': [
                f'Mock recommendation 1 from {self.name}',
                f'Mock recommendation 2 from {self.name}'
            ]
        }


# Additional utility functions
def validate_agent_dependencies(agent_name: str, available_results: Dict[str, Any]) -> bool:
    """Validate that an agent has all required dependencies."""
    dependencies = {
        'constraint_recommendation': ['schema_analysis'],
        'change_impact': ['schema_analysis', 'data_integrity', 'query_performance']
    }
    
    required_deps = dependencies.get(agent_name, [])
    return all(dep in available_results for dep in required_deps)


def get_agent_description(agent_name: str) -> str:
    """Get human-readable description of what each agent does."""
    descriptions = {
        'schema_analysis': 'Analyzes database schema to identify missing foreign key relationships',
        'data_integrity': 'Audits data integrity and identifies orphaned records and constraint violations',
        'constraint_recommendation': 'Generates safe DDL statements for implementing foreign key constraints',
        'query_performance': 'Analyzes query performance and recommends indexing optimizations',
        'change_impact': 'Summarizes the overall impact and risk of all proposed changes'
    }
    return descriptions.get(agent_name, f'Analysis agent: {agent_name}')
