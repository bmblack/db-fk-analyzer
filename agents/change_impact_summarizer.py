"""
Change Impact Summarizer Agent - Summarizes risk and impact of proposed changes.
"""
import logging
from typing import Dict, Any, List
from crewai import Agent, Task

logger = logging.getLogger(__name__)

class ChangeImpactSummarizer:
    """Agent responsible for summarizing the risk and impact of proposed database changes."""
    
    def __init__(self, database_manager):
        """Initialize the Change Impact Summarizer."""
        self.db_manager = database_manager
        self.agent = self._create_agent()
    
    def _create_agent(self) -> Agent:
        """Create the CrewAI agent."""
        return Agent(
            role="Risk Analyst",
            goal="Assess impact and risk of proposed database changes",
            backstory="""You are a senior database risk analyst with extensive experience in 
            change management and impact assessment. You excel at evaluating the potential 
            consequences of database modifications and providing comprehensive risk assessments 
            that help stakeholders make informed decisions.""",
            verbose=True,
            allow_delegation=False
        )
    
    def create_summary_task(self) -> Task:
        """Create the change impact summary task."""
        return Task(
            description="""Analyze and summarize the impact of all proposed database changes:
            
            1. Assess overall risk level of implementing all recommendations
            2. Identify dependencies between different changes
            3. Estimate implementation timeline and resource requirements
            4. Highlight potential business impact and downtime
            5. Recommend implementation phases and rollback strategies
            6. Provide executive summary for stakeholders
            
            Consider:
            - Data integrity risks
            - Performance impact during implementation
            - Application compatibility
            - Rollback complexity
            - Business continuity requirements""",
            agent=self.agent,
            expected_output="""A comprehensive change impact report containing:
            - Executive summary of all proposed changes
            - Risk assessment matrix
            - Implementation timeline and phases
            - Resource requirements and dependencies
            - Rollback strategy and contingency plans
            - Business impact assessment"""
        )
    
    def summarize_change_impact(self, all_agent_results: Dict[str, Any]) -> Dict[str, Any]:
        """Summarize the impact of all proposed changes across all agents."""
        try:
            logger.info("Starting change impact summarization...")
            
            # Extract results from all agents
            schema_results = all_agent_results.get('schema_analysis', {})
            integrity_results = all_agent_results.get('data_integrity', {})
            constraint_results = all_agent_results.get('constraint_recommendations', {})
            performance_results = all_agent_results.get('query_performance', {})
            
            # Generate comprehensive impact assessment
            impact_assessment = self._assess_overall_impact(
                schema_results, integrity_results, constraint_results, performance_results
            )
            
            # Create implementation timeline
            implementation_timeline = self._create_implementation_timeline(all_agent_results)
            
            # Generate risk matrix
            risk_matrix = self._generate_risk_matrix(all_agent_results)
            
            # Create executive summary
            executive_summary = self._create_executive_summary(
                impact_assessment, implementation_timeline, risk_matrix
            )
            
            result = {
                'status': 'success',
                'executive_summary': executive_summary,
                'impact_assessment': impact_assessment,
                'implementation_timeline': implementation_timeline,
                'risk_matrix': risk_matrix,
                'recommendations': self._generate_final_recommendations(all_agent_results),
                'success_metrics': self._define_success_metrics(all_agent_results)
            }
            
            logger.info("Change impact summarization completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Change impact summarization failed: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'executive_summary': {},
                'recommendations': []
            }
    
    def _assess_overall_impact(self, schema_results: Dict, integrity_results: Dict,
                             constraint_results: Dict, performance_results: Dict) -> Dict[str, Any]:
        """Assess the overall impact of all proposed changes."""
        
        # Count total changes
        total_fk_recommendations = len(schema_results.get('recommendations', []))
        total_integrity_issues = len(integrity_results.get('recommendations', []))
        total_constraints = constraint_results.get('total_constraints', 0)
        total_performance_optimizations = len(performance_results.get('optimizations', []))
        
        # Assess risk levels
        high_risk_changes = 0
        medium_risk_changes = 0
        low_risk_changes = 0
        
        # Count risk levels from schema analysis
        for rec in schema_results.get('recommendations', []):
            if rec.get('risk_level') == 'HIGH':
                high_risk_changes += 1
            elif rec.get('risk_level') == 'MEDIUM':
                medium_risk_changes += 1
            else:
                low_risk_changes += 1
        
        # Count risk levels from constraint recommendations
        for plan in constraint_results.get('constraint_plans', []):
            risk_level = plan.get('risk_assessment', {}).get('risk_level', 'LOW')
            if risk_level == 'HIGH':
                high_risk_changes += 1
            elif risk_level == 'MEDIUM':
                medium_risk_changes += 1
            else:
                low_risk_changes += 1
        
        # Determine overall risk level
        if high_risk_changes > 0:
            overall_risk = 'HIGH'
        elif medium_risk_changes > total_fk_recommendations * 0.3:  # More than 30% medium risk
            overall_risk = 'MEDIUM'
        else:
            overall_risk = 'LOW'
        
        return {
            'total_changes': total_fk_recommendations + total_integrity_issues + total_performance_optimizations,
            'foreign_key_changes': total_fk_recommendations,
            'integrity_fixes': total_integrity_issues,
            'performance_optimizations': total_performance_optimizations,
            'constraint_implementations': total_constraints,
            'risk_distribution': {
                'high_risk': high_risk_changes,
                'medium_risk': medium_risk_changes,
                'low_risk': low_risk_changes
            },
            'overall_risk_level': overall_risk,
            'estimated_effort': self._estimate_total_effort(
                total_fk_recommendations, total_integrity_issues, total_performance_optimizations
            )
        }
    
    def _estimate_total_effort(self, fk_count: int, integrity_count: int, performance_count: int) -> Dict[str, Any]:
        """Estimate total effort required for implementation."""
        
        # Base time estimates (in hours)
        fk_effort = fk_count * 2  # 2 hours per FK on average
        integrity_effort = integrity_count * 4  # 4 hours per integrity issue
        performance_effort = performance_count * 1  # 1 hour per performance optimization
        
        total_hours = fk_effort + integrity_effort + performance_effort
        total_days = max(1, total_hours / 8)  # Convert to working days
        
        return {
            'total_hours': total_hours,
            'total_days': round(total_days, 1),
            'breakdown': {
                'foreign_key_work': fk_effort,
                'integrity_fixes': integrity_effort,
                'performance_optimizations': performance_effort
            },
            'team_size_recommendation': 'Small team (1-2 DBAs)' if total_days <= 5 else 'Medium team (2-3 DBAs)'
        }
    
    def _create_implementation_timeline(self, all_results: Dict[str, Any]) -> Dict[str, Any]:
        """Create a phased implementation timeline."""
        
        phases = [
            {
                'phase': 1,
                'name': 'Data Cleanup and Preparation',
                'duration': '1-2 weeks',
                'activities': [
                    'Backup all affected tables',
                    'Clean up orphaned records',
                    'Resolve duplicate data issues',
                    'Validate data integrity'
                ],
                'risk_level': 'MEDIUM',
                'dependencies': []
            },
            {
                'phase': 2,
                'name': 'Index Creation',
                'duration': '3-5 days',
                'activities': [
                    'Create missing indexes on FK columns',
                    'Monitor index creation performance',
                    'Validate index effectiveness'
                ],
                'risk_level': 'LOW',
                'dependencies': ['Phase 1 completion']
            },
            {
                'phase': 3,
                'name': 'Foreign Key Implementation',
                'duration': '1-2 weeks',
                'activities': [
                    'Implement high-confidence FK constraints',
                    'Test constraint functionality',
                    'Monitor application performance',
                    'Implement medium-confidence constraints'
                ],
                'risk_level': 'HIGH',
                'dependencies': ['Phase 1 and 2 completion']
            },
            {
                'phase': 4,
                'name': 'Performance Optimization',
                'duration': '3-5 days',
                'activities': [
                    'Optimize query patterns',
                    'Fine-tune indexes',
                    'Monitor performance improvements'
                ],
                'risk_level': 'LOW',
                'dependencies': ['Phase 3 completion']
            },
            {
                'phase': 5,
                'name': 'Monitoring and Validation',
                'duration': '1 week',
                'activities': [
                    'Set up monitoring alerts',
                    'Validate all constraints',
                    'Performance testing',
                    'Documentation updates'
                ],
                'risk_level': 'LOW',
                'dependencies': ['All previous phases']
            }
        ]
        
        total_duration = '6-10 weeks'
        critical_path = ['Phase 1', 'Phase 3']
        
        return {
            'phases': phases,
            'total_duration': total_duration,
            'critical_path': critical_path,
            'parallel_opportunities': ['Phase 2 can overlap with Phase 1 completion'],
            'milestone_checkpoints': [
                'Phase 1: Data quality validated',
                'Phase 3: Core FK constraints implemented',
                'Phase 5: Full system validation complete'
            ]
        }
    
    def _generate_risk_matrix(self, all_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a comprehensive risk matrix."""
        
        risks = [
            {
                'risk_category': 'Data Loss',
                'probability': 'LOW',
                'impact': 'HIGH',
                'risk_score': 'MEDIUM',
                'mitigation': 'Comprehensive backups before any changes',
                'contingency': 'Full database restore from backup'
            },
            {
                'risk_category': 'Application Downtime',
                'probability': 'MEDIUM',
                'impact': 'HIGH',
                'risk_score': 'HIGH',
                'mitigation': 'Implement during maintenance windows',
                'contingency': 'Rollback scripts and constraint removal'
            },
            {
                'risk_category': 'Performance Degradation',
                'probability': 'LOW',
                'impact': 'MEDIUM',
                'risk_score': 'LOW',
                'mitigation': 'Thorough testing and monitoring',
                'contingency': 'Constraint disabling and index optimization'
            },
            {
                'risk_category': 'Constraint Violations',
                'probability': 'MEDIUM',
                'impact': 'MEDIUM',
                'risk_score': 'MEDIUM',
                'mitigation': 'Data cleanup before constraint creation',
                'contingency': 'Constraint modification or removal'
            },
            {
                'risk_category': 'Implementation Delays',
                'probability': 'MEDIUM',
                'impact': 'LOW',
                'risk_score': 'LOW',
                'mitigation': 'Phased approach with clear milestones',
                'contingency': 'Scope reduction and priority adjustment'
            }
        ]
        
        return {
            'risks': risks,
            'overall_risk_rating': 'MEDIUM',
            'key_risk_factors': [
                'Application downtime during constraint implementation',
                'Potential data cleanup complexity',
                'Coordination with application teams'
            ],
            'risk_mitigation_summary': 'Risks are manageable with proper planning and phased implementation'
        }
    
    def _create_executive_summary(self, impact_assessment: Dict, timeline: Dict, 
                                risk_matrix: Dict) -> Dict[str, Any]:
        """Create executive summary for stakeholders."""
        
        return {
            'project_overview': {
                'title': 'Database Foreign Key Analysis and Remediation',
                'scope': f"{impact_assessment['total_changes']} total improvements identified",
                'duration': timeline['total_duration'],
                'risk_level': impact_assessment['overall_risk_level']
            },
            'key_benefits': [
                'Improved data integrity and consistency',
                'Enhanced query performance through proper indexing',
                'Reduced risk of orphaned records',
                'Better database documentation and relationships',
                'Improved application reliability'
            ],
            'resource_requirements': {
                'team_size': impact_assessment['estimated_effort']['team_size_recommendation'],
                'estimated_effort': f"{impact_assessment['estimated_effort']['total_days']} working days",
                'budget_considerations': 'Primarily internal DBA time, minimal external costs'
            },
            'success_criteria': [
                'All high-confidence FK constraints implemented',
                'Zero data integrity violations',
                'No performance degradation',
                'Successful application compatibility testing'
            ],
            'recommendation': self._generate_go_no_go_recommendation(impact_assessment, risk_matrix)
        }
    
    def _generate_go_no_go_recommendation(self, impact_assessment: Dict, risk_matrix: Dict) -> Dict[str, str]:
        """Generate go/no-go recommendation based on analysis."""
        
        total_changes = impact_assessment['total_changes']
        risk_level = impact_assessment['overall_risk_level']
        high_risk_count = impact_assessment['risk_distribution']['high_risk']
        
        if total_changes == 0:
            return {
                'recommendation': 'NO-GO',
                'reasoning': 'No significant improvements identified',
                'alternative': 'Continue monitoring for future opportunities'
            }
        elif high_risk_count > total_changes * 0.5:  # More than 50% high risk
            return {
                'recommendation': 'CONDITIONAL GO',
                'reasoning': 'High number of risky changes require careful evaluation',
                'alternative': 'Implement only low and medium risk changes initially'
            }
        elif risk_level == 'LOW' and total_changes > 0:
            return {
                'recommendation': 'GO',
                'reasoning': 'Low risk with clear benefits justify implementation',
                'alternative': 'Proceed with full implementation as planned'
            }
        else:
            return {
                'recommendation': 'GO',
                'reasoning': 'Benefits outweigh risks with proper mitigation',
                'alternative': 'Proceed with phased implementation approach'
            }
    
    def _generate_final_recommendations(self, all_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate final high-level recommendations."""
        
        return [
            {
                'priority': 'HIGH',
                'category': 'Implementation Strategy',
                'recommendation': 'Adopt phased implementation approach',
                'rationale': 'Reduces risk and allows for course correction',
                'action_items': [
                    'Start with data cleanup phase',
                    'Implement high-confidence constraints first',
                    'Monitor each phase before proceeding'
                ]
            },
            {
                'priority': 'HIGH',
                'category': 'Risk Management',
                'recommendation': 'Establish comprehensive backup and rollback procedures',
                'rationale': 'Critical for business continuity',
                'action_items': [
                    'Full database backup before starting',
                    'Test rollback procedures',
                    'Prepare constraint removal scripts'
                ]
            },
            {
                'priority': 'MEDIUM',
                'category': 'Performance Monitoring',
                'recommendation': 'Implement continuous performance monitoring',
                'rationale': 'Early detection of performance issues',
                'action_items': [
                    'Set up query performance baselines',
                    'Monitor constraint creation impact',
                    'Track application response times'
                ]
            },
            {
                'priority': 'MEDIUM',
                'category': 'Team Coordination',
                'recommendation': 'Coordinate with application development teams',
                'rationale': 'Ensure application compatibility',
                'action_items': [
                    'Review constraint impact on applications',
                    'Plan coordinated testing',
                    'Establish communication protocols'
                ]
            }
        ]
    
    def _define_success_metrics(self, all_results: Dict[str, Any]) -> Dict[str, Any]:
        """Define metrics to measure implementation success."""
        
        return {
            'data_integrity_metrics': [
                'Zero orphaned records in FK relationships',
                'All implemented constraints pass validation',
                'No constraint violation errors in applications'
            ],
            'performance_metrics': [
                'Query performance maintained or improved',
                'Index usage statistics show positive utilization',
                'No increase in average query execution time'
            ],
            'operational_metrics': [
                'Implementation completed within timeline',
                'No unplanned downtime during implementation',
                'All rollback procedures tested and documented'
            ],
            'business_metrics': [
                'Application functionality unchanged',
                'Data quality reports show improvement',
                'Reduced manual data cleanup requirements'
            ]
        }
