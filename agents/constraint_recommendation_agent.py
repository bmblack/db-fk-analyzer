"""
Constraint Recommendation Agent - Generates safe foreign key DDL statements.
"""
import logging
from typing import Dict, Any, List
from crewai import Agent, Task
import pandas as pd

logger = logging.getLogger(__name__)

class ConstraintRecommendationAgent:
    """Agent responsible for generating safe foreign key constraint recommendations."""
    
    def __init__(self, database_manager):
        """Initialize the Constraint Recommendation Agent."""
        self.db_manager = database_manager
        self.agent = self._create_agent()
    
    def _create_agent(self) -> Agent:
        """Create the CrewAI agent."""
        return Agent(
            role="Database Architect",
            goal="Generate safe and optimized foreign key constraint DDL statements",
            backstory="""You are a senior database architect with deep expertise in 
            constraint design and implementation. You understand the nuances of foreign key 
            constraints, cascading options, and their impact on performance and data integrity. 
            You always prioritize safety and consider the business impact of constraint changes.""",
            verbose=True,
            allow_delegation=False
        )
    
    def create_recommendation_task(self) -> Task:
        """Create the constraint recommendation task."""
        return Task(
            description="""Generate comprehensive foreign key constraint recommendations including:
            
            1. Safe DDL statements for creating foreign key constraints
            2. Appropriate cascading options (CASCADE, SET NULL, RESTRICT)
            3. Performance considerations and indexing recommendations
            4. Implementation order to avoid circular dependencies
            5. Rollback scripts for each constraint
            6. Testing strategies to validate constraints
            
            Consider:
            - Data integrity requirements
            - Performance impact on INSERT/UPDATE/DELETE operations
            - Business rules and cascading behavior
            - Existing indexes and their optimization
            - Constraint naming conventions""",
            agent=self.agent,
            expected_output="""A detailed constraint implementation plan containing:
            - Prioritized list of constraints to implement
            - Complete DDL scripts with proper error handling
            - Cascading option recommendations with justification
            - Index recommendations for optimal performance
            - Implementation timeline and dependencies
            - Risk assessment and mitigation strategies"""
        )
    
    def generate_constraint_recommendations(self, schema_analysis_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comprehensive constraint recommendations based on schema analysis."""
        try:
            logger.info("Starting constraint recommendation generation...")
            
            recommendations = schema_analysis_results.get('recommendations', [])
            
            # Generate detailed constraint plans
            constraint_plans = self._create_constraint_plans(recommendations)
            
            # Determine implementation order
            implementation_order = self._determine_implementation_order(constraint_plans)
            
            # Generate DDL scripts
            ddl_scripts = self._generate_ddl_scripts(constraint_plans)
            
            # Create rollback scripts
            rollback_scripts = self._generate_rollback_scripts(constraint_plans)
            
            # Generate index recommendations
            index_recommendations = self._generate_index_recommendations(constraint_plans)
            
            result = {
                'status': 'success',
                'total_constraints': len(constraint_plans),
                'constraint_plans': constraint_plans,
                'implementation_order': implementation_order,
                'ddl_scripts': ddl_scripts,
                'rollback_scripts': rollback_scripts,
                'index_recommendations': index_recommendations,
                'summary': self._generate_implementation_summary(constraint_plans)
            }
            
            logger.info("Constraint recommendations generated successfully")
            return result
            
        except Exception as e:
            logger.error(f"Constraint recommendation generation failed: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'constraint_plans': [],
                'ddl_scripts': []
            }
    
    def _create_constraint_plans(self, recommendations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create detailed constraint implementation plans."""
        constraint_plans = []
        
        for rec in recommendations:
            # Determine cascading options
            cascade_options = self._determine_cascade_options(rec)
            
            # Assess implementation risk
            risk_assessment = self._assess_implementation_risk(rec)
            
            # Generate constraint name
            constraint_name = self._generate_constraint_name(rec)
            
            plan = {
                'constraint_name': constraint_name,
                'source_table': rec['source_table'],
                'source_column': rec['source_column'],
                'target_table': rec['target_table'],
                'target_column': rec['target_column'],
                'confidence_score': rec['confidence_score'],
                'cascade_options': cascade_options,
                'risk_assessment': risk_assessment,
                'implementation_priority': self._calculate_implementation_priority(rec, risk_assessment),
                'requires_index': self._check_index_requirement(rec),
                'estimated_impact': self._estimate_performance_impact(rec)
            }
            
            constraint_plans.append(plan)
        
        return constraint_plans
    
    def _determine_cascade_options(self, recommendation: Dict[str, Any]) -> Dict[str, str]:
        """Determine appropriate cascading options for the constraint."""
        # Default to RESTRICT for safety
        cascade_options = {
            'on_delete': 'RESTRICT',
            'on_update': 'CASCADE',
            'reasoning': 'Default safe options: RESTRICT on DELETE to prevent accidental data loss, CASCADE on UPDATE for consistency'
        }
        
        # Analyze table names and relationships for better cascade options
        source_table = recommendation['source_table'].lower()
        target_table = recommendation['target_table'].lower()
        
        # Common patterns for cascade decisions
        if any(keyword in source_table for keyword in ['detail', 'item', 'line']):
            cascade_options.update({
                'on_delete': 'CASCADE',
                'reasoning': 'Detail/line items should be deleted when parent is deleted'
            })
        elif any(keyword in source_table for keyword in ['log', 'audit', 'history']):
            cascade_options.update({
                'on_delete': 'SET NULL',
                'reasoning': 'Historical records should preserve data even if parent is deleted'
            })
        elif any(keyword in target_table for keyword in ['lookup', 'reference', 'type']):
            cascade_options.update({
                'on_delete': 'RESTRICT',
                'reasoning': 'Reference data should not be deleted if still in use'
            })
        
        return cascade_options
    
    def _assess_implementation_risk(self, recommendation: Dict[str, Any]) -> Dict[str, Any]:
        """Assess the risk of implementing the constraint."""
        risk_factors = []
        risk_level = 'LOW'
        
        # Check for orphaned records
        if recommendation.get('orphaned_records', 0) > 0:
            risk_factors.append(f"{recommendation['orphaned_records']} orphaned records require cleanup")
            risk_level = 'HIGH'
        
        # Check confidence score
        if recommendation['confidence_score'] < 0.7:
            risk_factors.append("Low confidence in relationship accuracy")
            risk_level = 'MEDIUM' if risk_level == 'LOW' else risk_level
        
        # Check table size (estimate based on common patterns)
        if any(keyword in recommendation['source_table'].lower() for keyword in ['transaction', 'order', 'log']):
            risk_factors.append("Large table - constraint creation may take significant time")
            risk_level = 'MEDIUM' if risk_level == 'LOW' else risk_level
        
        return {
            'risk_level': risk_level,
            'risk_factors': risk_factors,
            'mitigation_steps': self._generate_mitigation_steps(risk_factors)
        }
    
    def _generate_mitigation_steps(self, risk_factors: List[str]) -> List[str]:
        """Generate mitigation steps for identified risks."""
        mitigation_steps = []
        
        for factor in risk_factors:
            if 'orphaned records' in factor:
                mitigation_steps.append("Clean up orphaned records before constraint creation")
                mitigation_steps.append("Backup affected tables before cleanup")
            elif 'Low confidence' in factor:
                mitigation_steps.append("Manual review of relationship accuracy recommended")
                mitigation_steps.append("Test constraint on subset of data first")
            elif 'Large table' in factor:
                mitigation_steps.append("Schedule constraint creation during maintenance window")
                mitigation_steps.append("Monitor system resources during creation")
        
        return mitigation_steps
    
    def _generate_constraint_name(self, recommendation: Dict[str, Any]) -> str:
        """Generate a standardized constraint name."""
        source_table = recommendation['source_table']
        source_column = recommendation['source_column']
        target_table = recommendation['target_table']
        
        # Standard naming convention: FK_SourceTable_SourceColumn_TargetTable
        constraint_name = f"FK_{source_table}_{source_column}_{target_table}"
        
        # Truncate if too long (SQL Server limit is 128 characters)
        if len(constraint_name) > 120:
            constraint_name = f"FK_{source_table[:20]}_{source_column[:20]}_{target_table[:20]}"
        
        return constraint_name
    
    def _calculate_implementation_priority(self, recommendation: Dict[str, Any], 
                                         risk_assessment: Dict[str, Any]) -> int:
        """Calculate implementation priority (1-10, higher is more urgent)."""
        priority = 5  # Base priority
        
        # Boost priority for high confidence
        if recommendation['confidence_score'] >= 0.8:
            priority += 2
        elif recommendation['confidence_score'] >= 0.6:
            priority += 1
        
        # Reduce priority for high risk
        if risk_assessment['risk_level'] == 'HIGH':
            priority -= 2
        elif risk_assessment['risk_level'] == 'MEDIUM':
            priority -= 1
        
        # Boost priority for zero orphaned records
        if recommendation.get('orphaned_records', 0) == 0:
            priority += 1
        
        return max(1, min(10, priority))
    
    def _check_index_requirement(self, recommendation: Dict[str, Any]) -> Dict[str, Any]:
        """Check if additional indexes are needed for the constraint."""
        return {
            'requires_index': True,  # FK columns should generally be indexed
            'index_name': f"IX_{recommendation['source_table']}_{recommendation['source_column']}",
            'index_type': 'NONCLUSTERED',
            'reasoning': 'Foreign key columns should be indexed for optimal JOIN performance'
        }
    
    def _estimate_performance_impact(self, recommendation: Dict[str, Any]) -> Dict[str, str]:
        """Estimate the performance impact of implementing the constraint."""
        return {
            'insert_impact': 'LOW',
            'update_impact': 'LOW',
            'delete_impact': 'MEDIUM',
            'query_impact': 'POSITIVE',
            'overall_assessment': 'Constraint will improve query performance with minimal DML overhead'
        }
    
    def _determine_implementation_order(self, constraint_plans: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Determine the optimal order for implementing constraints."""
        # Sort by priority (descending) and risk level (ascending)
        risk_order = {'LOW': 1, 'MEDIUM': 2, 'HIGH': 3}
        
        ordered_plans = sorted(
            constraint_plans,
            key=lambda x: (-x['implementation_priority'], risk_order.get(x['risk_assessment']['risk_level'], 2))
        )
        
        return [
            {
                'order': i + 1,
                'constraint_name': plan['constraint_name'],
                'priority': plan['implementation_priority'],
                'risk_level': plan['risk_assessment']['risk_level'],
                'estimated_duration': self._estimate_implementation_duration(plan)
            }
            for i, plan in enumerate(ordered_plans)
        ]
    
    def _estimate_implementation_duration(self, plan: Dict[str, Any]) -> str:
        """Estimate how long the constraint implementation will take."""
        if plan['risk_assessment']['risk_level'] == 'HIGH':
            return '30-60 minutes'
        elif plan['risk_assessment']['risk_level'] == 'MEDIUM':
            return '10-30 minutes'
        else:
            return '5-10 minutes'
    
    def _generate_ddl_scripts(self, constraint_plans: List[Dict[str, Any]]) -> List[str]:
        """Generate DDL scripts for constraint creation."""
        scripts = []
        
        for plan in constraint_plans:
            cascade_opts = plan['cascade_options']
            
            script = f"""
-- Create Foreign Key Constraint: {plan['constraint_name']}
-- Priority: {plan['implementation_priority']}, Risk: {plan['risk_assessment']['risk_level']}
-- Relationship: {plan['source_table']}.{plan['source_column']} -> {plan['target_table']}.{plan['target_column']}

-- Step 1: Create supporting index if needed
{self._generate_index_script(plan) if plan['requires_index'] else '-- Index already exists or not required'}

-- Step 2: Create the foreign key constraint
ALTER TABLE [{plan['source_table']}]
ADD CONSTRAINT [{plan['constraint_name']}]
FOREIGN KEY ([{plan['source_column']}])
REFERENCES [{plan['target_table']}] ([{plan['target_column']}])
ON DELETE {cascade_opts['on_delete']}
ON UPDATE {cascade_opts['on_update']};

-- Step 3: Verify constraint creation
SELECT 
    CONSTRAINT_NAME,
    TABLE_NAME,
    COLUMN_NAME,
    REFERENCED_TABLE_NAME,
    REFERENCED_COLUMN_NAME
FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
WHERE rc.CONSTRAINT_NAME = '{plan['constraint_name']}';

-- Cascade Options: {cascade_opts['reasoning']}
"""
            scripts.append(script.strip())
        
        return scripts
    
    def _generate_index_script(self, plan: Dict[str, Any]) -> str:
        """Generate index creation script if needed."""
        index_info = plan['requires_index']
        if isinstance(index_info, dict) and index_info.get('requires_index'):
            return f"""
CREATE {index_info['index_type']} INDEX [{index_info['index_name']}]
ON [{plan['source_table']}] ([{plan['source_column']}]);"""
        return ""
    
    def _generate_rollback_scripts(self, constraint_plans: List[Dict[str, Any]]) -> List[str]:
        """Generate rollback scripts for constraint removal."""
        scripts = []
        
        for plan in constraint_plans:
            script = f"""
-- Rollback Script for: {plan['constraint_name']}
-- WARNING: This will remove the foreign key constraint

-- Step 1: Drop the foreign key constraint
ALTER TABLE [{plan['source_table']}]
DROP CONSTRAINT [{plan['constraint_name']}];

-- Step 2: Optionally drop the supporting index
-- DROP INDEX [{plan['requires_index']['index_name']}] ON [{plan['source_table']}];

-- Step 3: Verify constraint removal
SELECT COUNT(*) as constraint_exists
FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
WHERE CONSTRAINT_NAME = '{plan['constraint_name']}';
-- Should return 0 if successfully removed
"""
            scripts.append(script.strip())
        
        return scripts
    
    def _generate_index_recommendations(self, constraint_plans: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate index recommendations for optimal FK performance."""
        recommendations = []
        
        for plan in constraint_plans:
            if plan['requires_index']:
                recommendations.append({
                    'table': plan['source_table'],
                    'column': plan['source_column'],
                    'index_name': plan['requires_index']['index_name'],
                    'index_type': plan['requires_index']['index_type'],
                    'purpose': 'Foreign Key Performance',
                    'priority': 'HIGH',
                    'reasoning': plan['requires_index']['reasoning']
                })
        
        return recommendations
    
    def _generate_implementation_summary(self, constraint_plans: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate implementation summary statistics."""
        if not constraint_plans:
            return {
                'total_constraints': 0,
                'high_priority': 0,
                'medium_priority': 0,
                'low_priority': 0,
                'low_risk': 0,
                'medium_risk': 0,
                'high_risk': 0
            }
        
        high_priority = sum(1 for p in constraint_plans if p['implementation_priority'] >= 7)
        medium_priority = sum(1 for p in constraint_plans if 4 <= p['implementation_priority'] < 7)
        low_priority = sum(1 for p in constraint_plans if p['implementation_priority'] < 4)
        
        low_risk = sum(1 for p in constraint_plans if p['risk_assessment']['risk_level'] == 'LOW')
        medium_risk = sum(1 for p in constraint_plans if p['risk_assessment']['risk_level'] == 'MEDIUM')
        high_risk = sum(1 for p in constraint_plans if p['risk_assessment']['risk_level'] == 'HIGH')
        
        return {
            'total_constraints': len(constraint_plans),
            'high_priority': high_priority,
            'medium_priority': medium_priority,
            'low_priority': low_priority,
            'low_risk': low_risk,
            'medium_risk': medium_risk,
            'high_risk': high_risk,
            'estimated_total_time': f"{len(constraint_plans) * 15}-{len(constraint_plans) * 45} minutes"
        }
