"""
Schema Analysis Agent - Detects missing foreign key relationships.
"""
import logging
from typing import Dict, Any, List
from crewai import Agent, Task
import pandas as pd

logger = logging.getLogger(__name__)

class SchemaAnalysisAgent:
    """Agent responsible for analyzing database schema and detecting missing foreign keys."""
    
    def __init__(self, database_manager):
        """Initialize the Schema Analysis Agent."""
        self.db_manager = database_manager
        self.agent = self._create_agent()
    
    def _create_agent(self) -> Agent:
        """Create the CrewAI agent."""
        return Agent(
            role="Database Schema Detective",
            goal="Identify missing foreign key relationships in the database schema",
            backstory="""You are an expert database analyst with years of experience in 
            identifying data relationships and schema optimization. You excel at detecting 
            patterns in table structures and column naming conventions that suggest 
            foreign key relationships.""",
            verbose=True,
            allow_delegation=False
        )
    
    def create_analysis_task(self) -> Task:
        """Create the schema analysis task."""
        return Task(
            description="""Analyze the database schema to identify missing foreign key relationships.
            
            Your analysis should:
            1. Examine all tables and their column structures
            2. Identify potential foreign key relationships based on:
               - Column naming patterns (e.g., CustomerID, customer_id)
               - Data type compatibility
               - Referential patterns
            3. Compare with existing foreign key constraints
            4. Prioritize recommendations by confidence level
            5. Provide detailed reasoning for each recommendation
            
            Focus on finding relationships that would improve data integrity and query performance.""",
            agent=self.agent,
            expected_output="""A comprehensive report containing:
            - List of potential missing foreign keys with confidence scores
            - Detailed analysis of each recommendation
            - SQL statements to create the foreign keys
            - Risk assessment for each proposed constraint"""
        )
    
    def analyze_schema(self) -> Dict[str, Any]:
        """Perform schema analysis to detect missing foreign keys."""
        try:
            logger.info("Starting schema analysis...")
            
            # Get current foreign keys
            existing_fks = self.db_manager.get_foreign_keys()
            logger.info(f"Found {len(existing_fks)} existing foreign keys")
            
            # Get potential relationships
            potential_relationships = self.db_manager.get_table_relationships()
            logger.info(f"Found {len(potential_relationships)} potential relationships")
            
            # Filter out existing relationships
            missing_fks = self._find_missing_foreign_keys(existing_fks, potential_relationships)
            
            # Analyze and score recommendations
            recommendations = self._analyze_recommendations(missing_fks)
            
            # Generate SQL statements
            sql_statements = self._generate_fk_sql(recommendations)
            
            result = {
                'status': 'success',
                'existing_foreign_keys': len(existing_fks),
                'potential_relationships': len(potential_relationships),
                'missing_foreign_keys': len(missing_fks),
                'recommendations': recommendations,
                'sql_statements': sql_statements,
                'summary': self._generate_summary(recommendations)
            }
            
            logger.info("Schema analysis completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Schema analysis failed: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'recommendations': [],
                'sql_statements': []
            }
    
    def _find_missing_foreign_keys(self, existing_fks: pd.DataFrame, 
                                 potential_relationships: pd.DataFrame) -> pd.DataFrame:
        """Find relationships that don't have foreign key constraints."""
        if existing_fks.empty:
            return potential_relationships
        
        # Create a set of existing FK relationships for comparison
        existing_relationships = set()
        for _, row in existing_fks.iterrows():
            existing_relationships.add((
                row['parent_table'], 
                row['parent_column'], 
                row['referenced_table'], 
                row['referenced_column']
            ))
        
        # Filter out existing relationships
        missing = []
        for _, row in potential_relationships.iterrows():
            relationship = (
                row['source_table'], 
                row['source_column'], 
                row['target_table'], 
                row['target_column']
            )
            if relationship not in existing_relationships:
                missing.append(row)
        
        return pd.DataFrame(missing)
    
    def _analyze_recommendations(self, missing_fks: pd.DataFrame) -> List[Dict[str, Any]]:
        """Analyze and score foreign key recommendations."""
        recommendations = []
        
        for _, row in missing_fks.iterrows():
            # Calculate confidence score based on match type and naming patterns
            confidence = self._calculate_confidence_score(row)
            
            # Check for potential data integrity issues
            orphaned_count = self._check_orphaned_records(
                row['target_table'], row['target_column'],
                row['source_table'], row['source_column']
            )
            
            recommendation = {
                'source_table': row['source_table'],
                'source_column': row['source_column'],
                'target_table': row['target_table'],
                'target_column': row['target_column'],
                'match_type': row['match_type'],
                'confidence_score': confidence,
                'orphaned_records': orphaned_count,
                'risk_level': self._assess_risk_level(confidence, orphaned_count),
                'reasoning': self._generate_reasoning(row, confidence, orphaned_count)
            }
            
            recommendations.append(recommendation)
        
        # Sort by confidence score (descending)
        recommendations.sort(key=lambda x: x['confidence_score'], reverse=True)
        
        return recommendations
    
    def _calculate_confidence_score(self, row: pd.Series) -> float:
        """Calculate confidence score for a potential foreign key relationship."""
        base_score = 0.5
        
        # Boost score based on match type
        if row['match_type'] == 'EXACT_MATCH':
            base_score += 0.4
        elif row['match_type'] == 'TABLE_NAME_PATTERN':
            base_score += 0.3
        elif row['match_type'] == 'ID_PATTERN':
            base_score += 0.2
        
        # Additional scoring based on naming conventions
        source_col = row['source_column'].lower()
        target_col = row['target_column'].lower()
        
        if source_col.endswith('id') and target_col.endswith('id'):
            base_score += 0.1
        
        if 'id' in source_col and 'id' in target_col:
            base_score += 0.05
        
        return min(base_score, 1.0)
    
    def _check_orphaned_records(self, parent_table: str, parent_column: str,
                              child_table: str, child_column: str) -> int:
        """Check for orphaned records that would prevent FK creation."""
        try:
            result = self.db_manager.get_orphaned_records(
                parent_table, parent_column, child_table, child_column
            )
            return result['orphaned_count'].iloc[0] if not result.empty else 0
        except Exception as e:
            logger.warning(f"Could not check orphaned records: {e}")
            return -1  # Unknown
    
    def _assess_risk_level(self, confidence: float, orphaned_count: int) -> str:
        """Assess risk level for implementing the foreign key."""
        if orphaned_count > 0:
            return 'HIGH'
        elif confidence >= 0.8:
            return 'LOW'
        elif confidence >= 0.6:
            return 'MEDIUM'
        else:
            return 'HIGH'
    
    def _generate_reasoning(self, row: pd.Series, confidence: float, orphaned_count: int) -> str:
        """Generate human-readable reasoning for the recommendation."""
        reasoning = []
        
        # Match type reasoning
        if row['match_type'] == 'EXACT_MATCH':
            reasoning.append(f"Column names match exactly ({row['source_column']} = {row['target_column']})")
        elif row['match_type'] == 'TABLE_NAME_PATTERN':
            reasoning.append(f"Column {row['source_column']} follows naming pattern for table {row['target_table']}")
        
        # Confidence reasoning
        if confidence >= 0.8:
            reasoning.append("High confidence based on naming conventions")
        elif confidence >= 0.6:
            reasoning.append("Medium confidence based on column patterns")
        else:
            reasoning.append("Low confidence - manual review recommended")
        
        # Data integrity reasoning
        if orphaned_count == 0:
            reasoning.append("No orphaned records detected - safe to implement")
        elif orphaned_count > 0:
            reasoning.append(f"WARNING: {orphaned_count} orphaned records found - data cleanup required")
        
        return "; ".join(reasoning)
    
    def _generate_fk_sql(self, recommendations: List[Dict[str, Any]]) -> List[str]:
        """Generate SQL statements to create foreign keys."""
        sql_statements = []
        
        for rec in recommendations:
            if rec['risk_level'] != 'HIGH' or rec['orphaned_records'] == 0:
                constraint_name = f"FK_{rec['source_table']}_{rec['source_column']}"
                sql = f"""
-- Foreign Key: {rec['source_table']}.{rec['source_column']} -> {rec['target_table']}.{rec['target_column']}
-- Confidence: {rec['confidence_score']:.2f}, Risk: {rec['risk_level']}
-- Reasoning: {rec['reasoning']}
ALTER TABLE [{rec['source_table']}]
ADD CONSTRAINT [{constraint_name}]
FOREIGN KEY ([{rec['source_column']}])
REFERENCES [{rec['target_table']}] ([{rec['target_column']}]);
"""
                sql_statements.append(sql.strip())
        
        return sql_statements
    
    def _generate_summary(self, recommendations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate summary statistics."""
        if not recommendations:
            return {
                'total_recommendations': 0,
                'high_confidence': 0,
                'medium_confidence': 0,
                'low_confidence': 0,
                'safe_to_implement': 0,
                'requires_cleanup': 0
            }
        
        high_conf = sum(1 for r in recommendations if r['confidence_score'] >= 0.8)
        medium_conf = sum(1 for r in recommendations if 0.6 <= r['confidence_score'] < 0.8)
        low_conf = sum(1 for r in recommendations if r['confidence_score'] < 0.6)
        
        safe = sum(1 for r in recommendations if r['risk_level'] == 'LOW')
        cleanup = sum(1 for r in recommendations if r['orphaned_records'] > 0)
        
        return {
            'total_recommendations': len(recommendations),
            'high_confidence': high_conf,
            'medium_confidence': medium_conf,
            'low_confidence': low_conf,
            'safe_to_implement': safe,
            'requires_cleanup': cleanup
        }
