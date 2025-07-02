"""
Query Performance Analyst Agent - Analyzes slow queries related to FK operations.
"""
import logging
from typing import Dict, Any, List
from crewai import Agent, Task
import pandas as pd

logger = logging.getLogger(__name__)

class QueryPerformanceAnalyst:
    """Agent responsible for analyzing query performance related to foreign key operations."""
    
    def __init__(self, database_manager):
        """Initialize the Query Performance Analyst."""
        self.db_manager = database_manager
        self.agent = self._create_agent()
    
    def _create_agent(self) -> Agent:
        """Create the CrewAI agent."""
        return Agent(
            role="Performance Detective",
            goal="Identify slow queries related to foreign key operations and JOIN performance",
            backstory="""You are a database performance expert with deep knowledge of query 
            optimization, execution plans, and indexing strategies. You specialize in identifying 
            performance bottlenecks in JOIN operations and foreign key lookups, and you excel at 
            recommending targeted optimizations.""",
            verbose=True,
            allow_delegation=False
        )
    
    def create_analysis_task(self) -> Task:
        """Create the query performance analysis task."""
        return Task(
            description="""Analyze query performance issues related to foreign key operations:
            
            1. Identify slow JOIN operations that could benefit from FK constraints
            2. Analyze execution plans for FK-related queries
            3. Find missing indexes on foreign key columns
            4. Detect inefficient query patterns in FK lookups
            5. Recommend query optimizations and indexing strategies
            
            Focus on:
            - JOIN performance bottlenecks
            - Missing indexes on FK columns
            - Inefficient WHERE clauses on FK columns
            - Suboptimal query patterns
            - Opportunities for query rewriting""",
            agent=self.agent,
            expected_output="""A performance analysis report containing:
            - List of slow queries with FK-related performance issues
            - Execution plan analysis and bottleneck identification
            - Index recommendations for FK columns
            - Query optimization suggestions
            - Performance impact estimates"""
        )
    
    def analyze_query_performance(self) -> Dict[str, Any]:
        """Analyze query performance related to foreign key operations."""
        try:
            logger.info("Starting query performance analysis...")
            
            # Analyze potential FK columns for missing indexes
            missing_indexes = self._analyze_missing_fk_indexes()
            
            # Generate sample performance queries
            performance_queries = self._generate_performance_test_queries()
            
            # Analyze common query patterns
            query_patterns = self._analyze_query_patterns()
            
            # Generate optimization recommendations
            optimizations = self._generate_optimization_recommendations(
                missing_indexes, performance_queries, query_patterns
            )
            
            result = {
                'status': 'success',
                'analysis_summary': {
                    'missing_indexes_found': len(missing_indexes),
                    'performance_queries_analyzed': len(performance_queries),
                    'optimization_opportunities': len(optimizations)
                },
                'missing_indexes': missing_indexes,
                'performance_queries': performance_queries,
                'query_patterns': query_patterns,
                'optimizations': optimizations,
                'recommendations': self._generate_performance_recommendations(optimizations)
            }
            
            logger.info("Query performance analysis completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Query performance analysis failed: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'missing_indexes': [],
                'optimizations': []
            }
    
    def _analyze_missing_fk_indexes(self) -> List[Dict[str, Any]]:
        """Analyze potential foreign key columns for missing indexes."""
        missing_indexes = []
        
        try:
            # Get potential FK relationships
            potential_relationships = self.db_manager.get_table_relationships()
            
            for _, rel in potential_relationships.iterrows():
                # Check if index exists on the FK column
                index_check_query = f"""
                SELECT COUNT(*) as index_count
                FROM sys.indexes i
                INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                INNER JOIN sys.tables t ON i.object_id = t.object_id
                WHERE t.name = '{rel['source_table']}'
                    AND c.name = '{rel['source_column']}'
                    AND ic.key_ordinal > 0
                """
                
                result = self.db_manager.execute_query(index_check_query)
                index_count = result['index_count'].iloc[0] if not result.empty else 0
                
                if index_count == 0:
                    # Estimate table size for impact assessment
                    size_query = f"SELECT COUNT(*) as row_count FROM [{rel['source_table']}]"
                    size_result = self.db_manager.execute_query(size_query)
                    row_count = size_result['row_count'].iloc[0] if not size_result.empty else 0
                    
                    missing_indexes.append({
                        'table': rel['source_table'],
                        'column': rel['source_column'],
                        'target_table': rel['target_table'],
                        'target_column': rel['target_column'],
                        'match_type': rel['match_type'],
                        'estimated_rows': row_count,
                        'performance_impact': self._assess_index_impact(row_count),
                        'recommended_index': f"IX_{rel['source_table']}_{rel['source_column']}",
                        'index_script': self._generate_index_script(rel['source_table'], rel['source_column'])
                    })
                    
        except Exception as e:
            logger.warning(f"Could not analyze missing FK indexes: {e}")
        
        return missing_indexes
    
    def _assess_index_impact(self, row_count: int) -> str:
        """Assess the performance impact of missing index based on table size."""
        if row_count > 100000:
            return 'HIGH'
        elif row_count > 10000:
            return 'MEDIUM'
        elif row_count > 1000:
            return 'LOW'
        else:
            return 'MINIMAL'
    
    def _generate_index_script(self, table: str, column: str) -> str:
        """Generate index creation script."""
        index_name = f"IX_{table}_{column}"
        return f"CREATE NONCLUSTERED INDEX [{index_name}] ON [{table}] ([{column}]);"
    
    def _generate_performance_test_queries(self) -> List[Dict[str, Any]]:
        """Generate sample queries to test FK performance."""
        test_queries = []
        
        try:
            # Get potential relationships for testing
            potential_relationships = self.db_manager.get_table_relationships()
            
            for _, rel in potential_relationships.iterrows():
                # Generate different types of test queries
                queries = [
                    {
                        'query_type': 'INNER_JOIN',
                        'description': f"Inner join between {rel['source_table']} and {rel['target_table']}",
                        'sql': f"""
SELECT s.*, t.*
FROM [{rel['source_table']}] s
INNER JOIN [{rel['target_table']}] t ON s.[{rel['source_column']}] = t.[{rel['target_column']}]
""",
                        'performance_concern': 'JOIN without proper indexing may cause table scans'
                    },
                    {
                        'query_type': 'EXISTS_CHECK',
                        'description': f"Check existence in {rel['target_table']}",
                        'sql': f"""
SELECT *
FROM [{rel['source_table']}] s
WHERE EXISTS (
    SELECT 1 FROM [{rel['target_table']}] t 
    WHERE t.[{rel['target_column']}] = s.[{rel['source_column']}]
)
""",
                        'performance_concern': 'EXISTS subquery may be inefficient without proper indexing'
                    },
                    {
                        'query_type': 'COUNT_AGGREGATION',
                        'description': f"Count related records in {rel['source_table']}",
                        'sql': f"""
SELECT t.[{rel['target_column']}], COUNT(*) as related_count
FROM [{rel['target_table']}] t
LEFT JOIN [{rel['source_table']}] s ON t.[{rel['target_column']}] = s.[{rel['source_column']}]
GROUP BY t.[{rel['target_column']}]
""",
                        'performance_concern': 'Aggregation with JOIN may be slow without proper indexing'
                    }
                ]
                
                for query in queries:
                    query.update({
                        'source_table': rel['source_table'],
                        'source_column': rel['source_column'],
                        'target_table': rel['target_table'],
                        'target_column': rel['target_column'],
                        'optimization_potential': 'HIGH' if rel['match_type'] == 'EXACT_MATCH' else 'MEDIUM'
                    })
                    
                test_queries.append(query)
                
        except Exception as e:
            logger.warning(f"Could not generate performance test queries: {e}")
        
        return test_queries[:20]  # Limit to first 20 for performance
    
    def _analyze_query_patterns(self) -> List[Dict[str, Any]]:
        """Analyze common query patterns that could benefit from FK optimization."""
        patterns = [
            {
                'pattern_name': 'Frequent JOIN Operations',
                'description': 'Tables frequently joined together should have FK constraints',
                'detection_method': 'Analyze potential relationships with high confidence scores',
                'optimization': 'Create FK constraints and supporting indexes',
                'benefit': 'Improved JOIN performance and query plan optimization'
            },
            {
                'pattern_name': 'Lookup Table Access',
                'description': 'Frequent lookups to reference/lookup tables',
                'detection_method': 'Identify tables with "lookup", "reference", or "type" in name',
                'optimization': 'Ensure FK constraints exist and are properly indexed',
                'benefit': 'Faster reference data lookups'
            },
            {
                'pattern_name': 'Orphaned Record Queries',
                'description': 'Queries checking for orphaned records',
                'detection_method': 'LEFT JOIN with IS NULL conditions',
                'optimization': 'Implement FK constraints to prevent orphaned records',
                'benefit': 'Eliminate need for orphaned record checks'
            },
            {
                'pattern_name': 'Cascading Updates',
                'description': 'Manual cascading updates across related tables',
                'detection_method': 'Multiple UPDATE statements on related tables',
                'optimization': 'Use FK constraints with CASCADE options',
                'benefit': 'Automatic cascading and improved data consistency'
            }
        ]
        
        return patterns
    
    def _generate_optimization_recommendations(self, missing_indexes: List[Dict], 
                                             performance_queries: List[Dict],
                                             query_patterns: List[Dict]) -> List[Dict[str, Any]]:
        """Generate specific optimization recommendations."""
        optimizations = []
        
        # Index-based optimizations
        for idx in missing_indexes:
            if idx['performance_impact'] in ['HIGH', 'MEDIUM']:
                optimizations.append({
                    'type': 'INDEX_CREATION',
                    'priority': idx['performance_impact'],
                    'table': idx['table'],
                    'recommendation': f"Create index on {idx['table']}.{idx['column']} for FK performance",
                    'implementation': idx['index_script'],
                    'expected_benefit': f"Improve JOIN performance for {idx['estimated_rows']:,} rows",
                    'estimated_improvement': self._estimate_performance_improvement(idx['performance_impact'])
                })
        
        # Query pattern optimizations
        high_confidence_queries = [q for q in performance_queries if q['optimization_potential'] == 'HIGH']
        for query in high_confidence_queries[:5]:  # Top 5 high-confidence queries
            optimizations.append({
                'type': 'QUERY_OPTIMIZATION',
                'priority': 'MEDIUM',
                'table': query['source_table'],
                'recommendation': f"Optimize {query['query_type']} pattern between {query['source_table']} and {query['target_table']}",
                'implementation': f"Create FK constraint and index for optimal {query['query_type']} performance",
                'expected_benefit': query['performance_concern'],
                'estimated_improvement': '20-50% faster query execution'
            })
        
        return optimizations
    
    def _estimate_performance_improvement(self, impact_level: str) -> str:
        """Estimate performance improvement based on impact level."""
        improvements = {
            'HIGH': '50-80% faster JOIN operations',
            'MEDIUM': '20-50% faster JOIN operations',
            'LOW': '10-20% faster JOIN operations',
            'MINIMAL': '5-10% faster JOIN operations'
        }
        return improvements.get(impact_level, '10-30% faster operations')
    
    def _generate_performance_recommendations(self, optimizations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate high-level performance recommendations."""
        recommendations = []
        
        # Group optimizations by type
        index_optimizations = [opt for opt in optimizations if opt['type'] == 'INDEX_CREATION']
        query_optimizations = [opt for opt in optimizations if opt['type'] == 'QUERY_OPTIMIZATION']
        
        if index_optimizations:
            high_impact_indexes = [opt for opt in index_optimizations if opt['priority'] == 'HIGH']
            recommendations.append({
                'category': 'INDEX_OPTIMIZATION',
                'priority': 'HIGH',
                'title': 'Create Missing Foreign Key Indexes',
                'description': f"Create {len(index_optimizations)} missing indexes on FK columns",
                'impact': f"{len(high_impact_indexes)} high-impact indexes identified",
                'action_items': [
                    f"Create index on {opt['table']}" for opt in high_impact_indexes[:5]
                ]
            })
        
        if query_optimizations:
            recommendations.append({
                'category': 'QUERY_OPTIMIZATION',
                'priority': 'MEDIUM',
                'title': 'Optimize JOIN Query Patterns',
                'description': f"Optimize {len(query_optimizations)} query patterns for better performance",
                'impact': 'Improved query execution times and reduced resource usage',
                'action_items': [
                    'Implement FK constraints for frequently joined tables',
                    'Review and optimize JOIN query patterns',
                    'Consider query rewriting for complex patterns'
                ]
            })
        
        # General recommendations
        recommendations.append({
            'category': 'MONITORING',
            'priority': 'LOW',
            'title': 'Implement Performance Monitoring',
            'description': 'Set up monitoring for FK-related query performance',
            'impact': 'Proactive identification of performance issues',
            'action_items': [
                'Monitor slow queries involving JOINs',
                'Track index usage statistics',
                'Set up alerts for performance degradation'
            ]
        })
        
        return recommendations
