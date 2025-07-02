"""
Data Integrity Auditor Agent - Identifies orphaned records and referential integrity issues.
"""
import logging
from typing import Dict, Any, List
from crewai import Agent, Task
import pandas as pd

logger = logging.getLogger(__name__)

class DataIntegrityAuditor:
    """Agent responsible for auditing data integrity and finding orphaned records."""
    
    def __init__(self, database_manager):
        """Initialize the Data Integrity Auditor."""
        self.db_manager = database_manager
        self.agent = self._create_agent()
    
    def _create_agent(self) -> Agent:
        """Create the CrewAI agent."""
        return Agent(
            role="Data Quality Inspector",
            goal="Find orphaned records and referential integrity issues in the database",
            backstory="""You are a meticulous data quality expert with extensive experience 
            in identifying data inconsistencies and referential integrity violations. You have 
            a keen eye for spotting orphaned records, duplicate data, and constraint violations 
            that could impact database reliability and performance.""",
            verbose=True,
            allow_delegation=False
        )
    
    def create_audit_task(self) -> Task:
        """Create the data integrity audit task."""
        return Task(
            description="""Perform a comprehensive data integrity audit to identify:
            
            1. Orphaned records in child tables without corresponding parent records
            2. Referential integrity violations in existing foreign key constraints
            3. Duplicate records that could cause constraint conflicts
            4. Data type mismatches in related columns
            5. NULL values in columns that should have foreign key constraints
            
            For each issue found:
            - Quantify the scope of the problem
            - Assess the impact on data quality
            - Recommend remediation strategies
            - Prioritize fixes by severity and business impact""",
            agent=self.agent,
            expected_output="""A detailed data integrity report containing:
            - Summary of all integrity issues found
            - Detailed breakdown by table and relationship
            - Quantified impact assessment
            - Prioritized remediation recommendations
            - SQL scripts for data cleanup"""
        )
    
    def audit_data_integrity(self) -> Dict[str, Any]:
        """Perform comprehensive data integrity audit."""
        try:
            logger.info("Starting data integrity audit...")
            
            # Get existing foreign keys to audit
            existing_fks = self.db_manager.get_foreign_keys()
            logger.info(f"Auditing {len(existing_fks)} existing foreign key constraints")
            
            # Audit existing foreign key constraints
            fk_violations = self._audit_foreign_key_constraints(existing_fks)
            
            # Find orphaned records in potential relationships
            potential_relationships = self.db_manager.get_table_relationships()
            orphaned_records = self._find_orphaned_records(potential_relationships)
            
            # Check for duplicate records
            duplicate_issues = self._check_duplicate_records()
            
            # Analyze NULL values in key columns
            null_analysis = self._analyze_null_values()
            
            # Generate remediation recommendations
            recommendations = self._generate_remediation_recommendations(
                fk_violations, orphaned_records, duplicate_issues, null_analysis
            )
            
            result = {
                'status': 'success',
                'audit_summary': {
                    'foreign_key_violations': len(fk_violations),
                    'orphaned_record_issues': len(orphaned_records),
                    'duplicate_issues': len(duplicate_issues),
                    'null_value_issues': len(null_analysis)
                },
                'foreign_key_violations': fk_violations,
                'orphaned_records': orphaned_records,
                'duplicate_issues': duplicate_issues,
                'null_analysis': null_analysis,
                'recommendations': recommendations,
                'cleanup_scripts': self._generate_cleanup_scripts(recommendations)
            }
            
            logger.info("Data integrity audit completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Data integrity audit failed: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'audit_summary': {},
                'recommendations': []
            }
    
    def _audit_foreign_key_constraints(self, existing_fks: pd.DataFrame) -> List[Dict[str, Any]]:
        """Audit existing foreign key constraints for violations."""
        violations = []
        
        for _, fk in existing_fks.iterrows():
            try:
                # Check for orphaned records in this FK relationship
                orphaned_query = f"""
                SELECT COUNT(*) as violation_count
                FROM [{fk['parent_table']}] p
                LEFT JOIN [{fk['referenced_table']}] r ON p.[{fk['parent_column']}] = r.[{fk['referenced_column']}]
                WHERE p.[{fk['parent_column']}] IS NOT NULL 
                    AND r.[{fk['referenced_column']}] IS NULL
                """
                
                result = self.db_manager.execute_query(orphaned_query)
                violation_count = result['violation_count'].iloc[0] if not result.empty else 0
                
                if violation_count > 0:
                    violations.append({
                        'constraint_name': fk['constraint_name'],
                        'parent_table': fk['parent_table'],
                        'parent_column': fk['parent_column'],
                        'referenced_table': fk['referenced_table'],
                        'referenced_column': fk['referenced_column'],
                        'violation_count': violation_count,
                        'severity': self._assess_violation_severity(violation_count),
                        'impact': f"{violation_count} orphaned records violating FK constraint"
                    })
                
            except Exception as e:
                logger.warning(f"Could not audit FK constraint {fk['constraint_name']}: {e}")
        
        return violations
    
    def _find_orphaned_records(self, potential_relationships: pd.DataFrame) -> List[Dict[str, Any]]:
        """Find orphaned records in potential foreign key relationships."""
        orphaned_issues = []
        
        for _, rel in potential_relationships.iterrows():
            try:
                result = self.db_manager.get_orphaned_records(
                    rel['target_table'], rel['target_column'],
                    rel['source_table'], rel['source_column']
                )
                
                orphaned_count = result['orphaned_count'].iloc[0] if not result.empty else 0
                
                if orphaned_count > 0:
                    # Get sample orphaned records
                    sample_query = f"""
                    SELECT TOP 5 [{rel['source_column']}]
                    FROM [{rel['source_table']}] c
                    LEFT JOIN [{rel['target_table']}] p ON c.[{rel['source_column']}] = p.[{rel['target_column']}]
                    WHERE c.[{rel['source_column']}] IS NOT NULL 
                        AND p.[{rel['target_column']}] IS NULL
                    """
                    
                    sample_records = self.db_manager.execute_query(sample_query)
                    
                    orphaned_issues.append({
                        'source_table': rel['source_table'],
                        'source_column': rel['source_column'],
                        'target_table': rel['target_table'],
                        'target_column': rel['target_column'],
                        'orphaned_count': orphaned_count,
                        'match_type': rel['match_type'],
                        'severity': self._assess_violation_severity(orphaned_count),
                        'sample_values': sample_records[rel['source_column']].tolist() if not sample_records.empty else [],
                        'impact': f"{orphaned_count} records in {rel['source_table']} reference non-existent {rel['target_table']} records"
                    })
                    
            except Exception as e:
                logger.warning(f"Could not check orphaned records for {rel['source_table']}.{rel['source_column']}: {e}")
        
        return orphaned_issues
    
    def _check_duplicate_records(self) -> List[Dict[str, Any]]:
        """Check for duplicate records that could cause constraint issues."""
        duplicate_issues = []
        
        try:
            # Get all tables
            tables = self.db_manager.get_table_list()
            
            for table in tables[:10]:  # Limit to first 10 tables for performance
                try:
                    # Check for duplicate primary key candidates
                    schema = self.db_manager.get_table_schema(table)
                    id_columns = [col for col in schema['COLUMN_NAME'] if 'id' in col.lower()]
                    
                    for col in id_columns[:3]:  # Check first 3 ID columns
                        duplicate_query = f"""
                        SELECT [{col}], COUNT(*) as duplicate_count
                        FROM [{table}]
                        WHERE [{col}] IS NOT NULL
                        GROUP BY [{col}]
                        HAVING COUNT(*) > 1
                        """
                        
                        duplicates = self.db_manager.execute_query(duplicate_query)
                        
                        if not duplicates.empty:
                            total_duplicates = duplicates['duplicate_count'].sum() - len(duplicates)
                            
                            duplicate_issues.append({
                                'table': table,
                                'column': col,
                                'duplicate_groups': len(duplicates),
                                'total_duplicate_records': total_duplicates,
                                'severity': self._assess_violation_severity(total_duplicates),
                                'impact': f"{total_duplicates} duplicate values in {table}.{col} could prevent unique constraints"
                            })
                            
                except Exception as e:
                    logger.warning(f"Could not check duplicates in table {table}: {e}")
                    
        except Exception as e:
            logger.warning(f"Could not perform duplicate check: {e}")
        
        return duplicate_issues
    
    def _analyze_null_values(self) -> List[Dict[str, Any]]:
        """Analyze NULL values in columns that should likely have foreign key constraints."""
        null_issues = []
        
        try:
            # Get potential relationships
            potential_relationships = self.db_manager.get_table_relationships()
            
            for _, rel in potential_relationships.iterrows():
                try:
                    null_query = f"""
                    SELECT COUNT(*) as null_count,
                           (SELECT COUNT(*) FROM [{rel['source_table']}]) as total_count
                    FROM [{rel['source_table']}]
                    WHERE [{rel['source_column']}] IS NULL
                    """
                    
                    result = self.db_manager.execute_query(null_query)
                    
                    if not result.empty:
                        null_count = result['null_count'].iloc[0]
                        total_count = result['total_count'].iloc[0]
                        null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
                        
                        if null_count > 0 and null_percentage > 5:  # More than 5% NULL values
                            null_issues.append({
                                'table': rel['source_table'],
                                'column': rel['source_column'],
                                'null_count': null_count,
                                'total_count': total_count,
                                'null_percentage': round(null_percentage, 2),
                                'severity': 'HIGH' if null_percentage > 20 else 'MEDIUM',
                                'impact': f"{null_percentage:.1f}% NULL values in potential FK column {rel['source_table']}.{rel['source_column']}"
                            })
                            
                except Exception as e:
                    logger.warning(f"Could not analyze NULL values for {rel['source_table']}.{rel['source_column']}: {e}")
                    
        except Exception as e:
            logger.warning(f"Could not perform NULL analysis: {e}")
        
        return null_issues
    
    def _assess_violation_severity(self, count: int) -> str:
        """Assess severity based on violation count."""
        if count == 0:
            return 'NONE'
        elif count <= 10:
            return 'LOW'
        elif count <= 100:
            return 'MEDIUM'
        else:
            return 'HIGH'
    
    def _generate_remediation_recommendations(self, fk_violations: List[Dict], 
                                            orphaned_records: List[Dict],
                                            duplicate_issues: List[Dict],
                                            null_analysis: List[Dict]) -> List[Dict[str, Any]]:
        """Generate prioritized remediation recommendations."""
        recommendations = []
        
        # FK violation recommendations
        for violation in fk_violations:
            recommendations.append({
                'type': 'FK_VIOLATION',
                'priority': 'HIGH',
                'table': violation['parent_table'],
                'issue': f"Foreign key constraint violation: {violation['violation_count']} orphaned records",
                'recommendation': f"Clean up {violation['violation_count']} orphaned records in {violation['parent_table']}",
                'action': 'DELETE_ORPHANED_RECORDS',
                'details': violation
            })
        
        # Orphaned records recommendations
        for orphaned in orphaned_records:
            if orphaned['severity'] in ['HIGH', 'MEDIUM']:
                recommendations.append({
                    'type': 'ORPHANED_RECORDS',
                    'priority': orphaned['severity'],
                    'table': orphaned['source_table'],
                    'issue': f"{orphaned['orphaned_count']} orphaned records prevent FK creation",
                    'recommendation': f"Clean up orphaned records or create missing parent records",
                    'action': 'CLEANUP_ORPHANED_DATA',
                    'details': orphaned
                })
        
        # Duplicate issues recommendations
        for duplicate in duplicate_issues:
            if duplicate['severity'] in ['HIGH', 'MEDIUM']:
                recommendations.append({
                    'type': 'DUPLICATE_RECORDS',
                    'priority': duplicate['severity'],
                    'table': duplicate['table'],
                    'issue': f"{duplicate['total_duplicate_records']} duplicate records in key column",
                    'recommendation': f"Remove or merge duplicate records in {duplicate['table']}.{duplicate['column']}",
                    'action': 'RESOLVE_DUPLICATES',
                    'details': duplicate
                })
        
        # NULL value recommendations
        for null_issue in null_analysis:
            if null_issue['severity'] == 'HIGH':
                recommendations.append({
                    'type': 'NULL_VALUES',
                    'priority': 'MEDIUM',
                    'table': null_issue['table'],
                    'issue': f"{null_issue['null_percentage']}% NULL values in potential FK column",
                    'recommendation': f"Review business rules for NULL values in {null_issue['table']}.{null_issue['column']}",
                    'action': 'REVIEW_NULL_POLICY',
                    'details': null_issue
                })
        
        # Sort by priority
        priority_order = {'HIGH': 3, 'MEDIUM': 2, 'LOW': 1}
        recommendations.sort(key=lambda x: priority_order.get(x['priority'], 0), reverse=True)
        
        return recommendations
    
    def _generate_cleanup_scripts(self, recommendations: List[Dict[str, Any]]) -> List[str]:
        """Generate SQL cleanup scripts for remediation."""
        scripts = []
        
        for rec in recommendations:
            if rec['action'] == 'DELETE_ORPHANED_RECORDS' and rec['type'] == 'FK_VIOLATION':
                details = rec['details']
                script = f"""
-- Clean up orphaned records violating FK constraint: {details['constraint_name']}
-- WARNING: This will delete {details['violation_count']} records from {details['parent_table']}
-- Review and backup data before executing!

DELETE p
FROM [{details['parent_table']}] p
LEFT JOIN [{details['referenced_table']}] r ON p.[{details['parent_column']}] = r.[{details['referenced_column']}]
WHERE p.[{details['parent_column']}] IS NOT NULL 
    AND r.[{details['referenced_column']}] IS NULL;

-- Verify cleanup
SELECT COUNT(*) as remaining_violations
FROM [{details['parent_table']}] p
LEFT JOIN [{details['referenced_table']}] r ON p.[{details['parent_column']}] = r.[{details['referenced_column']}]
WHERE p.[{details['parent_column']}] IS NOT NULL 
    AND r.[{details['referenced_column']}] IS NULL;
"""
                scripts.append(script.strip())
            
            elif rec['action'] == 'CLEANUP_ORPHANED_DATA':
                details = rec['details']
                script = f"""
-- Clean up orphaned records in {details['source_table']}.{details['source_column']}
-- WARNING: This will delete {details['orphaned_count']} records
-- Review and backup data before executing!

-- Option 1: Delete orphaned records
DELETE c
FROM [{details['source_table']}] c
LEFT JOIN [{details['target_table']}] p ON c.[{details['source_column']}] = p.[{details['target_column']}]
WHERE c.[{details['source_column']}] IS NOT NULL 
    AND p.[{details['target_column']}] IS NULL;

-- Option 2: Set orphaned values to NULL (if business rules allow)
-- UPDATE [{details['source_table']}]
-- SET [{details['source_column']}] = NULL
-- WHERE [{details['source_column']}] NOT IN (SELECT [{details['target_column']}] FROM [{details['target_table']}] WHERE [{details['target_column']}] IS NOT NULL);
"""
                scripts.append(script.strip())
        
        return scripts
