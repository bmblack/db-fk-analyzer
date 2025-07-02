# 🔗 AI-Powered Database Foreign Key Analyzer

An intelligent Streamlit application that uses AI agents to analyze SQL Server databases and provide comprehensive recommendations for foreign key relationships, data integrity improvements, and performance optimizations.

## 🌟 Features

### AI-Powered Analysis Agents
- **Schema Analysis Agent**: Detects missing foreign key relationships using pattern matching and naming conventions
- **Data Integrity Auditor**: Identifies orphaned records, constraint violations, and data quality issues
- **Constraint Recommendation Agent**: Generates safe DDL statements with appropriate cascading options
- **Query Performance Analyst**: Analyzes slow queries and recommends indexing optimizations
- **Change Impact Summarizer**: Provides executive-level risk assessment and implementation planning

### Key Capabilities
- ✅ **Automated FK Detection**: Uses AI to identify potential foreign key relationships
- ✅ **Data Quality Assessment**: Comprehensive audit of data integrity issues
- ✅ **Safe Implementation**: Generates tested DDL scripts with rollback procedures
- ✅ **Performance Optimization**: Identifies missing indexes and query bottlenecks
- ✅ **Risk Management**: Executive-level impact analysis and implementation planning
- ✅ **Interactive UI**: Modern Streamlit interface with real-time progress tracking

## 🏗️ Architecture

```
db-fk-analyzer/
├── agents/                     # AI agents for different analysis tasks
│   ├── schema_analysis_agent.py
│   ├── data_integrity_auditor.py
│   ├── constraint_recommendation_agent.py
│   ├── query_performance_analyst.py
│   └── change_impact_summarizer.py
├── utils/                      # Utility modules
│   ├── database.py            # Database connection and operations
│   └── logging_config.py      # Logging configuration
├── config/                     # Configuration files
│   ├── settings.env           # Environment variables
│   └── .env.example          # Example configuration
├── crew.py                    # CrewAI orchestration
├── main.py                    # Streamlit application
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- SQL Server with sample database (AdventureWorks recommended)
- OpenAI API key
- SQL Server ODBC Driver

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd db-fk-analyzer
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment**
   ```bash
   cp config/.env.example config/settings.env
   ```
   
   Edit `config/settings.env` with your settings:
   ```env
   # Database Configuration
   DB_CONNECTION_STRING=mssql+pyodbc:///?odbc_connect=DRIVER%3D%7BODBC+Driver+17+for+SQL+Server%7D%3BSERVER%3D127.0.0.1%2C1433%3BDATABASE%3DAdventureWorks2016-backup%3BUID%3Dsa%3BPWD%3Dyour_password%3BTrustServerCertificate%3Dyes%3BEncrypt%3Dno
   
   # AI Configuration
   OPENAI_API_KEY=your_openai_api_key_here
   OPENAI_MODEL=gpt-4
   
   # Logging
   LOG_LEVEL=INFO
   ```

4. **Run the application**
   ```bash
   streamlit run main.py
   ```

5. **Access the application**
   Open your browser to `http://localhost:8501`

## 📊 Usage Guide

### 1. Database Connection
- Click "Connect to Database" in the sidebar
- Verify connection status and database statistics
- Review any connection errors in the logs

### 2. Running Analysis
- **Full Analysis**: Click "🚀 Run All Agents" for comprehensive analysis
- **Individual Agents**: Use individual agent buttons for targeted analysis
- **Progress Tracking**: Monitor real-time progress and agent status

### 3. Reviewing Results
- **Summary Metrics**: High-level overview of findings
- **Agent Results**: Detailed results organized by agent type
- **Download Scripts**: Export SQL scripts for implementation
- **Executive Summary**: Business-level impact assessment

### 4. Implementation
- Review generated DDL scripts
- Follow phased implementation recommendations
- Use provided rollback scripts for safety
- Monitor performance impact

## 🤖 AI Agents Deep Dive

### Schema Analysis Agent
**Role**: Database Schema Detective  
**Goal**: Identify missing foreign key relationships

**Analysis Methods**:
- Column naming pattern matching (e.g., CustomerID, customer_id)
- Data type compatibility checking
- Referential pattern analysis
- Confidence scoring based on multiple factors

**Output**:
- List of potential FK relationships with confidence scores
- SQL statements for constraint creation
- Risk assessment for each recommendation

### Data Integrity Auditor
**Role**: Data Quality Inspector  
**Goal**: Find orphaned records and integrity violations

**Analysis Methods**:
- Orphaned record detection in potential FK relationships
- Existing constraint violation checking
- Duplicate record identification
- NULL value analysis in key columns

**Output**:
- Comprehensive data integrity report
- Prioritized remediation recommendations
- SQL cleanup scripts with safety warnings

### Constraint Recommendation Agent
**Role**: Database Architect  
**Goal**: Generate safe FK constraint implementations

**Analysis Methods**:
- Cascading option determination (CASCADE, SET NULL, RESTRICT)
- Implementation risk assessment
- Dependency analysis and ordering
- Performance impact estimation

**Output**:
- Detailed constraint implementation plans
- Complete DDL scripts with error handling
- Rollback procedures and testing strategies

### Query Performance Analyst
**Role**: Performance Detective  
**Goal**: Optimize FK-related query performance

**Analysis Methods**:
- Missing index identification on FK columns
- Slow query pattern analysis
- JOIN operation optimization
- Execution plan recommendations

**Output**:
- Performance bottleneck identification
- Index creation recommendations
- Query optimization suggestions

### Change Impact Summarizer
**Role**: Risk Analyst  
**Goal**: Assess overall impact of proposed changes

**Analysis Methods**:
- Cross-agent result synthesis
- Risk matrix generation
- Implementation timeline planning
- Resource requirement estimation

**Output**:
- Executive summary with go/no-go recommendation
- Phased implementation timeline
- Risk mitigation strategies

## 🔧 Configuration Options

### Database Settings
```env
# Connection string format for SQL Server
DB_CONNECTION_STRING=mssql+pyodbc:///?odbc_connect=...

# Connection timeout (seconds)
DB_TIMEOUT=30

# Query timeout (seconds)
QUERY_TIMEOUT=300
```

### AI Settings
```env
# OpenAI configuration
OPENAI_API_KEY=your_key_here
OPENAI_MODEL=gpt-4
OPENAI_TEMPERATURE=0.1

# CrewAI settings
CREW_VERBOSE=true
CREW_MEMORY=false
```

### Application Settings
```env
# Logging level
LOG_LEVEL=INFO

# Streamlit configuration
STREAMLIT_SERVER_PORT=8501
STREAMLIT_SERVER_ADDRESS=localhost
```

## 📈 Performance Considerations

### Database Impact
- **Read-Only Operations**: All analysis operations are read-only
- **Query Optimization**: Efficient queries with appropriate limits
- **Connection Pooling**: Managed database connections
- **Timeout Handling**: Configurable timeouts for long-running queries

### AI API Usage
- **Token Optimization**: Efficient prompt engineering
- **Rate Limiting**: Respectful API usage patterns
- **Error Handling**: Graceful degradation on API failures
- **Cost Management**: Configurable model selection

## 🛡️ Security Best Practices

### Database Security
- Use dedicated read-only database accounts
- Implement connection string encryption
- Enable SSL/TLS for database connections
- Regular credential rotation

### API Security
- Secure API key storage
- Environment variable usage
- No hardcoded credentials
- API key rotation policies

### Application Security
- Input validation and sanitization
- SQL injection prevention
- Secure logging practices
- Error message sanitization

## 🧪 Testing

### Unit Tests
```bash
# Run unit tests
python -m pytest tests/

# Run with coverage
python -m pytest tests/ --cov=.
```

### Integration Tests
```bash
# Test database connectivity
python -c "from utils.database import get_database_manager; print(get_database_manager().test_connection())"

# Test AI agent initialization
python -c "from crew import create_database_crew; from utils.database import get_database_manager; print('Agents initialized successfully')"
```

## 🚨 Troubleshooting

### Common Issues

**Database Connection Failures**
- Verify SQL Server is running
- Check connection string format
- Confirm ODBC driver installation
- Test network connectivity

**AI Agent Errors**
- Verify OpenAI API key validity
- Check API rate limits
- Review model availability
- Monitor token usage

**Performance Issues**
- Increase query timeouts
- Optimize database queries
- Monitor system resources
- Check network latency

### Debug Mode
Enable detailed logging:
```env
LOG_LEVEL=DEBUG
CREW_VERBOSE=true
```

## 📝 Contributing

### Development Setup
1. Fork the repository
2. Create a feature branch
3. Install development dependencies: `pip install -r requirements-dev.txt`
4. Make your changes
5. Run tests: `pytest`
6. Submit a pull request

### Code Standards
- Follow PEP 8 style guidelines
- Add type hints for all functions
- Include comprehensive docstrings
- Write unit tests for new features
- Update documentation as needed

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🤝 Support

### Getting Help
- 📖 Check this README for common solutions
- 🐛 Report bugs via GitHub Issues
- 💡 Request features via GitHub Issues
- 📧 Contact the development team

### Community
- ⭐ Star the repository if you find it useful
- 🍴 Fork and contribute improvements
- 📢 Share with your network
- 📝 Provide feedback and suggestions

## 🔮 Roadmap

### Upcoming Features
- [ ] Support for additional database systems (PostgreSQL, MySQL)
- [ ] Advanced visualization dashboards
- [ ] Automated constraint implementation
- [ ] Integration with CI/CD pipelines
- [ ] Machine learning model training on schema patterns
- [ ] Real-time monitoring and alerting
- [ ] Multi-database analysis and comparison

### Long-term Vision
- Comprehensive database health monitoring
- Predictive analytics for database optimization
- Integration with popular database management tools
- Enterprise-grade security and compliance features
- Cloud-native deployment options

---

**Built with ❤️ using CrewAI, Streamlit, and OpenAI**

*For questions, suggestions, or contributions, please reach out to the development team.*
