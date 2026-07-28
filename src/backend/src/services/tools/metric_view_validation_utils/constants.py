"""Constants for Metric Expression Validator."""

# DAX and SQL reserved keywords (control-flow, operators, clauses)
SQL_KEYWORDS = {
    'SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'STDDEV',
    'WHERE', 'FILTER', 'SELECT', 'FROM', 'IN', 'AS', 'AND', 'OR',
    'NOT', 'NULL', 'NULLIF', 'COALESCE', 'TRUE', 'FALSE',
    'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'BETWEEN', 'LIKE',
    'IS', 'WITH', 'ON', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER',
    'DIVIDE', 'CAST', 'OVER', 'PARTITION', 'BY', 'ORDER', 'GROUP',
    'BLANK', 'DISTINCT', 'HAVING', 'LIMIT', 'OFFSET', 'UNION',
    'EXCEPT', 'INTERSECT', 'EXISTS', 'ALL', 'ANY', 'SOME',
    'ASC', 'DESC', 'CROSS', 'FULL', 'USING', 'SET',
}

# Well-known SQL / Spark scalar and date/time functions that should not be
# mistaken for column references when they appear as bare identifiers.
SQL_FUNCTIONS = {
    # String functions
    'UPPER', 'LOWER', 'TRIM', 'LTRIM', 'RTRIM', 'LENGTH', 'LEN',
    'SUBSTR', 'SUBSTRING', 'REPLACE', 'CONCAT', 'CONCAT_WS', 'SPLIT',
    'LPAD', 'RPAD', 'REVERSE', 'INITCAP', 'INSTR', 'LOCATE',
    'SPACE', 'REPEAT', 'TRANSLATE', 'FORMAT',
    # Date/time functions
    'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND', 'QUARTER',
    'WEEKOFYEAR', 'DAYOFWEEK', 'DAYOFYEAR', 'DATE', 'NOW', 'TODAY',
    'CURRENT_DATE', 'CURRENT_TIMESTAMP', 'CURRENT_TIME',
    'DATE_FORMAT', 'DATE_TRUNC', 'DATE_ADD', 'DATE_SUB', 'DATEDIFF',
    'TO_DATE', 'TO_TIMESTAMP', 'FROM_UNIXTIME', 'UNIX_TIMESTAMP',
    'LAST_DAY', 'NEXT_DAY', 'MONTHS_BETWEEN', 'ADD_MONTHS',
    'EXTRACT', 'TRUNC',
    # Math functions
    'ABS', 'CEIL', 'CEILING', 'FLOOR', 'ROUND', 'TRUNCATE',
    'SQRT', 'POW', 'POWER', 'EXP', 'LN', 'LOG', 'LOG10', 'LOG2',
    'MOD', 'SIGN', 'RAND', 'RANDOM',
    # Conditional / type functions
    'IF', 'IIF', 'IFS', 'IFNULL', 'NVL', 'NVL2', 'ISNULL',
    'ISNUMERIC', 'ISDATE', 'TRY_CAST', 'TRY_CONVERT',
    'CONVERT', 'TO_CHAR', 'TO_NUMBER',
    # Aggregate / window functions not already in SQL_KEYWORDS
    'MEDIAN', 'PERCENTILE', 'PERCENTILE_APPROX', 'VARIANCE', 'VAR_POP',
    'VAR_SAMP', 'STDDEV_POP', 'STDDEV_SAMP', 'CORR', 'COVAR_POP',
    'COVAR_SAMP', 'FIRST', 'LAST', 'NTH_VALUE', 'CUME_DIST',
    'PERCENT_RANK', 'RANK', 'DENSE_RANK', 'ROW_NUMBER', 'NTILE',
    'LAG', 'LEAD',
    # Spark / Databricks-specific
    'ARRAY', 'MAP', 'STRUCT', 'EXPLODE', 'POSEXPLODE', 'COLLECT_LIST',
    'COLLECT_SET', 'SIZE', 'ELEMENT_AT', 'ARRAY_CONTAINS',
    'TRANSFORM', 'AGGREGATE', 'FLATTEN', 'ZIP_WITH',
    'HASH', 'MD5', 'SHA1', 'SHA2', 'CRC32', 'XXHASH64',
    'UUID', 'MONOTONICALLY_INCREASING_ID',
    'GREATEST', 'LEAST', 'TYPEOF',
}

# Combined exclusion set used when scanning for bare column identifiers
# (union of keywords and known function names, all upper-cased)
SQL_IDENTIFIER_EXCLUSIONS = SQL_KEYWORDS | SQL_FUNCTIONS

# Aggregation Functions
AGGREGATION_FUNCTIONS = {
    'SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'STDDEV',
    'SUMX', 'COUNTX', 'AVERAGEX'
}

# DAX Functions
DAX_FUNCTIONS = {
    'SUMX', 'SUM', 'CALCULATE', 'COUNTX', 'COUNT', 
    'AVERAGEX', 'AVG', 'DIVIDE', 'FILTER'
}

# Mapping of DAX aggregations to Databricks equivalents
DAX_TO_DB_AGG_MAP = {
    "SUMX": "SUM",
    "SUM": "SUM",
    "COUNTX": "COUNT",
    "COUNT": "COUNT",
    "AVERAGEX": "AVG",
    "AVG": "AVG",
}

# Comment markers
PBI_COMMENT_MARKER = 'PBI:'

# Complexity levels
COMPLEXITY_SIMPLE = "simple"
COMPLEXITY_MEDIUM = "medium"
COMPLEXITY_COMPLEX = "complex"

# Validation status
STATUS_VALID = "VALID"
STATUS_INVALID = "INVALID"
STATUS_SKIPPED = "SKIPPED"
STATUS_ERROR = "ERROR"
STATUS_EQUIVALENT = "EQUIVALENT"  # Structurally different but semantically correct translation
STATUS_REVIEW = "REVIEW"          # Partial match, needs human verification

# Confidence levels
CONFIDENCE_HIGH = "high"
CONFIDENCE_MEDIUM = "medium"
CONFIDENCE_LOW = "low"
