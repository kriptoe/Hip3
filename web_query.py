#!/usr/bin/env python3
"""
Vault Database Web Interface
Flask web app for querying and analyzing vault data
"""

from flask import Flask, render_template, request, jsonify
import sqlite3
from datetime import datetime

app = Flask(__name__)
DB_PATH = 'vaults.db'


def get_db_connection():
    """Get database connection"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def format_money(amount):
    """Format money with K/M/B suffix"""
    if amount is None:
        return "N/A"
    
    if amount >= 1_000_000_000:
        return f"${amount/1_000_000_000:.2f}B"
    elif amount >= 1_000_000:
        return f"${amount/1_000_000:.2f}M"
    elif amount >= 1_000:
        return f"${amount/1_000:.2f}K"
    else:
        return f"${amount:.2f}"


def format_apy(apy):
    """Format APY percentage"""
    if apy is None:
        return "N/A"
    return f"{apy:.2f}%"


@app.route('/')
def index():
    """Main page with query interface"""
    conn = get_db_connection()
    
    # Get available platforms and vault types
    platforms = [row[0] for row in conn.execute("SELECT DISTINCT platform FROM vault_snapshots ORDER BY platform").fetchall()]
    vault_types = [row[0] for row in conn.execute("SELECT DISTINCT vault_type FROM vault_snapshots ORDER BY vault_type").fetchall()]
    
    conn.close()
    
    return render_template('index.html', platforms=platforms, vault_types=vault_types)


@app.route('/api/available_vaults', methods=['POST'])
def get_available_vaults():
    """API endpoint for getting list of all unique vaults with optional filters"""
    data = request.json if request.json else {}
    
    platforms = data.get('platforms', [])
    vault_type = data.get('vault_type')
    
    conn = get_db_connection()
    
    query = """
        SELECT DISTINCT platform, vault_name
        FROM vault_snapshots
        WHERE 1=1
    """
    
    params = []
    
    if platforms:
        placeholders = ','.join('?' * len(platforms))
        query += f" AND platform IN ({placeholders})"
        params.extend(platforms)
    
    if vault_type and vault_type != '':
        query += " AND vault_type = ?"
        params.append(vault_type)
    
    query += " ORDER BY platform, vault_name"
    
    cursor = conn.cursor()
    cursor.execute(query, params)
    results = cursor.fetchall()
    
    # Format results
    vaults = []
    for row in results:
        vaults.append({
            'platform': row['platform'],
            'vault_name': row['vault_name'],
            'display_name': f"{row['platform']} - {row['vault_name']}"
        })
    
    conn.close()
    
    return jsonify({'vaults': vaults})


@app.route('/api/cumulative_deposits', methods=['POST'])
def get_cumulative_deposits():
    """API endpoint for getting cumulative deposit data over time"""
    data = request.json
    
    platforms = data.get('platforms', [])
    vault_type = data.get('vault_type')
    deposit_token = data.get('deposit_token')
    days_back = data.get('days_back', 30)
    
    conn = get_db_connection()
    
    # Build query for historical data grouped by timestamp
    query = """
        SELECT 
            timestamp,
            SUM(deposit_amount) as total_deposits
        FROM vault_snapshots
        WHERE 1=1
    """
    
    params = []
    
    if platforms:
        placeholders = ','.join('?' * len(platforms))
        query += f" AND platform IN ({placeholders})"
        params.extend(platforms)
    
    if vault_type and vault_type != '':
        query += " AND vault_type = ?"
        params.append(vault_type)
    
    if deposit_token and deposit_token != '':
        query += " AND deposit_token = ?"
        params.append(deposit_token)
    
    if days_back and days_back != '':
        query += " AND datetime(timestamp) >= datetime('now', '-' || ? || ' days')"
        params.append(int(days_back))
    
    query += " GROUP BY timestamp ORDER BY timestamp ASC"
    
    cursor = conn.cursor()
    cursor.execute(query, params)
    results = cursor.fetchall()
    
    # Format results
    history = []
    deposit_values = []
    for row in results:
        history.append({
            'timestamp': row['timestamp'],
            'total_deposits': row['total_deposits']
        })
        if row['total_deposits'] is not None:
            deposit_values.append(row['total_deposits'])
    
    # Calculate statistics
    avg_deposits = sum(deposit_values) / len(deposit_values) if deposit_values else 0
    min_deposits = min(deposit_values) if deposit_values else 0
    max_deposits = max(deposit_values) if deposit_values else 0
    current_deposits = deposit_values[-1] if deposit_values else 0
    
    conn.close()
    
    return jsonify({
        'history': history,
        'avg_deposits': avg_deposits,
        'min_deposits': min_deposits,
        'max_deposits': max_deposits,
        'current_deposits': current_deposits
    })


@app.route('/api/available_tokens', methods=['GET'])
def get_available_tokens():
    """API endpoint for getting list of all unique deposit tokens"""
    conn = get_db_connection()
    
    query = """
        SELECT DISTINCT deposit_token
        FROM vault_snapshots
        WHERE deposit_token IS NOT NULL
        ORDER BY deposit_token
    """
    
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    
    tokens = [row['deposit_token'] for row in results]
    
    conn.close()
    
    return jsonify({'tokens': tokens})


@app.route('/api/vault_history', methods=['POST'])
def get_vault_history():
    """API endpoint for getting historical APY data for a specific vault"""
    data = request.json
    
    platform = data.get('platform')
    vault_name = data.get('vault_name')
    days_back = data.get('days_back', 30)  # Default to 30 days
    
    if not platform or not vault_name:
        return jsonify({'error': 'Platform and vault_name required'}), 400
    
    conn = get_db_connection()
    
    # Build query for historical data
    query = """
        SELECT 
            timestamp,
            apy_percentage,
            deposit_amount
        FROM vault_snapshots
        WHERE platform = ? AND vault_name = ?
    """
    
    params = [platform, vault_name]
    
    if days_back and days_back != '':
        query += " AND datetime(timestamp) >= datetime('now', '-' || ? || ' days')"
        params.append(int(days_back))
    
    query += " ORDER BY timestamp ASC"
    
    cursor = conn.cursor()
    cursor.execute(query, params)
    results = cursor.fetchall()
    
    # Format results
    history = []
    valid_apys = []
    for row in results:
        history.append({
            'timestamp': row['timestamp'],
            'apy': row['apy_percentage'],
            'deposits': row['deposit_amount']
        })
        if row['apy_percentage'] is not None:
            valid_apys.append(row['apy_percentage'])
    
    # Calculate average APY
    avg_apy = sum(valid_apys) / len(valid_apys) if valid_apys else 0
    
    conn.close()
    
    return jsonify({
        'platform': platform,
        'vault_name': vault_name,
        'history': history,
        'avg_apy': avg_apy
    })


@app.route('/api/query', methods=['POST'])
def query_vaults():
    """API endpoint for querying vaults"""
    data = request.json
    
    platforms = data.get('platforms', [])
    vault_type = data.get('vault_type')  # Changed to single selection
    sort_by = data.get('sort_by', 'apy')
    sort_order = data.get('sort_order', 'desc')
    min_apy = data.get('min_apy')
    max_apy = data.get('max_apy')
    days_back = data.get('days_back')  # New parameter for date range
    min_deposits = data.get('min_deposits')  # New parameter for minimum deposits
    
    conn = get_db_connection()
    
    # Build subquery for date filtering
    date_condition = ""
    if days_back and days_back != '':
        date_condition = f"AND datetime(timestamp) >= datetime('now', '-{int(days_back)} days')"
    
    # Build query to get both latest APY and average APY
    query = f"""
        WITH latest_snapshots AS (
            SELECT 
                platform,
                vault_name,
                vault_type,
                total_deposits,
                deposit_amount,
                deposit_token,
                apy_percentage as latest_apy,
                timestamp
            FROM vault_snapshots
            WHERE id IN (
                SELECT MAX(id)
                FROM vault_snapshots
                WHERE 1=1 {date_condition}
                GROUP BY platform, vault_name
            )
        ),
        avg_apy AS (
            SELECT 
                platform,
                vault_name,
                AVG(apy_percentage) as average_apy
            FROM vault_snapshots
            WHERE 1=1 {date_condition}
            GROUP BY platform, vault_name
        )
        SELECT 
            ls.platform,
            ls.vault_name,
            ls.vault_type,
            ls.total_deposits,
            ls.deposit_amount,
            ls.deposit_token,
            ls.latest_apy,
            aa.average_apy,
            ls.timestamp
        FROM latest_snapshots ls
        LEFT JOIN avg_apy aa ON ls.platform = aa.platform AND ls.vault_name = aa.vault_name
        WHERE 1=1
    """
    
    conditions = []
    params = []
    
    if platforms:
        placeholders = ','.join('?' * len(platforms))
        conditions.append(f"ls.platform IN ({placeholders})")
        params.extend(platforms)
    
    if vault_type and vault_type != '':  # Changed to handle single selection
        conditions.append("ls.vault_type = ?")
        params.append(vault_type)
    
    if min_apy is not None and min_apy != '':
        conditions.append("ls.latest_apy >= ?")
        params.append(float(min_apy))
    
    if max_apy is not None and max_apy != '':
        conditions.append("ls.latest_apy <= ?")
        params.append(float(max_apy))
    
    if min_deposits is not None and min_deposits != '':
        conditions.append("ls.deposit_amount >= ?")
        params.append(float(min_deposits))
    
    if conditions:
        query += " AND " + " AND ".join(conditions)
    
    # Add sorting
    if sort_by == 'apy':
        query += f" ORDER BY ls.latest_apy {sort_order.upper()}"
    elif sort_by == 'avg_apy':
        query += f" ORDER BY aa.average_apy {sort_order.upper()}"
    elif sort_by == 'deposits':
        query += f" ORDER BY ls.deposit_amount {sort_order.upper()}"
    else:
        query += " ORDER BY ls.vault_name"
    
    cursor = conn.cursor()
    cursor.execute(query, params)
    results = cursor.fetchall()
    
    # Format results
    vaults = []
    for row in results:
        vaults.append({
            'platform': row['platform'],
            'vault_name': row['vault_name'],
            'vault_type': row['vault_type'],
            'total_deposits': row['total_deposits'],
            'deposit_amount': row['deposit_amount'],
            'deposit_amount_formatted': format_money(row['deposit_amount']),
            'deposit_token': row['deposit_token'],
            'latest_apy': row['latest_apy'],
            'latest_apy_formatted': format_apy(row['latest_apy']),
            'average_apy': row['average_apy'],
            'average_apy_formatted': format_apy(row['average_apy']),
            'timestamp': row['timestamp']
        })
    
    conn.close()
    
    return jsonify({
        'vaults': vaults,
        'count': len(vaults)
    })


@app.route('/api/stats', methods=['POST'])
def get_stats():
    """API endpoint for getting statistics"""
    data = request.json
    
    platforms = data.get('platforms', [])
    vault_type = data.get('vault_type')  # Changed to single selection
    days_back = data.get('days_back')  # New parameter for date range
    min_deposits = data.get('min_deposits')  # New parameter for minimum deposits
    
    conn = get_db_connection()
    
    # Build subquery for date filtering
    date_condition = ""
    if days_back and days_back != '':
        date_condition = f"AND datetime(timestamp) >= datetime('now', '-{int(days_back)} days')"
    
    # Build query for latest snapshots with average APY
    query = f"""
        WITH latest_snapshots AS (
            SELECT 
                platform,
                vault_name,
                vault_type,
                deposit_amount,
                apy_percentage as latest_apy
            FROM vault_snapshots
            WHERE id IN (
                SELECT MAX(id)
                FROM vault_snapshots
                WHERE 1=1 {date_condition}
                GROUP BY platform, vault_name
            )
        ),
        avg_apy AS (
            SELECT 
                platform,
                vault_name,
                AVG(apy_percentage) as average_apy
            FROM vault_snapshots
            WHERE 1=1 {date_condition}
            GROUP BY platform, vault_name
        )
        SELECT 
            ls.platform,
            ls.vault_type,
            ls.deposit_amount,
            ls.latest_apy,
            aa.average_apy
        FROM latest_snapshots ls
        LEFT JOIN avg_apy aa ON ls.platform = aa.platform AND ls.vault_name = aa.vault_name
        WHERE 1=1
    """
    
    conditions = []
    params = []
    
    if platforms:
        placeholders = ','.join('?' * len(platforms))
        conditions.append(f"ls.platform IN ({placeholders})")
        params.extend(platforms)
    
    if vault_type and vault_type != '':  # Changed to handle single selection
        conditions.append("ls.vault_type = ?")
        params.append(vault_type)
    
    if min_deposits is not None and min_deposits != '':
        conditions.append("ls.deposit_amount >= ?")
        params.append(float(min_deposits))
    
    if conditions:
        query += " AND " + " AND ".join(conditions)
    
    cursor = conn.cursor()
    cursor.execute(query, params)
    results = cursor.fetchall()
    
    # Calculate statistics
    valid_latest_apys = [row['latest_apy'] for row in results if row['latest_apy'] is not None]
    valid_avg_apys = [row['average_apy'] for row in results if row['average_apy'] is not None]
    valid_deposits = [row['deposit_amount'] for row in results if row['deposit_amount'] is not None]
    
    stats = {
        'total_vaults': len(results),
        'avg_latest_apy': sum(valid_latest_apys) / len(valid_latest_apys) if valid_latest_apys else 0,
        'avg_period_apy': sum(valid_avg_apys) / len(valid_avg_apys) if valid_avg_apys else 0,
        'max_apy': max(valid_latest_apys) if valid_latest_apys else 0,
        'min_apy': min(valid_latest_apys) if valid_latest_apys else 0,
        'total_tvl': sum(valid_deposits) if valid_deposits else 0,
        'avg_tvl': sum(valid_deposits) / len(valid_deposits) if valid_deposits else 0,
        'total_tvl_formatted': format_money(sum(valid_deposits) if valid_deposits else 0),
        'avg_tvl_formatted': format_money(sum(valid_deposits) / len(valid_deposits) if valid_deposits else 0),
        'avg_latest_apy_formatted': format_apy(sum(valid_latest_apys) / len(valid_latest_apys) if valid_latest_apys else 0),
        'avg_period_apy_formatted': format_apy(sum(valid_avg_apys) / len(valid_avg_apys) if valid_avg_apys else 0),
        'max_apy_formatted': format_apy(max(valid_latest_apys) if valid_latest_apys else 0),
        'min_apy_formatted': format_apy(min(valid_latest_apys) if valid_latest_apys else 0),
    }
    
    # Get breakdown by platform
    platform_stats = {}
    for platform in set(row['platform'] for row in results):
        platform_results = [r for r in results if r['platform'] == platform]
        platform_latest_apys = [r['latest_apy'] for r in platform_results if r['latest_apy']]
        platform_avg_apys = [r['average_apy'] for r in platform_results if r['average_apy']]
        platform_deposits = [r['deposit_amount'] for r in platform_results if r['deposit_amount']]
        
        platform_stats[platform] = {
            'count': len(platform_results),
            'avg_latest_apy': sum(platform_latest_apys) / len(platform_latest_apys) if platform_latest_apys else 0,
            'avg_period_apy': sum(platform_avg_apys) / len(platform_avg_apys) if platform_avg_apys else 0,
            'total_tvl': sum(platform_deposits) if platform_deposits else 0,
            'total_tvl_formatted': format_money(sum(platform_deposits) if platform_deposits else 0),
        }
    
    # Get breakdown by type
    type_stats = {}
    for vault_type_stat in set(row['vault_type'] for row in results):
        type_results = [r for r in results if r['vault_type'] == vault_type_stat]
        type_latest_apys = [r['latest_apy'] for r in type_results if r['latest_apy']]
        type_avg_apys = [r['average_apy'] for r in type_results if r['average_apy']]
        type_deposits = [r['deposit_amount'] for r in type_results if r['deposit_amount']]
        
        type_stats[vault_type_stat] = {
            'count': len(type_results),
            'avg_latest_apy': sum(type_latest_apys) / len(type_latest_apys) if type_latest_apys else 0,
            'avg_period_apy': sum(type_avg_apys) / len(type_avg_apys) if type_avg_apys else 0,
            'total_tvl': sum(type_deposits) if type_deposits else 0,
            'total_tvl_formatted': format_money(sum(type_deposits) if type_deposits else 0),
        }
    
    conn.close()
    
    return jsonify({
        'overall': stats,
        'by_platform': platform_stats,
        'by_type': type_stats
    })


if __name__ == '__main__':
    print("="*60)
    print("VAULT DATABASE WEB INTERFACE")
    print("="*60)
    print("\nStarting web server...")
    print("Open your browser and go to: http://localhost:5000")
    print("\nPress Ctrl+C to stop the server")
    print("="*60 + "\n")
    
    app.run(debug=True, host='0.0.0.0', port=5000)
