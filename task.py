from flask import Flask, request, jsonify
from datetime import datetime
import sqlite3
import json
import os

app = Flask(__name__)

# Database setup
DATABASE = 'task_scheduler.db'

def init_db():
    """Initialize the database with the tasks table"""
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tasks (
            task_str_id TEXT PRIMARY KEY,
            description TEXT NOT NULL,
            estimated_time_minutes INTEGER NOT NULL,
            status TEXT DEFAULT 'pending',
            submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()

def get_db_connection():
    """Get database connection"""
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row  # This enables column access by name
    return conn

def dict_from_row(row):
    """Convert sqlite Row to dictionary"""
    return {key: row[key] for key in row.keys()}

# Initialize database on startup
init_db()

@app.route('/tasks', methods=['POST'])
def create_task():
    """
    POST /tasks - Create a new task
    Accepts JSON: {"task_str_id": "TASK001", "description": "Process data", "estimated_time_minutes": 30}
    """
    try:
        data = request.get_json()
        
        # Validation
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400
        
        required_fields = ['task_str_id', 'description', 'estimated_time_minutes']
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Missing required field: {field}"}), 400
        
        task_str_id = data['task_str_id']
        description = data['description']
        estimated_time_minutes = data['estimated_time_minutes']
        
        # Validate estimated_time_minutes > 0
        if not isinstance(estimated_time_minutes, int) or estimated_time_minutes <= 0:
            return jsonify({"error": "estimated_time_minutes must be greater than 0"}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO tasks (task_str_id, description, estimated_time_minutes, status)
                VALUES (?, ?, ?, 'pending')
            ''', (task_str_id, description, estimated_time_minutes))
            
            conn.commit()
            
            # Retrieve the created task
            cursor.execute('SELECT * FROM tasks WHERE task_str_id = ?', (task_str_id,))
            task = cursor.execute('SELECT * FROM tasks WHERE task_str_id = ?', (task_str_id,)).fetchone()
            
            return jsonify(dict_from_row(task)), 201
            
        except sqlite3.IntegrityError:
            return jsonify({"error": "task_str_id must be unique"}), 400
        finally:
            conn.close()
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/tasks/<task_str_id>', methods=['GET'])
def get_task(task_str_id):
    """
    GET /tasks/{task_str_id} - Retrieve a task by its task_str_id
    Returns: Full task object or 404
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM tasks WHERE task_str_id = ?', (task_str_id,))
        task = cursor.fetchone()
        
        conn.close()
        
        if task:
            return jsonify(dict_from_row(task))
        else:
            return jsonify({"error": "Task not found"}), 404
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/tasks/<task_str_id>/status', methods=['PUT'])
def update_task_status(task_str_id):
    """
    PUT /tasks/{task_str_id}/status - Update task status
    Accepts JSON: {"new_status": "processing"}
    """
    try:
        data = request.get_json()
        
        if not data or 'new_status' not in data:
            return jsonify({"error": "Missing required field: new_status"}), 400
        
        new_status = data['new_status']
        allowed_statuses = ['pending', 'processing', 'completed']
        
        if new_status not in allowed_statuses:
            return jsonify({"error": f"Invalid status. Allowed values: {allowed_statuses}"}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get current task to check for status regression
        cursor.execute('SELECT * FROM tasks WHERE task_str_id = ?', (task_str_id,))
        current_task = cursor.fetchone()
        
        if not current_task:
            conn.close()
            return jsonify({"error": "Task not found"}), 404
        
        current_status = current_task['status']
        
        # Prevent invalid status transitions
        invalid_transitions = [
            ('completed', 'pending'),
            ('completed', 'processing'),
            ('processing', 'pending')
        ]
        
        if (current_status, new_status) in invalid_transitions:
            conn.close()
            return jsonify({
                "error": f"Invalid status transition from '{current_status}' to '{new_status}'"
            }), 400
        
        # Update the status
        cursor.execute('''
            UPDATE tasks SET status = ? WHERE task_str_id = ?
        ''', (new_status, task_str_id))
        
        conn.commit()
        
        # Return updated task
        cursor.execute('SELECT * FROM tasks WHERE task_str_id = ?', (task_str_id,))
        updated_task = cursor.fetchone()
        
        conn.close()
        
        return jsonify(dict_from_row(updated_task))
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/tasks/next-to-process', methods=['GET'])
def get_next_task_to_process():
    """
    GET /tasks/next-to-process - Get next task to process (shortest first, then oldest)
    Returns: Single pending task with smallest estimated_time_minutes, oldest submitted_at as tiebreaker
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM tasks 
            WHERE status = 'pending' 
            ORDER BY estimated_time_minutes ASC, submitted_at ASC 
            LIMIT 1
        ''')
        
        task = cursor.fetchone()
        conn.close()
        
        if task:
            return jsonify(dict_from_row(task))
        else:
            return jsonify({"error": "No pending tasks found"}), 404
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/tasks/pending', methods=['GET'])
def list_pending_tasks():
    """
    GET /tasks/pending?sort_by=time&order=asc&limit=10 - List pending tasks with sorting and limit
    Parameters are optional:
    - sort_by: 'time' (estimated_time_minutes) or 'submitted_at'
    - order: 'asc' or 'desc'
    - limit: number of results to return
    """
    try:
        # Get query parameters
        sort_by = request.args.get('sort_by', 'estimated_time_minutes')
        order = request.args.get('order', 'asc')
        limit = request.args.get('limit', type=int)
        
        # Validate parameters
        valid_sort_fields = ['estimated_time_minutes', 'submitted_at', 'time']
        if sort_by == 'time':
            sort_by = 'estimated_time_minutes'  # Map 'time' to actual column name
        
        if sort_by not in ['estimated_time_minutes', 'submitted_at']:
            return jsonify({"error": "Invalid sort_by parameter. Use 'time' or 'submitted_at'"}), 400
        
        if order not in ['asc', 'desc']:
            return jsonify({"error": "Invalid order parameter. Use 'asc' or 'desc'"}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Build query
        query = f'''
            SELECT * FROM tasks 
            WHERE status = 'pending' 
            ORDER BY {sort_by} {order.upper()}
        '''
        
        if limit:
            query += f' LIMIT {limit}'
        
        cursor.execute(query)
        tasks = cursor.fetchall()
        
        conn.close()
        
        # Convert to list of dictionaries
        result = [dict_from_row(task) for task in tasks]
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Additional helpful endpoints

@app.route('/tasks', methods=['GET'])
def list_all_tasks():
    """GET /tasks - List all tasks"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM tasks ORDER BY submitted_at DESC')
        tasks = cursor.fetchall()
        
        conn.close()
        
        result = [dict_from_row(task) for task in tasks]
        return jsonify(result)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/tasks/<task_str_id>', methods=['DELETE'])
def delete_task(task_str_id):
    """DELETE /tasks/{task_str_id} - Delete a task"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM tasks WHERE task_str_id = ?', (task_str_id,))
        
        if cursor.rowcount == 0:
            conn.close()
            return jsonify({"error": "Task not found"}), 404
        
        conn.commit()
        conn.close()
        
        return jsonify({"message": "Task deleted successfully"})
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({"error": "Method not allowed"}), 405

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
