# -*- coding: utf-8 -*-
"""
Database Service for Form Submission Logs
Stores submission results in database (SQLite by default)
"""

import logging
import sqlite3
from typing import Dict, Optional, List
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class FormSubmissionDB:
    """
    Database storage for form submission logs.
    Uses SQLite by default, can be extended for PostgreSQL/MySQL.
    """
    
    def __init__(self, db_path: str = "form_submissions.db"):
        """
        Initialize database service.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self._ensure_table()
        self._ensure_email_table() 
    
    def _get_connection(self):
        """Get database connection."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Return dict-like rows
        return conn
    
    def _ensure_table(self):
        """Create form_send_logs table if it doesn't exist."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS form_send_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    form_url TEXT NOT NULL,
                    base_url TEXT,
                    send_status TEXT NOT NULL,
                    http_status INTEGER,
                    mode TEXT,
                    error_reason TEXT,
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    company_name TEXT,
                    sender_email TEXT,
                    submission_method TEXT,
                    verification_confidence REAL,
                    retry_count INTEGER DEFAULT 0,
                    filled_fields INTEGER,
                    total_fields INTEGER,
                    response_url TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes for common queries
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_form_url ON form_send_logs(form_url)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_send_status ON form_send_logs(send_status)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_sent_at ON form_send_logs(sent_at)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_base_url ON form_send_logs(base_url)
            """)
            
            conn.commit()
            conn.close()
            logger.debug(f"Database table ensured: {self.db_path}")
            
        except Exception as e:
            logger.error(f"Failed to create database table: {e}")
            raise
    
    def log_submission(
        self,
        form_url: str,
        send_status: str,
        http_status: Optional[int] = None,
        mode: Optional[str] = None,
        error_reason: Optional[str] = None,
        company_name: Optional[str] = None,
        sender_email: Optional[str] = None,
        submission_method: Optional[str] = None,
        verification_confidence: Optional[float] = None,
        retry_count: int = 0,
        filled_fields: Optional[int] = None,
        total_fields: Optional[int] = None,
        response_url: Optional[str] = None,
        sent_at: Optional[datetime] = None
    ) -> Optional[int]:
        """
        Log form submission to database.
        
        Args:
            form_url: URL of the form
            send_status: 'success', 'failed', 'error', 'captcha_blocked', etc.
            http_status: HTTP status code
            mode: 'direct' or 'browser'
            error_reason: Error message if failed
            company_name: Company name
            sender_email: Sender email
            submission_method: 'standard', 'ajax', 'csrf', 'multi_step', 'browser'
            verification_confidence: Confidence score (0.0-1.0)
            retry_count: Number of retries
            filled_fields: Number of fields filled
            total_fields: Total fields in form
            response_url: Final response URL
            sent_at: Timestamp (defaults to now)
            
        Returns:
            Inserted row ID or None on error
        """
        try:
            # Extract base URL
            parsed = urlparse(form_url)
            base_url = f"{parsed.scheme}://{parsed.netloc}"
            
            if sent_at is None:
                sent_at = datetime.utcnow()
            
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO form_send_logs (
                    form_url, base_url, send_status, http_status, mode,
                    error_reason, company_name, sender_email, submission_method,
                    verification_confidence, retry_count, filled_fields,
                    total_fields, response_url, sent_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                form_url, base_url, send_status, http_status, mode,
                error_reason, company_name, sender_email, submission_method,
                verification_confidence, retry_count, filled_fields,
                total_fields, response_url, sent_at
            ))
            
            row_id = cursor.lastrowid
            conn.commit()
            conn.close()
            
            logger.debug(f"Logged submission to database: ID {row_id}")
            return row_id
            
        except Exception as e:
            logger.error(f"Failed to log submission to database: {e}")
            return None
    
    def get_submissions(
        self,
        form_url: Optional[str] = None,
        send_status: Optional[str] = None,
        base_url: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict]:
        """
        Query submission logs.
        
        Args:
            form_url: Filter by form URL
            send_status: Filter by status
            base_url: Filter by base URL
            limit: Maximum results
            offset: Offset for pagination
            
        Returns:
            List of submission dictionaries
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            query = "SELECT * FROM form_send_logs WHERE 1=1"
            params = []
            
            if form_url:
                query += " AND form_url = ?"
                params.append(form_url)
            
            if send_status:
                query += " AND send_status = ?"
                params.append(send_status)
            
            if base_url:
                query += " AND base_url = ?"
                params.append(base_url)
            
            query += " ORDER BY sent_at DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            conn.close()
            
            # Convert rows to dicts
            results = [dict(row) for row in rows]
            return results
            
        except Exception as e:
            logger.error(f"Failed to query submissions: {e}")
            return []
    

    def get_statistics(self) -> Dict:
        """
        Get submission statistics.
        
        Returns:
            Dictionary with statistics
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Total submissions
            cursor.execute("SELECT COUNT(*) FROM form_send_logs")
            total = cursor.fetchone()[0]
            
            # By status
            cursor.execute("""
                SELECT send_status, COUNT(*) as count
                FROM form_send_logs
                GROUP BY send_status
            """)
            by_status = {row[0]: row[1] for row in cursor.fetchall()}
            
            # By mode
            cursor.execute("""
                SELECT mode, COUNT(*) as count
                FROM form_send_logs
                WHERE mode IS NOT NULL
                GROUP BY mode
            """)
            by_mode = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Success rate
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN send_status = 'success' THEN 1 ELSE 0 END) as successful
                FROM form_send_logs
            """)
            row = cursor.fetchone()
            success_rate = (row[1] / row[0] * 100) if row[0] > 0 else 0
            
            conn.close()
            
            return {
                'total': total,
                'by_status': by_status,
                'by_mode': by_mode,
                'success_rate': round(success_rate, 2)
            }
            
        except Exception as e:
            logger.error(f"Failed to get statistics: {e}")
            return {}

    def _ensure_email_table(self):
        """Create email_send_logs table if it doesn't exist."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS email_send_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    recipient_email TEXT NOT NULL,
                    recipient_name TEXT,
                    company_name TEXT,
                    subject TEXT,
                    send_status TEXT NOT NULL,
                    message_id TEXT,
                    error_reason TEXT,
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    opened_at TIMESTAMP,
                    clicked_at TIMESTAMP,
                    bounced BOOLEAN DEFAULT 0,
                    source_url TEXT,
                    campaign_id TEXT,
                    template_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_email_recipient ON email_send_logs(recipient_email)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_email_status ON email_send_logs(send_status)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_email_sent_at ON email_send_logs(sent_at)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_email_campaign ON email_send_logs(campaign_id)
            """)
            
            conn.commit()
            conn.close()
            logger.debug("Email send logs table ensured")
            
        except Exception as e:
            logger.error(f"Failed to create email table: {e}")
            raise

    def log_email_send(
        self,
        recipient_email: str,
        send_status: str,
        recipient_name: Optional[str] = None,
        company_name: Optional[str] = None,
        subject: Optional[str] = None,
        message_id: Optional[str] = None,
        error_reason: Optional[str] = None,
        source_url: Optional[str] = None,
        campaign_id: Optional[str] = None,
        template_id: Optional[str] = None,
        sent_at: Optional[datetime] = None
    ) -> Optional[int]:
        """
        Log email send to database.
        
        Args:
            recipient_email: Recipient email address
            send_status: 'sent', 'failed', 'bounced', 'queued'
            recipient_name: Recipient name (optional)
            company_name: Company name (optional)
            subject: Email subject (optional)
            message_id: SendGrid message ID (optional)
            error_reason: Error message if failed (optional)
            source_url: Website URL from crawl results (optional)
            campaign_id: Campaign identifier (optional)
            template_id: SendGrid template ID (optional)
            sent_at: Timestamp (defaults to now)
            
        Returns:
            Inserted row ID or None on error
        """
        try:
            if sent_at is None:
                sent_at = datetime.utcnow()
            
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO email_send_logs (
                    recipient_email, recipient_name, company_name, subject,
                    send_status, message_id, error_reason, source_url,
                    campaign_id, template_id, sent_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                recipient_email, recipient_name, company_name, subject,
                send_status, message_id, error_reason, source_url,
                campaign_id, template_id, sent_at
            ))
            
            row_id = cursor.lastrowid
            conn.commit()
            conn.close()
            
            logger.debug(f"Logged email send to database: ID {row_id}")
            return row_id
            
        except Exception as e:
            logger.error(f"Failed to log email send: {e}")
            return None

    def get_email_sends(
        self,
        recipient_email: Optional[str] = None,
        send_status: Optional[str] = None,
        campaign_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict]:
        """
        Query email send logs.
        
        Args:
            recipient_email: Filter by recipient
            send_status: Filter by status
            campaign_id: Filter by campaign
            limit: Maximum results
            offset: Offset for pagination
            
        Returns:
            List of email send dictionaries
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            query = "SELECT * FROM email_send_logs WHERE 1=1"
            params = []
            
            if recipient_email:
                query += " AND recipient_email = ?"
                params.append(recipient_email)
            
            if send_status:
                query += " AND send_status = ?"
                params.append(send_status)
            
            if campaign_id:
                query += " AND campaign_id = ?"
                params.append(campaign_id)
            
            query += " ORDER BY sent_at DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            conn.close()
            
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.error(f"Failed to query email sends: {e}")
            return []

    def get_email_statistics(self) -> Dict:
        """
        Get email campaign statistics.
        
        Returns:
            Dictionary with statistics
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Total emails
            cursor.execute("SELECT COUNT(*) FROM email_send_logs")
            total = cursor.fetchone()[0]
            
            # By status
            cursor.execute("""
                SELECT send_status, COUNT(*) as count
                FROM email_send_logs
                GROUP BY send_status
            """)
            by_status = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Success rate
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN send_status = 'sent' THEN 1 ELSE 0 END) as successful
                FROM email_send_logs
            """)
            row = cursor.fetchone()
            success_rate = (row[1] / row[0] * 100) if row[0] > 0 else 0
            
            # Today's count
            cursor.execute("""
                SELECT COUNT(*) FROM email_send_logs
                WHERE DATE(sent_at) = DATE('now')
            """)
            today_count = cursor.fetchone()[0]
            
            conn.close()
            
            return {
                'total': total,
                'by_status': by_status,
                'success_rate': round(success_rate, 2),
                'today_count': today_count
            }
            
        except Exception as e:
            logger.error(f"Failed to get email statistics: {e}")
            return {}
        


    # That's it! Your db_service.py now supports both form submissions AND email campaigns!

    def close(self):
        """Close database connection (no-op for SQLite, but kept for API consistency)."""
        pass

