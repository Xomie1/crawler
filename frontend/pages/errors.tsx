import { useState, useEffect } from 'react';
import type { ReactNode } from 'react';
import Layout from '../components/Layout';
import Badge from '../components/Badge';
import { api } from '../lib/api';

export default function ErrorsPage() {
  const [errors, setErrors] = useState<any[]>([]);
  const [phase, setPhase] = useState<string>('all');
  const [loading, setLoading] = useState(false);
  const [expandedError, setExpandedError] = useState<number | null>(null);

  async function loadErrors() {
    try {
      setLoading(true);
      const data = await api.getErrors?.(phase === 'all' ? 'all' : phase, 100);
      setErrors(data || []);
    } catch (e: any) {
      console.error('Failed to load errors:', e);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadErrors();
  }, [phase]);

  async function exportErrors() {
    try {
      const blob = await api.exportErrors?.(phase === 'all' ? undefined : phase);
      const url = window.URL.createObjectURL(blob!);
      const a = document.createElement('a');
      a.href = url;
      a.download = `errors_${phase}_${new Date().toISOString().split('T')[0]}.csv`;
      a.click();
      window.URL.revokeObjectURL(url);
    } catch (e: any) {
      console.error('Export failed:', e);
    }
  }

  const groupedErrors = errors.reduce((acc: Record<string, number>, err: any) => {
    const key = err.error_type;
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {} as Record<string, number>);

  return (
    <Layout children={<div className="main-content">
      <div className="panel">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div>
            <h2 style={{ margin: '0 0 4px 0' }}>Error Log</h2>
            <p style={{ margin: 0, fontSize: '14px', color: 'var(--color-text-secondary)' }}>
              Track and export errors from all phases
            </p>
          </div>
          <div style={{ display: 'flex', gap: '8px' }}>
            <button className="btn-secondary" onClick={loadErrors} style={{ fontSize: '12px' }}>
              🔄 Refresh
            </button>
            <button className="btn-secondary" onClick={exportErrors} style={{ fontSize: '12px' }}>
              📥 Export CSV
            </button>
          </div>
        </div>
      </div>

      {/* Filter */}
      <div className="panel">
        <label style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span style={{ fontSize: '14px' }}>Phase:</span>
          <select
            className="input"
            value={phase}
            onChange={(e: React.ChangeEvent<HTMLSelectElement>) => setPhase(e.target.value)}
            style={{ maxWidth: '200px' }}
          >
            <option value="all">All Phases</option>
            <option value="phase1">Phase 1 (Crawl)</option>
            <option value="phase2">Phase 2 (Email)</option>
            <option value="phase3">Phase 3 (Forms)</option>
            <option value="phase4">Phase 4 (PDF)</option>
          </select>
        </label>
      </div>

      {/* Error Summary */}
      {errors.length > 0 && (
        <div className="panel">
          <h3 style={{ marginTop: 0 }}>Error Summary</h3>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', gap: '12px' }}>
            <div style={{ padding: '12px', backgroundColor: 'var(--color-bg-secondary)', borderRadius: '8px' }}>
              <div style={{ fontSize: '12px', color: 'var(--color-text-secondary)' }}>Total Errors</div>
              <div style={{ fontSize: '24px', fontWeight: 'bold', color: 'var(--color-error)' }}>{errors.length}</div>
            </div>
            <div style={{ padding: '12px', backgroundColor: 'var(--color-bg-secondary)', borderRadius: '8px' }}>
              <div style={{ fontSize: '12px', color: 'var(--color-text-secondary)' }}>Unresolved</div>
              <div style={{ fontSize: '24px', fontWeight: 'bold', color: 'var(--color-error)' }}>
                {errors.filter(e => !e.resolved).length}
              </div>
            </div>
            <div style={{ padding: '12px', backgroundColor: 'var(--color-bg-secondary)', borderRadius: '8px' }}>
              <div style={{ fontSize: '12px', color: 'var(--color-text-secondary)' }}>Error Types</div>
              <div style={{ fontSize: '24px', fontWeight: 'bold', color: 'var(--color-warn)' }}>
                {Object.keys(groupedErrors).length}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Error Type Breakdown */}
      {Object.keys(groupedErrors).length > 0 && (
        <div className="panel">
          <h3 style={{ marginTop: 0 }}>By Error Type</h3>
          <div style={{ overflowX: 'auto' }}>
            <table className="table">
              <thead>
                <tr>
                  <th>Error Type</th>
                  <th style={{ textAlign: 'right' }}>Count</th>
                  <th style={{ textAlign: 'right' }}>%</th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(groupedErrors)
                  .sort((a: any, b: any) => b[1] - a[1])
                  .map(([type, count]: any) => (
                    <tr key={type}>
                      <td style={{ fontWeight: 500 }}>{type}</td>
                      <td style={{ textAlign: 'right' }}>{count}</td>
                      <td style={{ textAlign: 'right', color: 'var(--color-text-secondary)' }}>
                        {Math.round((count / errors.length) * 100)}%
                      </td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Error Details */}
      <div className="panel">
        <h3 style={{ marginTop: 0 }}>
          Details ({loading ? 'loading...' : errors.length})
        </h3>
        {loading ? (
          <p style={{ color: 'var(--color-text-secondary)' }}>Loading errors...</p>
        ) : errors.length === 0 ? (
          <p style={{ color: 'var(--color-text-secondary)' }}>
            {phase === 'all' ? 'No errors found' : `No errors found in ${phase}`}
          </p>
        ) : (
          <div style={{ overflowX: 'auto' }}>
            <table className="table">
              <thead>
                <tr>
                  <th>Phase</th>
                  <th>Type</th>
                  <th>Message</th>
                  <th>Context</th>
                  <th>Time</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {errors.map((error: any, idx: number) => (
                  <tr
                    key={idx}
                    style={{
                      backgroundColor: error.resolved ? 'transparent' : 'rgba(239, 68, 68, 0.05)',
                      cursor: 'pointer',
                    }}
                    onClick={() => setExpandedError(expandedError === idx ? null : idx)}
                  >
                    <td style={{ fontWeight: 600, color: 'var(--color-error)' }}>{error.phase}</td>
                    <td style={{ fontSize: '12px', maxWidth: '150px', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                      {error.error_type}
                    </td>
                    <td style={{ fontSize: '12px', maxWidth: '250px', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                      {error.error_message}
                    </td>
                    <td style={{ fontSize: '11px', color: 'var(--color-text-secondary)', maxWidth: '150px', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                      {error.context}
                    </td>
                    <td style={{ fontSize: '11px', color: 'var(--color-text-secondary)', whiteSpace: 'nowrap' }}>
                      {new Date(error.created_at).toLocaleDateString()} {new Date(error.created_at).toLocaleTimeString()}
                    </td>
                    <td>
                      <Badge status={error.resolved ? 'success' : 'error'} text={error.resolved ? 'Resolved' : 'Active'} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Error Details Expandable */}
      {expandedError !== null && errors[expandedError] && (
        <div className="panel" style={{ backgroundColor: 'var(--color-bg-secondary)', marginTop: '16px' }}>
          <h4 style={{ marginTop: 0 }}>Full Details</h4>
          <div style={{ fontFamily: 'monospace', fontSize: '12px', lineHeight: '1.5' }}>
            <div><strong>Phase:</strong> {errors[expandedError].phase}</div>
            <div><strong>Type:</strong> {errors[expandedError].error_type}</div>
            <div style={{ marginTop: '8px' }}>
              <strong>Message:</strong>
              <pre style={{ margin: '4px 0', padding: '8px', backgroundColor: 'var(--color-bg-primary)', borderRadius: '4px', overflow: 'auto', maxHeight: '150px' }}>
                {errors[expandedError].error_message}
              </pre>
            </div>
            <div style={{ marginTop: '8px' }}>
              <strong>Context:</strong>
              <pre style={{ margin: '4px 0', padding: '8px', backgroundColor: 'var(--color-bg-primary)', borderRadius: '4px', overflow: 'auto', maxHeight: '150px' }}>
                {errors[expandedError].context}
              </pre>
            </div>
            <div><strong>Time:</strong> {new Date(errors[expandedError].created_at).toISOString()}</div>
            <div><strong>Status:</strong> {errors[expandedError].resolved ? '✓ Resolved' : '✕ Active'}</div>
          </div>
        </div>
      )}
    </div>} />
  );
}
