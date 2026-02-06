import Layout from '../components/Layout';
import StatCard from '../components/StatCard';
import Badge from '../components/Badge';
import CircularProgress from '../components/CircularProgress';
import { useEffect, useState } from 'react';
import { api } from '../lib/api';

export default function Home() {
  const [metrics, setMetrics] = useState<any | null>(null);
  const [campaigns, setCampaigns] = useState<any[]>([]);
  const [errors, setErrors] = useState<any[]>([]);
  const [showDebug, setShowDebug] = useState(false);
  const [loading, setLoading] = useState(true);

  async function refresh() {
    try {
      setLoading(true);
      const [metricsData, campaignsData, errorsData] = await Promise.all([
        api.getQueueMetrics().catch(() => null),
        api.getCampaigns?.().catch(() => []),
        api.getErrors?.('all', 10).catch(() => []),
      ]);
      setMetrics(metricsData);
      setCampaigns(campaignsData || []);
      setErrors(errorsData || []);
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    refresh();
    // Fast refresh (5s) when items are in progress, slow refresh (30s) when idle
    const interval = setInterval(() => {
      const hasActive = metrics?.summary?.total_in_progress > 0 || (Array.isArray(campaigns) && campaigns.some((c: any) => c.status === 'in_progress'));
      // Refresh every 5s if active, 30s if idle
      setTimeout(refresh, hasActive ? 5000 : 30000);
    }, 5000);
    return () => clearInterval(interval);
  }, [metrics?.summary?.total_in_progress, campaigns?.length]);

  if (loading && !metrics) {
    return (
      <Layout>
        <div className="panel fade-in">
          <h2>Dashboard</h2>
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
            <div className="spinner lg" />
            <p>Loading dashboard...</p>
          </div>
        </div>
      </Layout>
    );
  }

  const summary = metrics?.summary || { total_queued: 0, total_in_progress: 0, total_done: 0, total_failed: 0 };
  const perPhase = metrics?.per_phase || {};
  const total = summary.total_queued + summary.total_in_progress + summary.total_done + summary.total_failed;

  async function exportResults(type: string) {
    try {
      let blob;
      const timestamp = new Date().toISOString().split('T')[0];
      let filename = `export_${timestamp}.csv`;
      
      switch(type) {
        case 'crawl':
          blob = await api.exportCrawlResults();
          filename = `crawl_results_${timestamp}.csv`;
          break;
        case 'campaigns':
          blob = await api.exportCampaignResults();
          filename = `campaign_results_${timestamp}.csv`;
          break;
        case 'all':
          blob = await api.exportResults();
          filename = `complete_results_${timestamp}.csv`;
          break;
      }
      
      if (!blob) throw new Error('No data to export');
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      a.click();
      window.URL.revokeObjectURL(url);
    } catch (e: any) {
      console.error('Export failed:', e);
      alert('Export failed: ' + e.message);
    }
  }

  return (
    <Layout>
      {/* Header */}
      <div className="panel slide-up">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '16px' }}>
          <div>
            <h2 style={{ margin: '0 0 8px 0', fontSize: '28px', fontWeight: 700 }}>Dashboard</h2>
            <p style={{ margin: 0, fontSize: '14px', color: 'var(--color-text-secondary)' }}>
              Real-time system metrics and campaign tracking
            </p>
          </div>
          <div style={{ display: 'flex', gap: '10px' }}>
            <button className="btn-secondary" onClick={refresh} style={{ fontSize: '13px' }}>
              🔄 Refresh
            </button>
            <div style={{ position: 'relative', display: 'inline-block' }}>
              <button 
                className="btn-secondary" 
                style={{ fontSize: '13px' }}
                onMouseEnter={(e) => {
                  const menu = (e.currentTarget.nextElementSibling as HTMLElement);
                  if (menu) menu.style.display = 'block';
                }}
              >
                📥 Export
              </button>
              <div 
                style={{
                  display: 'none',
                  position: 'absolute',
                  right: 0,
                  top: '100%',
                  marginTop: '8px',
                  background: 'var(--glass-bg)',
                  backdropFilter: 'var(--backdrop-blur)',
                  border: '1px solid var(--glass-border)',
                  borderRadius: '12px',
                  boxShadow: 'var(--glass-shadow)',
                  zIndex: 1000,
                  minWidth: '200px',
                  overflow: 'hidden',
                }}
                onMouseLeave={(e) => {
                  (e.currentTarget as HTMLElement).style.display = 'none';
                }}
                onMouseEnter={(e) => {
                  (e.currentTarget as HTMLElement).style.display = 'block';
                }}
              >
                <button
                  onClick={() => exportResults('crawl')}
                  style={{
                    display: 'block',
                    width: '100%',
                    padding: '12px 16px',
                    textAlign: 'left',
                    border: 'none',
                    background: 'transparent',
                    cursor: 'pointer',
                    fontSize: '13px',
                    color: 'var(--color-text)',
                    borderBottom: '1px solid var(--color-border)',
                    transition: 'background 0.2s ease',
                  }}
                  onMouseEnter={(e) => {
                    (e.currentTarget as HTMLElement).style.background = 'var(--color-surface-hover)';
                  }}
                  onMouseLeave={(e) => {
                    (e.currentTarget as HTMLElement).style.background = 'transparent';
                  }}
                >
                  📊 Crawl Results
                </button>
                <button
                  onClick={() => exportResults('campaigns')}
                  style={{
                    display: 'block',
                    width: '100%',
                    padding: '12px 16px',
                    textAlign: 'left',
                    border: 'none',
                    background: 'transparent',
                    cursor: 'pointer',
                    fontSize: '13px',
                    color: 'var(--color-text)',
                    borderBottom: '1px solid var(--color-border)',
                    transition: 'background 0.2s ease',
                  }}
                  onMouseEnter={(e) => {
                    (e.currentTarget as HTMLElement).style.background = 'var(--color-surface-hover)';
                  }}
                  onMouseLeave={(e) => {
                    (e.currentTarget as HTMLElement).style.background = 'transparent';
                  }}
                >
                  📧 Campaign Results
                </button>
                <button
                  onClick={() => exportResults('all')}
                  style={{
                    display: 'block',
                    width: '100%',
                    padding: '12px 16px',
                    textAlign: 'left',
                    border: 'none',
                    background: 'transparent',
                    cursor: 'pointer',
                    fontSize: '13px',
                    color: 'var(--color-text)',
                    transition: 'background 0.2s ease',
                  }}
                  onMouseEnter={(e) => {
                    (e.currentTarget as HTMLElement).style.background = 'var(--color-surface-hover)';
                  }}
                  onMouseLeave={(e) => {
                    (e.currentTarget as HTMLElement).style.background = 'transparent';
                  }}
                >
                  🗂️ All Results
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Main Progress Section - Circular on Left, Metrics on Right */}
      <div className="panel slide-up" style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '40px', alignItems: 'center', animationDelay: '0.1s' }}>
        {/* Left: Circular Progress Ring */}
        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', padding: '24px' }}>
          <CircularProgress
            percentage={total > 0 ? Math.round(((summary.total_done + summary.total_failed) / total) * 100) : 0}
            color="primary"
            size={200}
            strokeWidth={10}
            showPercentage={true}
          />
          <p style={{ marginTop: '20px', fontSize: '14px', color: 'var(--color-text-secondary)', textAlign: 'center' }}>
            {total > 0 ? `${summary.total_done + summary.total_failed} of ${total} tasks completed` : 'No tasks yet'}
          </p>
        </div>

        {/* Right: Status Metrics */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
          <div style={{ 
            display: 'flex', 
            alignItems: 'center', 
            gap: '12px',
            padding: '16px',
            borderRadius: '12px',
            backgroundColor: 'var(--color-surface)',
            borderLeft: '4px solid var(--color-info)',
          }}>
            <span style={{ fontSize: '24px' }}>⏳</span>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--color-text-secondary)' }}>Queued</div>
              <div style={{ fontSize: '20px', fontWeight: 700, color: 'var(--color-info)' }}>{summary.total_queued}</div>
            </div>
          </div>

          <div style={{ 
            display: 'flex', 
            alignItems: 'center', 
            gap: '12px',
            padding: '16px',
            borderRadius: '12px',
            backgroundColor: 'var(--color-surface)',
            borderLeft: '4px solid var(--color-primary)',
          }}>
            <span style={{ fontSize: '24px' }}>⚙️</span>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--color-text-secondary)' }}>In Progress</div>
              <div style={{ fontSize: '20px', fontWeight: 700, color: 'var(--color-primary)' }}>{summary.total_in_progress}</div>
            </div>
          </div>

          <div style={{ 
            display: 'flex', 
            alignItems: 'center', 
            gap: '12px',
            padding: '16px',
            borderRadius: '12px',
            backgroundColor: 'var(--color-surface)',
            borderLeft: '4px solid var(--color-success)',
          }}>
            <span style={{ fontSize: '24px' }}>✓</span>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--color-text-secondary)' }}>Completed</div>
              <div style={{ fontSize: '20px', fontWeight: 700, color: 'var(--color-success)' }}>{summary.total_done}</div>
            </div>
          </div>

          <div style={{ 
            display: 'flex', 
            alignItems: 'center', 
            gap: '12px',
            padding: '16px',
            borderRadius: '12px',
            backgroundColor: 'var(--color-surface)',
            borderLeft: '4px solid var(--color-error)',
          }}>
            <span style={{ fontSize: '24px' }}>✕</span>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--color-text-secondary)' }}>Failed</div>
              <div style={{ fontSize: '20px', fontWeight: 700, color: 'var(--color-error)' }}>{summary.total_failed}</div>
            </div>
          </div>
        </div>
      </div>

      {/* Circular Progress by Phase */}
      <div className="panel slide-up" style={{ animationDelay: '0.2s' }}>
        <h3 style={{ marginTop: 0, fontSize: '20px', fontWeight: 600 }}>Progress by Phase</h3>
        <div style={{ 
          display: 'grid', 
          gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))', 
          gap: '32px',
          padding: '20px 0',
        }}>
          {perPhase.crawls && (
            <div style={{ textAlign: 'center' }}>
              <CircularProgress
                percentage={perPhase.crawls.done > 0 ? Math.round((perPhase.crawls.done / (perPhase.crawls.done + perPhase.crawls.queued)) * 100) : 0}
                color="primary"
                label="Crawls"
                size={120}
              />
              <div style={{ marginTop: '12px' }}>
                <Badge status="success" text={`${Math.round((perPhase.crawls.success_rate || 0) * 100)}% success`} />
              </div>
            </div>
          )}
          {perPhase.emails && (
            <div style={{ textAlign: 'center' }}>
              <CircularProgress
                percentage={perPhase.emails.done > 0 ? Math.round((perPhase.emails.done / (perPhase.emails.done + perPhase.emails.queued)) * 100) : 0}
                color="success"
                label="Emails"
                size={120}
              />
              <div style={{ marginTop: '12px' }}>
                <Badge status="success" text={`${Math.round((perPhase.emails.success_rate || 0) * 100)}% success`} />
              </div>
            </div>
          )}
          {perPhase.forms && (
            <div style={{ textAlign: 'center' }}>
              <CircularProgress
                percentage={perPhase.forms.done > 0 ? Math.round((perPhase.forms.done / (perPhase.forms.done + perPhase.forms.queued)) * 100) : 0}
                color="info"
                label="Forms"
                size={120}
              />
              <div style={{ marginTop: '12px' }}>
                <Badge status="success" text={`${Math.round((perPhase.forms.success_rate || 0) * 100)}% success`} />
              </div>
            </div>
          )}
          {perPhase.pdfs && (
            <div style={{ textAlign: 'center' }}>
              <CircularProgress
                percentage={perPhase.pdfs.done > 0 ? Math.round((perPhase.pdfs.done / (perPhase.pdfs.done + perPhase.pdfs.queued)) * 100) : 0}
                color="pending"
                label="PDFs"
                size={120}
              />
              <div style={{ marginTop: '12px' }}>
                <Badge status="success" text={`${Math.round((perPhase.pdfs.success_rate || 0) * 100)}% success`} />
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Recent Campaigns */}
      {campaigns.length > 0 && (
        <div className="panel slide-up" style={{ animationDelay: '0.3s' }}>
          <h3 style={{ marginTop: 0, fontSize: '20px', fontWeight: 600 }}>Recent Campaigns</h3>
          <div style={{ overflowX: 'auto' }}>
            <table className="table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Created</th>
                  <th>Phase 1</th>
                  <th>Phase 2</th>
                  <th>Phase 3</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {campaigns.slice(0, 10).map((campaign) => (
                  <tr key={campaign.id}>
                    <td style={{ fontWeight: 600 }}>{campaign.name}</td>
                    <td style={{ fontSize: '12px', color: 'var(--color-text-secondary)' }}>
                      {new Date(campaign.created_at).toLocaleDateString()}
                    </td>
                    <td>{campaign.phase1_done || 0}</td>
                    <td>{campaign.phase2_done || 0}</td>
                    <td>{campaign.phase3_done || 0}</td>
                    <td>
                      <Badge
                        status={campaign.status === 'completed' ? 'success' : campaign.status === 'paused' ? 'warn' : 'pending'}
                        text={campaign.status}
                        className={campaign.status === 'in_progress' ? 'badge-pulse' : ''}
                      />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Recent Errors */}
      {errors.length > 0 && (
        <div className="panel slide-up" style={{ animationDelay: '0.4s' }}>
          <h3 style={{ marginTop: 0, color: 'var(--color-error)', fontSize: '20px', fontWeight: 600 }}>
            ⚠️ Recent Errors
          </h3>
          <div style={{ overflowX: 'auto' }}>
            <table className="table">
              <thead>
                <tr>
                  <th>Phase</th>
                  <th>Type</th>
                  <th>Message</th>
                  <th>Time</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {errors.map((error, idx) => (
                  <tr 
                    key={idx} 
                    style={{ 
                      background: error.resolved ? 'transparent' : 'rgba(244, 63, 94, 0.05)',
                    }}
                  >
                    <td style={{ fontWeight: 600, color: 'var(--color-error)' }}>{error.phase}</td>
                    <td style={{ fontSize: '12px' }}>{error.error_type}</td>
                    <td style={{ fontSize: '12px', maxWidth: '300px', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                      {error.error_message}
                    </td>
                    <td style={{ fontSize: '11px', color: 'var(--color-text-secondary)' }}>
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
        </div>
      )}

      {/* Debug Section */}
      <div className="panel slide-up" style={{ animationDelay: '0.5s' }}>
        <button
          className="btn-secondary"
          onClick={() => setShowDebug(!showDebug)}
          style={{ fontSize: '12px', marginBottom: showDebug ? '16px' : '0' }}
        >
          {showDebug ? '▼ Hide' : '▶ Show'} Raw Metrics (Debug)
        </button>
        {showDebug && (
          <pre className="log fade-in" style={{ fontSize: '11px', maxHeight: '300px', overflowY: 'auto' }}>
            {JSON.stringify(metrics, null, 2)}
          </pre>
        )}
      </div>
    </Layout>
  );
}