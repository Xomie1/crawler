import { useEffect, useState } from 'react';
import Layout from '../components/Layout';
import { api } from '../lib/api';

export default function Phase5() {
  const [queues, setQueues] = useState<any | null>(null);
  const [emailStats, setEmailStats] = useState<any | null>(null);
  const [formStats, setFormStats] = useState<any | null>(null);

  async function refresh() {
    try {
      const [q, e, f] = await Promise.all([
        api.getQueueMetrics().catch(() => null),
        api.getEmailMetrics().catch(() => null),
        api.getFormMetrics().catch(() => null),
      ]);
      setQueues(q);
      setEmailStats(e);
      setFormStats(f);
    } catch (error) {
      console.error('Failed to refresh metrics:', error);
    }
  }

  useEffect(() => {
    refresh();
  }, []);

  return (
    <Layout>
      <div className="panel">
        <h2>Phase 5 – Metrics & Monitoring</h2>
        <p>Queue status and phase statistics for production-style monitoring.</p>
        <button className="btn-secondary" onClick={refresh}>
          Refresh
        </button>
      </div>
      <div className="panel">
        <h3>Queues</h3>
        <pre className="log">{queues ? JSON.stringify(queues, null, 2) : 'Loading...'}</pre>
      </div>
      <div className="panel">
        <h3>Email / Form Stats</h3>
        <pre className="log">
          {emailStats || formStats
            ? JSON.stringify({ email: emailStats, forms: formStats }, null, 2)
            : 'Loading...'}
        </pre>
      </div>
    </Layout>
  );
}