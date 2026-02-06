import { useEffect, useState } from 'react';
import Layout from '../components/Layout';
import Badge from '../components/Badge';
import FileUpload from '../components/FileUpload';
import ProgressBar from '../components/ProgressBar';
import { api } from '../lib/api';

export default function Phase1() {
  const [urlsText, setUrlsText] = useState('');
  const [enqueueResult, setEnqueueResult] = useState<any | null>(null);
  const [crawls, setCrawls] = useState<any[]>([]);
  const [uploadLoading, setUploadLoading] = useState(false);
  const [uploadResult, setUploadResult] = useState<any | null>(null);

  async function enqueue() {
    try {
      const urls = urlsText
        .split('\n')
        .map(x => x.trim())
        .filter(Boolean);
      if (!urls.length) return;
      const res = await api.enqueueCrawls(urls);
      setEnqueueResult(res);
    } catch (error) {
      console.error('Enqueue failed:', error);
      alert('Failed to enqueue crawls: ' + String(error));
    }
  }

  async function handleFileUpload(file: File) {
    try {
      setUploadLoading(true);
      const res = await api.enqueueUpload(file);
      setUploadResult(res);
      setEnqueueResult(res);
      await loadCrawls();
    } catch (err) {
      console.error(err);
      alert('Upload failed: ' + String(err));
    } finally {
      setUploadLoading(false);
    }
  }

  async function loadCrawls() {
    const data = await api.getCrawls();
    setCrawls(data?.results || []);
  }

  useEffect(() => {
    loadCrawls();
  }, []);

  const successCount = crawls.filter(r => r.crawl_status === 'success').length;
  const errorCount = crawls.filter(r => r.crawl_status === 'error').length;

  return (
    <Layout>
      <div className="panel">
        <h2 style={{ margin: '0 0 8px 0' }}>Phase 1 – Crawl</h2>
        <p style={{ margin: 0, fontSize: '14px', color: 'var(--color-text-secondary)' }}>
          Extract company information from websites
        </p>
      </div>

      {/* File Upload */}
      <div className="panel">
        <h3 style={{ marginTop: 0 }}>Quick Upload</h3>
        <p style={{ color: 'var(--color-text-secondary)', fontSize: '14px', marginBottom: '16px' }}>
          Upload a file with URLs (Excel, CSV, or JSONL)
        </p>
        <FileUpload
          onFile={handleFileUpload}
          accept=".xlsx,.csv,.jsonl,.xls"
          maxSize={10485760}
          label="Choose file or drag here"
          isLoading={uploadLoading}
        />
        {uploadResult && (
          <div style={{ marginTop: '16px', padding: '12px', background: 'rgba(16, 185, 129, 0.1)', borderRadius: '8px' }}>
            <p style={{ margin: '0 0 8px 0', fontWeight: 600, color: 'var(--color-success)' }}>
              ✓ Upload successful
            </p>
            <p style={{ margin: '0 0 4px 0', fontSize: '14px' }}>
              <strong>Queued:</strong> {uploadResult.queued} URLs
            </p>
            <p style={{ margin: '0', fontSize: '14px' }}>
              <strong>Total parsed:</strong> {uploadResult.parsed_count || uploadResult.queued}
            </p>
          </div>
        )}
      </div>

      {/* Manual Input */}
      <div className="panel">
        <h3 style={{ marginTop: 0 }}>Or Paste URLs</h3>
        <p style={{ color: 'var(--color-text-secondary)', fontSize: '14px', marginBottom: '8px' }}>
          One URL per line
        </p>
        <textarea
          className="input"
          rows={6}
          value={urlsText}
          onChange={e => setUrlsText(e.target.value)}
          placeholder="https://example.com&#10;https://another.com&#10;https://company.jp"
        />
        <div style={{ display: 'flex', gap: '12px' }}>
          <button className="btn-primary" onClick={enqueue}>
            → Enqueue Crawls
          </button>
          <button className="btn-secondary" onClick={loadCrawls}>
            🔄 Refresh Results
          </button>
        </div>
        {enqueueResult && (
          <div style={{ marginTop: '12px', padding: '12px', background: 'rgba(14, 165, 233, 0.1)', borderRadius: '8px' }}>
            <p style={{ margin: '0 0 8px 0', fontWeight: 600, color: 'var(--color-primary)' }}>
              ✓ Enqueued {enqueueResult.queued} jobs
            </p>
            <p style={{ margin: 0, fontSize: '12px', color: 'var(--color-text-secondary)' }}>
              Track progress in the Results section below
            </p>
          </div>
        )}
      </div>

      {/* Results Summary */}
      <div className="panel">
        <h3 style={{ marginTop: 0 }}>Results Summary</h3>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', gap: '16px', marginBottom: '16px' }}>
          <div>
            <p style={{ margin: '0 0 8px 0', fontSize: '12px', color: 'var(--color-text-secondary)' }}>Total Crawled</p>
            <p style={{ margin: 0, fontSize: '24px', fontWeight: 700 }}>{crawls.length}</p>
          </div>
          <div>
            <p style={{ margin: '0 0 8px 0', fontSize: '12px', color: 'var(--color-text-secondary)' }}>Success</p>
            <p style={{ margin: 0, fontSize: '24px', fontWeight: 700, color: 'var(--color-success)' }}>{successCount}</p>
          </div>
          <div>
            <p style={{ margin: '0 0 8px 0', fontSize: '12px', color: 'var(--color-text-secondary)' }}>Failed</p>
            <p style={{ margin: 0, fontSize: '24px', fontWeight: 700, color: 'var(--color-error)' }}>{errorCount}</p>
          </div>
          <div>
            <p style={{ margin: '0 0 8px 0', fontSize: '12px', color: 'var(--color-text-secondary)' }}>Success Rate</p>
            <p style={{ margin: 0, fontSize: '24px', fontWeight: 700, color: 'var(--color-success)' }}>
              {crawls.length > 0 ? Math.round((successCount / crawls.length) * 100) : 0}%
            </p>
          </div>
        </div>
        {crawls.length > 0 && (
          <ProgressBar done={successCount} total={crawls.length} label="Success Rate" />
        )}
      </div>

      {/* Results Table */}
      <div className="panel">
        <h3 style={{ marginTop: 0 }}>Latest Results ({crawls.length})</h3>
        <div style={{ overflowX: 'auto' }}>
          <table className="table">
            <thead>
              <tr>
                <th>URL</th>
                <th>Company</th>
                <th>Email</th>
                <th>Form</th>
                <th>HTTP</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {crawls.map(r => (
                <tr key={r.id || r.url}>
                  <td style={{ fontSize: '12px', maxWidth: '200px', wordBreak: 'break-word' }}>{r.url}</td>
                  <td>{r.company_name || '—'}</td>
                  <td style={{ fontSize: '12px' }}>{r.email || '—'}</td>
                  <td style={{ fontSize: '12px' }}>
                    {r.inquiry_form_url ? (
                      <a href={r.inquiry_form_url} target="_blank" rel="noopener noreferrer" style={{ color: 'var(--color-primary)' }}>
                        View
                      </a>
                    ) : (
                      '—'
                    )}
                  </td>
                  <td>{r.http_status || '—'}</td>
                  <td>
                    <Badge
                      status={r.crawl_status === 'success' ? 'success' : 'error'}
                      text={r.crawl_status}
                    />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </Layout>
  );
}