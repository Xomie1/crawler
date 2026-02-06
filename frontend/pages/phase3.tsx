import { useState } from 'react';
import Layout from '../components/Layout';
import Badge from '../components/Badge';
import FileUpload from '../components/FileUpload';
import { api } from '../lib/api';

export default function Phase3() {
  const [inputMethod, setInputMethod] = useState<'db' | 'file'>('db');
  const [uploadedFile, setUploadedFile] = useState<File | null>(null);
  const [messageChoice, setMessageChoice] = useState<'reuse_email' | 'custom'>('reuse_email');
  const [customMessage, setCustomMessage] = useState('');
  const [limit, setLimit] = useState(50);
  const [result, setResult] = useState<any | null>(null);
  const [loading, setLoading] = useState(false);

  async function runForms() {
    if (inputMethod === 'file' && !uploadedFile) {
      alert('Please upload a file');
      return;
    }

    try {
      setLoading(true);
      let data;
      if (inputMethod === 'file') {
        data = await api.formsFromFile(uploadedFile!, messageChoice, messageChoice === 'custom' ? customMessage : undefined);
      } else {
        data = await api.submitForms(limit);
      }
      setResult(data);
    } catch (err) {
      console.error(err);
      alert('Form submission failed: ' + String(err));
    } finally {
      setLoading(false);
    }
  }

  return (
    <Layout>
      <div className="panel">
        <h2 style={{ margin: '0 0 8px 0' }}>Phase 3 – Form Submissions</h2>
        <p style={{ margin: 0, fontSize: '14px', color: 'var(--color-text-secondary)' }}>
          Automatically submit inquiry forms to collected companies
        </p>
      </div>

      <div className="panel">
        <h3 style={{ marginTop: 0 }}>Configuration</h3>

        {/* Safety Warning */}
        <div style={{
          backgroundColor: 'rgba(249, 115, 22, 0.1)',
          border: '2px solid var(--color-warn)',
          borderRadius: '8px',
          padding: '12px',
          marginBottom: '20px',
          color: 'var(--color-warn)',
          fontSize: '13px',
        }}>
          <strong>⚠️ RATE LIMITED</strong>
          <p style={{ margin: '4px 0 0 0' }}>
            Form submissions are rate-limited to 2 per domain per 5 seconds to avoid blocking.
          </p>
        </div>

        {/* Input Method */}
        <div style={{ marginBottom: '20px' }}>
          <label style={{ fontSize: '14px', fontWeight: 600, display: 'block', marginBottom: '12px' }}>
            Data Source
          </label>
          <label style={{ display: 'flex', alignItems: 'center', gap: '12px', cursor: 'pointer', marginBottom: '12px' }}>
            <input
              type="radio"
              checked={inputMethod === 'db'}
              onChange={() => setInputMethod('db')}
              style={{ cursor: 'pointer' }}
            />
            <span>Use crawl results from database</span>
          </label>
          <label style={{ display: 'flex', alignItems: 'center', gap: '12px', cursor: 'pointer' }}>
            <input
              type="radio"
              checked={inputMethod === 'file'}
              onChange={() => setInputMethod('file')}
              style={{ cursor: 'pointer' }}
            />
            <span>Upload file (XLSX, CSV, JSONL)</span>
          </label>
        </div>

        {/* File Upload or Limit */}
        {inputMethod === 'file' ? (
          <div style={{ marginBottom: '20px' }}>
            <label style={{ display: 'block', fontSize: '14px', fontWeight: 600, marginBottom: '8px' }}>
              Select crawl results file
            </label>
            <FileUpload
              onFile={(file) => setUploadedFile(file)}
              accept=".xlsx,.csv,.jsonl"
              label="Upload or drag file here"
            />
          </div>
        ) : (
          <div style={{ marginBottom: '20px' }}>
            <label style={{ display: 'block', fontSize: '14px', fontWeight: 600, marginBottom: '8px' }}>
              Maximum records to process
            </label>
            <input
              type="number"
              className="input"
              value={limit}
              onChange={e => setLimit(Number(e.target.value) || 0)}
            />
          </div>
        )}

        {/* Message Choice */}
        <div style={{ marginBottom: '20px' }}>
          <label style={{ fontSize: '14px', fontWeight: 600, display: 'block', marginBottom: '12px' }}>
            Message
          </label>
          <label style={{ display: 'flex', alignItems: 'center', gap: '12px', cursor: 'pointer', marginBottom: '12px' }}>
            <input
              type="radio"
              checked={messageChoice === 'reuse_email'}
              onChange={() => setMessageChoice('reuse_email')}
              style={{ cursor: 'pointer' }}
            />
            <span>Reuse Phase 2 email campaign message</span>
          </label>
          <label style={{ display: 'flex', alignItems: 'center', gap: '12px', cursor: 'pointer' }}>
            <input
              type="radio"
              checked={messageChoice === 'custom'}
              onChange={() => setMessageChoice('custom')}
              style={{ cursor: 'pointer' }}
            />
            <span>Use custom form message</span>
          </label>
        </div>

        {/* Custom Message */}
        {messageChoice === 'custom' && (
          <div style={{ marginBottom: '20px' }}>
            <label style={{ display: 'block', fontSize: '14px', fontWeight: 600, marginBottom: '8px' }}>
              Custom Message
            </label>
            <p style={{ margin: '0 0 8px 0', fontSize: '12px', color: 'var(--color-text-secondary)' }}>
              Use {`{company_name}`} as placeholder
            </p>
            <textarea
              className="input"
              rows={6}
              value={customMessage}
              onChange={e => setCustomMessage(e.target.value)}
              placeholder="Your custom message here"
            />
          </div>
        )}

        {/* Action Button */}
        <button
          className="btn-primary"
          onClick={runForms}
          disabled={loading || (inputMethod === 'file' && !uploadedFile)}
        >
          {loading ? '...' : '→ Submit Forms'}
        </button>
      </div>

      {/* Results */}
      {result && (
        <div className="panel">
          <h3 style={{ marginTop: 0 }}>Submission Results</h3>
          <div style={{ padding: '12px', background: 'rgba(16, 185, 129, 0.1)', borderRadius: '8px', marginBottom: '16px' }}>
            <p style={{ margin: '0 0 8px 0', fontWeight: 600, color: 'var(--color-success)' }}>
              ✓ Submitted {result.total || result.queued} form(s)
            </p>
            {result.sample && (
              <div style={{ fontSize: '12px', color: 'var(--color-text-secondary)' }}>
                Showing sample of {result.sample.length} results
              </div>
            )}
          </div>
          <details style={{ cursor: 'pointer' }}>
            <summary style={{ fontWeight: 600, color: 'var(--color-text-secondary)', marginBottom: '8px' }}>
              Details (click to expand)
            </summary>
            <pre className="log" style={{ marginTop: '8px', fontSize: '11px' }}>
              {JSON.stringify(result, null, 2)}
            </pre>
          </details>
        </div>
      )}
    </Layout>
  );
}

