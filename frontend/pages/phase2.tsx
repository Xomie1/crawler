import { useState } from 'react';
import Layout from '../components/Layout';
import Badge from '../components/Badge';
import FileUpload from '../components/FileUpload';
import { api } from '../lib/api';

export default function Phase2() {
  const [useFileUpload, setUseFileUpload] = useState(true);
  const [filePath, setFilePath] = useState('');
  const [uploadedFile, setUploadedFile] = useState<File | null>(null);
  const [subjectTemplate, setSubjectTemplate] = useState('{company_name}様へのご提案');
  const [messageTemplate, setMessageTemplate] = useState(
    'こんにちは、\n\n株式会社{company_name}のサービスについてお問い合わせさせていただきました。\n\nよろしくお願いいたします。'
  );
  const [testRecipient, setTestRecipient] = useState('');
  const [dryRun, setDryRun] = useState(true);
  const [result, setResult] = useState<any | null>(null);
  const [loading, setLoading] = useState(false);

  async function runCampaign() {
    if (useFileUpload && !uploadedFile) {
      alert('Please upload a file');
      return;
    }
    if (!useFileUpload && !filePath.trim()) {
      alert('Please enter a file path');
      return;
    }

    try {
      setLoading(true);
      let data;
      if (useFileUpload) {
        data = await api.emailFromFile(uploadedFile!, subjectTemplate, messageTemplate, dryRun, testRecipient);
      } else {
        // Legacy: use old endpoint
        data = await api.runEmailCampaign(filePath.trim(), dryRun);
      }
      setResult(data);
    } catch (err) {
      console.error(err);
      alert('Campaign failed: ' + String(err));
    } finally {
      setLoading(false);
    }
  }

  return (
    <Layout>
      <div className="panel">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div>
            <h2 style={{ margin: '0 0 8px 0' }}>Phase 2 – Email Campaign</h2>
            <p style={{ margin: 0, fontSize: '14px', color: 'var(--color-text-secondary)' }}>
              Build and launch targeted email campaigns
            </p>
          </div>
          <Badge status={dryRun ? 'warn' : 'error'} text={dryRun ? '⚪ DRY-RUN' : '🔴 LIVE'} />
        </div>
      </div>

      {/* Campaign Builder */}
      <div className="panel">
        <h3 style={{ marginTop: 0 }}>Campaign Settings</h3>

        {/* Safety Warning */}
        {!dryRun && (
          <div style={{
            backgroundColor: 'rgba(239, 68, 68, 0.1)',
            border: '2px solid var(--color-error)',
            borderRadius: '8px',
            padding: '12px',
            marginBottom: '20px',
            color: 'var(--color-error)',
            fontSize: '13px',
          }}>
            <strong>⚠️ LIVE MODE ENABLED</strong>
            <p style={{ margin: '4px 0 0 0' }}>
              Emails will be sent to all recipients. This cannot be undone. Ensure templates are correct.
            </p>
          </div>
        )}

        {/* Input Method Toggle */}
        <div style={{ marginBottom: '20px' }}>
          <label style={{ display: 'flex', alignItems: 'center', gap: '12px', cursor: 'pointer', marginBottom: '12px' }}>
            <input
              type="radio"
              checked={useFileUpload}
              onChange={() => setUseFileUpload(true)}
              style={{ cursor: 'pointer' }}
            />
            <span>Upload file (XLSX, CSV, JSONL)</span>
          </label>
          <label style={{ display: 'flex', alignItems: 'center', gap: '12px', cursor: 'pointer' }}>
            <input
              type="radio"
              checked={!useFileUpload}
              onChange={() => setUseFileUpload(false)}
              style={{ cursor: 'pointer' }}
            />
            <span>File path (Legacy)</span>
          </label>
        </div>

        {/* File Input */}
        {useFileUpload ? (
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
              File path
            </label>
            <input
              className="input"
              placeholder="crawl_results.jsonl"
              value={filePath}
              onChange={e => setFilePath(e.target.value)}
            />
          </div>
        )}

        {/* Subject Template */}
        <div style={{ marginBottom: '20px' }}>
          <label style={{ display: 'block', fontSize: '14px', fontWeight: 600, marginBottom: '8px' }}>
            Subject Template
          </label>
          <p style={{ margin: '0 0 8px 0', fontSize: '12px', color: 'var(--color-text-secondary)' }}>
            Use {`{company_name}`}, {`{website_url}`} as placeholders
          </p>
          <input
            className="input"
            value={subjectTemplate}
            onChange={e => setSubjectTemplate(e.target.value)}
            placeholder="Your subject here"
          />
        </div>

        {/* Message Template */}
        <div style={{ marginBottom: '20px' }}>
          <label style={{ display: 'block', fontSize: '14px', fontWeight: 600, marginBottom: '8px' }}>
            Message Body
          </label>
          <p style={{ margin: '0 0 8px 0', fontSize: '12px', color: 'var(--color-text-secondary)' }}>
            Use {`{company_name}`}, {`{website_url}`} as placeholders
          </p>
          <textarea
            className="input"
            rows={8}
            value={messageTemplate}
            onChange={e => setMessageTemplate(e.target.value)}
            placeholder="Your message here"
          />
        </div>

        {/* Test Recipient */}
        <div style={{ marginBottom: '20px' }}>
          <label style={{ display: 'block', fontSize: '14px', fontWeight: 600, marginBottom: '8px' }}>
            Test Recipient (optional)
          </label>
          <p style={{ margin: '0 0 8px 0', fontSize: '12px', color: 'var(--color-text-secondary)' }}>
            Send a test to this email before bulk sending
          </p>
          <input
            className="input"
            type="email"
            value={testRecipient}
            onChange={e => setTestRecipient(e.target.value)}
            placeholder="test@example.com"
          />
        </div>

        {/* Mode Toggle */}
        <div style={{ marginBottom: '20px', padding: '16px', background: 'rgba(245, 158, 11, 0.1)', borderRadius: '8px', border: '1px solid rgba(245, 158, 11, 0.2)' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '12px' }}>
            <div>
              <h4 style={{ margin: '0 0 4px 0' }}>Send Mode</h4>
              <p style={{ margin: 0, fontSize: '12px', color: 'var(--color-text-secondary)' }}>
                {dryRun ? '✓ Test mode: Simulate without sending' : '🔴 Production: Will send to real recipients'}
              </p>
            </div>
            <Badge status={dryRun ? 'warn' : 'error'} text={dryRun ? '🧪 TEST' : '🚀 LIVE'} />
          </div>
          <label style={{ display: 'flex', alignItems: 'center', gap: '8px', cursor: 'pointer', margin: 0 }}>
            <input
              type="checkbox"
              checked={dryRun}
              onChange={e => setDryRun(e.target.checked)}
              style={{ cursor: 'pointer', width: '18px', height: '18px' }}
            />
            <span style={{ fontWeight: 500 }}>
              Enable Dry Run mode (recommended for testing)
            </span>
          </label>
          {!dryRun && (
            <div style={{ marginTop: '12px', padding: '12px', backgroundColor: 'rgba(239, 68, 68, 0.1)', borderRadius: '6px', border: '1px solid rgba(239, 68, 68, 0.3)' }}>
              <p style={{ margin: 0, fontSize: '13px', color: 'var(--color-error)', fontWeight: 500 }}>
                ⚠️ <strong>LIVE MODE ENABLED</strong><br/>
                <span style={{ fontSize: '12px', fontWeight: 'normal' }}>Emails will be sent to actual recipients. Review campaign details before proceeding.</span>
              </p>
            </div>
          )}
        </div>

        {/* Action Buttons */}
        <div style={{ display: 'flex', gap: '12px' }}>
          <button
            className="btn-primary"
            onClick={runCampaign}
            disabled={loading || (useFileUpload && !uploadedFile) || (!useFileUpload && !filePath.trim())}
          >
            {loading ? '...' : '→ Launch Campaign'}
          </button>
        </div>
      </div>

      {/* Results */}
      {result && (
        <div className="panel">
          <h3 style={{ marginTop: 0 }}>Campaign Result</h3>
          <div style={{ padding: '12px', background: 'rgba(16, 185, 129, 0.1)', borderRadius: '8px', marginBottom: '16px' }}>
            <p style={{ margin: '0 0 8px 0', fontWeight: 600, color: 'var(--color-success)' }}>
              ✓ Campaign {result.status || 'completed'}
            </p>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(120px, 1fr))', gap: '12px' }}>
              {result.sent && <div><strong>Sent:</strong> {result.sent}</div>}
              {result.skipped && <div><strong>Skipped:</strong> {result.skipped}</div>}
              {result.failed && <div><strong>Failed:</strong> {result.failed}</div>}
            </div>
          </div>
          <details style={{ cursor: 'pointer' }}>
            <summary style={{ fontWeight: 600, color: 'var(--color-text-secondary)', marginBottom: '8px' }}>
              Raw Response (click to expand)
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

