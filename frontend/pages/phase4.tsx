import { useState } from 'react';
import Layout from '../components/Layout';
import { api } from '../lib/api';

export default function Phase4() {
  const [docType, setDocType] = useState<'prenuptial' | 'divorce'>('prenuptial');
  const [p1, setP1] = useState({ name: '', address: '', role: 'husband' });
  const [p2, setP2] = useState({ name: '', address: '', role: 'wife' });
  const [options, setOptions] = useState({
    property_separation: false,
    alimony: false,
    children: false,
  });
  const [result, setResult] = useState<any | null>(null);

  async function generate() {
    try {
      const payload = {
        document_type: docType,
        parties: [
          { name: p1.name, address: p1.address, role: p1.role },
          { name: p2.name, address: p2.address, role: p2.role },
        ],
        options,
        custom_values: {},
      };
      const data = await api.generatePdf(payload);
      setResult(data);
    } catch (error) {
      console.error('PDF generation failed:', error);
      alert('PDF generation failed: ' + String(error));
    }
  }

  return (
    <Layout>
      <div className="panel">
        <h2>Phase 4 – PDF Generation</h2>
        <p>Generate prenuptial or divorce documents using the PDFdocsEngine.</p>
        <label>
          Document type:
          <select
            className="input"
            value={docType}
            onChange={e => setDocType(e.target.value as 'prenuptial' | 'divorce')}
          >
            <option value="prenuptial">Prenuptial (婚前契約書)</option>
            <option value="divorce">Divorce (離婚協議書)</option>
          </select>
        </label>
        <div className="grid-2">
          <div>
            <h3>Party 1</h3>
            <input
              className="input"
              placeholder="Name"
              value={p1.name}
              onChange={e => setP1({ ...p1, name: e.target.value })}
            />
            <input
              className="input"
              placeholder="Address"
              value={p1.address}
              onChange={e => setP1({ ...p1, address: e.target.value })}
            />
            <input
              className="input"
              placeholder="Role (husband / wife / spouse)"
              value={p1.role}
              onChange={e => setP1({ ...p1, role: e.target.value })}
            />
          </div>
          <div>
            <h3>Party 2</h3>
            <input
              className="input"
              placeholder="Name"
              value={p2.name}
              onChange={e => setP2({ ...p2, name: e.target.value })}
            />
            <input
              className="input"
              placeholder="Address"
              value={p2.address}
              onChange={e => setP2({ ...p2, address: e.target.value })}
            />
            <input
              className="input"
              placeholder="Role (husband / wife / spouse)"
              value={p2.role}
              onChange={e => setP2({ ...p2, role: e.target.value })}
            />
          </div>
        </div>
        <h3>Options</h3>
        <label>
          <input
            type="checkbox"
            checked={options.property_separation}
            onChange={e => setOptions({ ...options, property_separation: e.target.checked })}
          />{' '}
          Property separation clause
        </label>
        <br />
        <label>
          <input
            type="checkbox"
            checked={options.alimony}
            onChange={e => setOptions({ ...options, alimony: e.target.checked })}
          />{' '}
        Alimony / support clause
        </label>
        <br />
        <label>
          <input
            type="checkbox"
            checked={options.children}
            onChange={e => setOptions({ ...options, children: e.target.checked })}
          />{' '}
          Children-related clauses
        </label>
        <div style={{ marginTop: 8 }}>
          <button className="btn-primary" onClick={generate}>
            Enqueue PDF Job
          </button>
        </div>
        <pre className="log">{result ? JSON.stringify(result, null, 2) : 'No job yet.'}</pre>
      </div>
    </Layout>
  );
}