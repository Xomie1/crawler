'use client';

import React, { useState } from 'react';
import { Input, Select, Button, Alert, Card } from './ui';
import { Party, DocumentOptions, GenerateRequest } from '@/lib/api';
import { useDocumentGeneration } from '@/hooks/useDocuments';

interface DocumentFormProps {
  onSubmit?: (jobId: string) => void;
  loading?: boolean;
}

export function DocumentForm({ onSubmit, loading: externalLoading }: DocumentFormProps) {
  const { status, generateDocument } = useDocumentGeneration();
  const [formData, setFormData] = useState({
    documentType: '',
    partyAName: '',
    partyAAddress: '',
    partyBName: '',
    partyBAddress: '',
    propertySeparation: false,
    alimony: false,
    children: false,
  });

  const [validationErrors, setValidationErrors] = useState<Record<string, string>>({});

  const loading = externalLoading || status.status === 'loading';

  const validateForm = (): boolean => {
    const errors: Record<string, string> = {};

    if (!formData.documentType) {
      errors.documentType = 'Document type is required';
    }

    if (!formData.partyAName.trim()) {
      errors.partyAName = 'Party A name is required';
    }

    if (formData.partyAName.trim().length < 2) {
      errors.partyAName = 'Name must be at least 2 characters';
    }

    if (!formData.partyAAddress.trim()) {
      errors.partyAAddress = 'Party A address is required';
    }

    if (formData.partyAAddress.trim().length < 5) {
      errors.partyAAddress = 'Address must be at least 5 characters';
    }

    if (!formData.partyBName.trim()) {
      errors.partyBName = 'Party B name is required';
    }

    if (formData.partyBName.trim().length < 2) {
      errors.partyBName = 'Name must be at least 2 characters';
    }

    if (!formData.partyBAddress.trim()) {
      errors.partyBAddress = 'Party B address is required';
    }

    if (formData.partyBAddress.trim().length < 5) {
      errors.partyBAddress = 'Address must be at least 5 characters';
    }

    setValidationErrors(errors);
    return Object.keys(errors).length === 0;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    if (!validateForm()) {
      return;
    }

    const parties: Party[] = [
      {
        name: formData.partyAName.trim(),
        address: formData.partyAAddress.trim(),
        role: 'party_a',
      },
      {
        name: formData.partyBName.trim(),
        address: formData.partyBAddress.trim(),
        role: 'party_b',
      },
    ];

    const options: DocumentOptions = {
      property_separation: formData.propertySeparation,
      alimony: formData.alimony,
      children: formData.children,
    };

    const request: GenerateRequest = {
      document_type: formData.documentType as 'prenuptial' | 'divorce',
      parties,
      options,
      metadata: {
        user_id: localStorage.getItem('userId') || 'unknown',
      },
    };

    try {
      const jobId = await generateDocument(request);
      if (onSubmit) {
        onSubmit(jobId);
      }
    } catch (error) {
      console.error('Failed to generate document:', error);
    }
  };

  return (
    <form onSubmit={handleSubmit} className="space-y-6">
      {status.status === 'error' && (
        <Alert variant="error" title="Error" message={status.error || 'Unknown error'} onClose={() => {}} />
      )}

      <Card title="Document Type">
        <Select
          label="Document Type"
          value={formData.documentType}
          onChange={(e) => {
            setFormData({ ...formData, documentType: e.target.value });
            if (validationErrors.documentType) {
              setValidationErrors({ ...validationErrors, documentType: '' });
            }
          }}
          error={validationErrors.documentType}
          options={[
            { value: 'prenuptial', label: 'Prenuptial Agreement' },
            { value: 'divorce', label: 'Divorce Settlement' },
          ]}
        />
      </Card>

      <Card title="Party A Information">
        <div className="space-y-4">
          <Input
            label="Full Name"
            placeholder="e.g., John Smith"
            value={formData.partyAName}
            onChange={(e) => {
              setFormData({ ...formData, partyAName: e.target.value });
              if (validationErrors.partyAName) {
                setValidationErrors({ ...validationErrors, partyAName: '' });
              }
            }}
            error={validationErrors.partyAName}
          />
          <Input
            label="Full Address"
            placeholder="e.g., 123 Main Street, New York, NY 10001"
            value={formData.partyAAddress}
            onChange={(e) => {
              setFormData({ ...formData, partyAAddress: e.target.value });
              if (validationErrors.partyAAddress) {
                setValidationErrors({ ...validationErrors, partyAAddress: '' });
              }
            }}
            error={validationErrors.partyAAddress}
          />
        </div>
      </Card>

      <Card title="Party B Information">
        <div className="space-y-4">
          <Input
            label="Full Name"
            placeholder="e.g., Jane Doe"
            value={formData.partyBName}
            onChange={(e) => {
              setFormData({ ...formData, partyBName: e.target.value });
              if (validationErrors.partyBName) {
                setValidationErrors({ ...validationErrors, partyBName: '' });
              }
            }}
            error={validationErrors.partyBName}
          />
          <Input
            label="Full Address"
            placeholder="e.g., 456 Oak Avenue, Los Angeles, CA 90001"
            value={formData.partyBAddress}
            onChange={(e) => {
              setFormData({ ...formData, partyBAddress: e.target.value });
              if (validationErrors.partyBAddress) {
                setValidationErrors({ ...validationErrors, partyBAddress: '' });
              }
            }}
            error={validationErrors.partyBAddress}
          />
        </div>
      </Card>

      <Card title="Document Options">
        <div className="space-y-3">
          <label className="flex items-center gap-3 cursor-pointer">
            <input
              type="checkbox"
              checked={formData.propertySeparation}
              onChange={(e) => setFormData({ ...formData, propertySeparation: e.target.checked })}
              className="w-4 h-4 rounded border-gray-300"
            />
            <span className="text-sm font-medium text-gray-700">Property Separation</span>
          </label>
          <label className="flex items-center gap-3 cursor-pointer">
            <input
              type="checkbox"
              checked={formData.alimony}
              onChange={(e) => setFormData({ ...formData, alimony: e.target.checked })}
              className="w-4 h-4 rounded border-gray-300"
            />
            <span className="text-sm font-medium text-gray-700">Alimony</span>
          </label>
          <label className="flex items-center gap-3 cursor-pointer">
            <input
              type="checkbox"
              checked={formData.children}
              onChange={(e) => setFormData({ ...formData, children: e.target.checked })}
              className="w-4 h-4 rounded border-gray-300"
            />
            <span className="text-sm font-medium text-gray-700">Children Custody</span>
          </label>
        </div>
      </Card>

      <Button type="submit" variant="primary" size="lg" loading={loading} className="w-full">
        {loading ? 'Generating Document...' : 'Generate Document'}
      </Button>
    </form>
  );
}
