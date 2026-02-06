import { useState, useRef } from 'react';
import type React from 'react';
import Spinner from './Spinner';

interface FileUploadProps {
  onFile: (file: File) => void;
  accept?: string;
  maxSize?: number;
  label?: string;
  className?: string;
  isLoading?: boolean;
}

export default function FileUpload({
  onFile,
  accept = '*',
  maxSize = 10485760, // 10MB default
  label = 'Upload file',
  className = '',
  isLoading = false,
}: FileUploadProps) {
  const [isDragging, setIsDragging] = useState(false);
  const [fileName, setFileName] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const handleFileSelect = (file: File) => {
    if (file.size > maxSize) {
      alert(`File is too large. Max size: ${maxSize / 1024 / 1024}MB`);
      return;
    }
    setFileName(file.name);
    onFile(file);
  };

  const handleDrop = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setIsDragging(false);
    
    const files = e.dataTransfer.files;
    if (files.length > 0) {
      handleFileSelect(files[0]);
    }
  };

  const handleDragOver = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setIsDragging(true);
  };

  const handleDragLeave = () => {
    setIsDragging(false);
  };

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.currentTarget.files;
    if (files && files.length > 0) {
      handleFileSelect(files[0]);
    }
  };

  return (
    <div className={`file-upload-container ${className}`}>
      <div
        className={`file-upload-zone ${isDragging ? 'dragging' : ''} ${isLoading ? 'loading' : ''}`}
        onDrop={handleDrop}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        onClick={() => !isLoading && fileInputRef.current?.click()}
        style={{
          border: `2px dashed ${isDragging ? 'var(--color-primary)' : 'var(--color-border-light)'}`,
          borderRadius: '16px',
          padding: '48px 24px',
          textAlign: 'center',
          cursor: isLoading ? 'not-allowed' : 'pointer',
          transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
          background: isDragging 
            ? 'var(--color-surface-hover)' 
            : 'var(--glass-bg)',
          backdropFilter: 'var(--backdrop-blur)',
          boxShadow: isDragging ? `0 0 0 4px ${isDragging ? 'var(--color-primary-glow)' : 'transparent'}` : 'none',
          transform: isDragging ? 'scale(1.02)' : 'scale(1)',
        }}
      >
        <input
          ref={fileInputRef}
          type="file"
          accept={accept}
          onChange={handleInputChange}
          style={{ display: 'none' }}
          disabled={isLoading}
        />
        
        {isLoading ? (
          <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '12px' }}>
            <Spinner size="lg" />
            <p style={{ margin: 0, fontSize: '14px', color: 'var(--color-text-secondary)', fontWeight: 500 }}>
              Processing file...
            </p>
          </div>
        ) : fileName ? (
          <div style={{ textAlign: 'center' }} className="fade-in">
            <div 
              style={{ 
                fontSize: '48px', 
                marginBottom: '12px',
                animation: 'bounce 0.6s ease-in-out',
              }}
            >
              ✓
            </div>
            <p style={{ margin: '0 0 8px 0', fontSize: '15px', fontWeight: 600, color: 'var(--color-success)' }}>
              {fileName}
            </p>
            <p style={{ margin: 0, fontSize: '13px', color: 'var(--color-text-secondary)' }}>
              Click or drag to replace
            </p>
          </div>
        ) : (
          <div style={{ textAlign: 'center' }}>
            <div 
              style={{ 
                fontSize: '56px', 
                marginBottom: '16px',
                filter: 'drop-shadow(0 0 8px var(--color-primary-glow))',
                animation: isDragging ? 'bounce 0.6s ease-in-out infinite' : 'none',
              }}
            >
              📁
            </div>
            <p style={{ margin: '0 0 8px 0', fontSize: '16px', fontWeight: 600, color: 'var(--color-text)' }}>
              {label}
            </p>
            <p style={{ margin: 0, fontSize: '13px', color: 'var(--color-text-secondary)' }}>
              or drag and drop here
            </p>
            <p style={{ margin: '12px 0 0 0', fontSize: '11px', color: 'var(--color-text-secondary)', opacity: 0.7 }}>
              Max size: {maxSize / 1024 / 1024}MB
            </p>
          </div>
        )}
      </div>
    </div>
  );
}