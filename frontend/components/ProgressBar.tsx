interface ProgressBarProps {
  done: number;
  total: number;
  label?: string;
  showPercentage?: boolean;
  className?: string;
}

export default function ProgressBar({ done, total, label, showPercentage = true, className = '' }: ProgressBarProps) {
  const percentage = total > 0 ? Math.round((done / total) * 100) : 0;
  
  return (
    <div style={{ marginBottom: '16px' }}>
      {(label || showPercentage) && (
        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '6px', fontSize: '12px' }}>
          <span style={{ color: 'var(--color-text-secondary)' }}>{label}</span>
          {showPercentage && (
            <span style={{ color: 'var(--color-text-secondary)', fontWeight: 600 }}>
              {done} / {total} ({percentage}%)
            </span>
          )}
        </div>
      )}
      <div className="progress-bar">
        <div
          className="progress-bar-fill"
          style={{ width: `${percentage}%` }}
        />
      </div>
    </div>
  );
}
