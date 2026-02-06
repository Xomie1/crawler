import { ReactNode } from 'react';

interface CardProps {
  title: string;
  value: string | number;
  label?: string;
  icon?: string;
  trend?: number;
  className?: string;
}

export default function Card({ title, value, label, icon, trend, className = '' }: CardProps) {
  const trendColor = trend !== undefined ? (trend > 0 ? 'var(--color-success)' : 'var(--color-error)') : undefined;
  const trendIcon = trend !== undefined ? (trend > 0 ? '↑' : '↓') : null;

  return (
    <div className={`metric-card ${className}`}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
        {icon && <span style={{ fontSize: '24px' }}>{icon}</span>}
        <h3 className="metric-card-title">{title}</h3>
      </div>
      <p className="metric-card-value">{value}</p>
      {label && <p className="metric-card-label">{label}</p>}
      {trend !== undefined && (
        <p className="metric-card-label" style={{ color: trendColor, marginTop: '6px' }}>
          {trendIcon} {Math.abs(trend)}% from last period
        </p>
      )}
    </div>
  );
}
