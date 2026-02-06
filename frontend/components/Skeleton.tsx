import React from 'react';

interface SkeletonProps {
  width?: string;
  height?: string;
  count?: number;
  circle?: boolean;
}

export default function Skeleton({ width = '100%', height = '20px', count = 1, circle = false }: SkeletonProps) {
  const skeletons = Array.from({ length: count });

  return (
    <>
      {skeletons.map((_, i) => (
        <div
          key={i}
          style={{
            width,
            height,
            backgroundColor: 'var(--color-surface-alt)',
            borderRadius: circle ? '50%' : '8px',
            animation: 'pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite',
            marginBottom: count > 1 && i < count - 1 ? '12px' : '0',
          }}
        />
      ))}
    </>
  );
}
