import Link from 'next/link';
import { useRouter } from 'next/router';
import { ReactNode } from 'react';
import ThemeToggle from './ThemeToggle';

interface Props {
  children: ReactNode;
}

export default function Layout({ children }: Props) {
  const router = useRouter();
  
  const navItems = [
    { href: '/', label: 'Dashboard', icon: '📊' },
    { href: '/phase1', label: 'Phase 1 – Crawl', icon: '🕷️' },
    { href: '/phase2', label: 'Phase 2 – Email', icon: '📧' },
    { href: '/phase3', label: 'Phase 3 – Forms', icon: '📝' },
    { href: '/phase4', label: 'Phase 4 – PDF', icon: '📄' },
    { href: '/phase5', label: 'Phase 5 – Metrics', icon: '📈' },
  ];
  
  return (
    <div className="layout">
      <aside className="sidebar">
        <h1>🚀 Crawler Console</h1>
        <nav className="nav">
          {navItems.map((item) => (
            <Link
              key={item.href}
              href={item.href}
              className={router.pathname === item.href ? 'active' : ''}
            >
              <span style={{ marginRight: '8px' }}>{item.icon}</span>
              {item.label}
            </Link>
          ))}
          <hr style={{ margin: '16px 0', borderColor: 'var(--color-border)', opacity: 0.5 }} />
          <Link
            href="/errors"
            className={router.pathname === '/errors' ? 'active' : ''}
            style={{ color: router.pathname === '/errors' ? 'white' : 'var(--color-error)' }}
          >
            <span style={{ marginRight: '8px' }}>⚠️</span>
            Error Log
          </Link>
        </nav>
      </aside>
      <main className="main">
        {children}
        <ThemeToggle />
      </main>
    </div>
  );
}