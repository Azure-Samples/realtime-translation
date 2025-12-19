"use client";
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { ReactNode, useMemo } from 'react';

/*
  SiteShell conditionally renders a header depending on route patterns.
  - Show header only on home ("/") and interview analysis listing (/interview-analysis) pages.
  - Provide simpler / full-bleed canvas for room, simulate, and dynamic conversation pages.
*/

const headerEligible = (path: string) => {
  if (path === '/') return true;
  if (path === '/interview-analysis') return true;
  return false; // everything else gets no global header
};

export function SiteShell({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const showHeader = useMemo(() => headerEligible(pathname), [pathname]);
  return (
    <div className="min-h-screen flex flex-col">
      {showHeader && (
        <header className="w-full border-b bg-white/70 backdrop-blur sticky top-0 z-20">
          <nav className="mx-auto max-w-6xl flex items-center justify-between px-4 py-3">
            <div className="flex items-center gap-4">
              <Link href="/" className="text-lg font-semibold tracking-tight">Voice Video Chat</Link>
            </div>
            <div className="flex items-center gap-3">
              <Link
                href="/interview-analysis"
                className="inline-flex items-center rounded-md border border-gray-300 bg-white px-3 py-1.5 text-sm font-medium shadow-sm hover:bg-gray-50 transition"
              >Interview Analysis</Link>
            </div>
          </nav>
        </header>
      )}
      <main className={showHeader ? 'flex-1 mx-auto w-full max-w-6xl px-4 py-6' : 'flex-1 w-full'}>
        {children}
      </main>
    </div>
  );
}
