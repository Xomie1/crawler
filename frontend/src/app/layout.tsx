import type { Metadata } from 'next';
import './globals.css';
import { UserProvider } from '@/context/UserContext';
import Link from 'next/link';
import { FileText, History, Home, Globe, Mail, FormInput } from 'lucide-react';

export const metadata: Metadata = {
  title: 'Document Generator',
  description: 'Generate legal documents with ease',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="bg-gray-50">
        <UserProvider>
          <div className="min-h-screen flex flex-col">
            {/* Header */}
            <header className="bg-white border-b border-gray-200 sticky top-0 z-50">
              <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
                <div className="flex justify-between items-center">
                  <Link href="/" className="flex items-center gap-2 group">
                    <div className="w-8 h-8 bg-primary-600 rounded-lg flex items-center justify-center group-hover:bg-primary-700 transition">
                      <FileText className="w-5 h-5 text-white" />
                    </div>
                    <span className="text-xl font-bold text-gray-900">DocGen</span>
                  </Link>

                  {/* Navigation */}
                  <nav className="flex gap-1">
                    <Link
                      href="/"
                      className="flex items-center gap-2 px-4 py-2 rounded-lg hover:bg-gray-100 text-gray-700 font-medium transition"
                    >
                      <Home className="w-4 h-4" />
                      <span className="hidden sm:inline">Home</span>
                    </Link>
                    <Link
                      href="/crawl"
                      className="flex items-center gap-2 px-4 py-2 rounded-lg hover:bg-gray-100 text-gray-700 font-medium transition"
                    >
                      <Globe className="w-4 h-4" />
                      <span className="hidden sm:inline">Crawl</span>
                    </Link>
                    <Link
                      href="/email"
                      className="flex items-center gap-2 px-4 py-2 rounded-lg hover:bg-gray-100 text-gray-700 font-medium transition"
                    >
                      <Mail className="w-4 h-4" />
                      <span className="hidden sm:inline">Email</span>
                    </Link>
                    <Link
                      href="/forms"
                      className="flex items-center gap-2 px-4 py-2 rounded-lg hover:bg-gray-100 text-gray-700 font-medium transition"
                    >
                      <FormInput className="w-4 h-4" />
                      <span className="hidden sm:inline">Forms</span>
                    </Link>
                    <Link
                      href="/generate"
                      className="flex items-center gap-2 px-4 py-2 rounded-lg hover:bg-gray-100 text-gray-700 font-medium transition"
                    >
                      <FileText className="w-4 h-4" />
                      <span className="hidden sm:inline">Generate</span>
                    </Link>
                    <Link
                      href="/history"
                      className="flex items-center gap-2 px-4 py-2 rounded-lg hover:bg-gray-100 text-gray-700 font-medium transition"
                    >
                      <History className="w-4 h-4" />
                      <span className="hidden sm:inline">History</span>
                    </Link>
                  </nav>

                  {/* User ID */}
                  <div className="text-xs text-gray-500 max-w-[150px] truncate hidden sm:block">
                    User ID: <span className="font-mono">{process.env.NODE_ENV}</span>
                  </div>
                </div>
              </div>
            </header>

            {/* Main Content */}
            <main className="flex-1 max-w-6xl w-full mx-auto px-4 sm:px-6 lg:px-8 py-8">
              {children}
            </main>

            {/* Footer */}
            <footer className="bg-gray-900 text-gray-300 mt-12">
              <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
                <div className="grid grid-cols-1 md:grid-cols-3 gap-8 mb-8">
                  <div>
                    <h3 className="text-white font-semibold mb-3">DocGen</h3>
                    <p className="text-sm">
                      Professional legal document generation powered by AI
                    </p>
                  </div>
                  <div>
                    <h3 className="text-white font-semibold mb-3">Quick Links</h3>
                    <ul className="space-y-2 text-sm">
                      <li>
                        <Link href="/" className="hover:text-white transition">
                          Home
                        </Link>
                      </li>
                      <li>
                        <Link href="/generate" className="hover:text-white transition">
                          Generate Document
                        </Link>
                      </li>
                      <li>
                        <Link href="/history" className="hover:text-white transition">
                          Document History
                        </Link>
                      </li>
                    </ul>
                  </div>
                  <div>
                    <h3 className="text-white font-semibold mb-3">Support</h3>
                    <ul className="space-y-2 text-sm">
                      <li>
                        <a href="#" className="hover:text-white transition">
                          Documentation
                        </a>
                      </li>
                      <li>
                        <a href="#" className="hover:text-white transition">
                          FAQ
                        </a>
                      </li>
                      <li>
                        <a href="#" className="hover:text-white transition">
                          Contact Support
                        </a>
                      </li>
                    </ul>
                  </div>
                </div>
                <div className="border-t border-gray-800 pt-8 flex justify-between items-center text-sm">
                  <p>&copy; 2024 DocGen. All rights reserved.</p>
                  <div className="flex gap-4">
                    <a href="#" className="hover:text-white transition">
                      Privacy Policy
                    </a>
                    <a href="#" className="hover:text-white transition">
                      Terms of Service
                    </a>
                  </div>
                </div>
              </div>
            </footer>
          </div>
        </UserProvider>
      </body>
    </html>
  );
}
