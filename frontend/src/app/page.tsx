'use client';

import React from 'react';
import Link from 'next/link';
import { DocumentList } from '@/components/DocumentList';
import { Button, Card } from '@/components/ui';
import { ArrowRight, Zap, Shield, Clock, Download } from 'lucide-react';

export default function Home() {
  return (
    <div className="space-y-12">
      {/* Hero Section */}
      <section className="text-center py-12">
        <div className="space-y-4 mb-8">
          <h1 className="text-4xl sm:text-5xl font-bold text-gray-900 leading-tight">
            Generate Legal Documents
            <span className="text-transparent bg-clip-text bg-gradient-to-r from-primary-600 to-blue-600">
              {' '}with AI
            </span>
          </h1>
          <p className="text-xl text-gray-600 max-w-2xl mx-auto">
            Create professional prenuptial agreements and divorce settlements instantly. Customized for your needs.
          </p>
        </div>

        <Link href="/generate">
          <Button variant="primary" size="lg" className="inline-flex items-center gap-2">
            Get Started <ArrowRight className="w-5 h-5" />
          </Button>
        </Link>
      </section>

      {/* Features Section */}
      <section className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {[
          {
            icon: <Zap className="w-6 h-6" />,
            title: 'Lightning Fast',
            description: 'Generate documents in seconds, not hours',
          },
          {
            icon: <Shield className="w-6 h-6" />,
            title: 'Secure & Private',
            description: 'Your data is encrypted and protected',
          },
          {
            icon: <Clock className="w-6 h-6" />,
            title: 'Real-time Tracking',
            description: 'Monitor document generation status',
          },
          {
            icon: <Download className="w-6 h-6" />,
            title: 'Multiple Formats',
            description: 'Download as PDF, DOCX, or both',
          },
        ].map((feature, idx) => (
          <Card key={idx} className="text-center">
            <div className="flex justify-center mb-3 text-primary-600">{feature.icon}</div>
            <h3 className="font-semibold text-gray-900 mb-1">{feature.title}</h3>
            <p className="text-sm text-gray-600">{feature.description}</p>
          </Card>
        ))}
      </section>

      {/* How It Works */}
      <section className="bg-white rounded-lg border border-gray-200 p-8">
        <h2 className="text-2xl font-bold text-gray-900 mb-8 text-center">How It Works</h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
          {[
            {
              step: 1,
              title: 'Fill Information',
              description: 'Enter the names, addresses, and preferences for both parties',
            },
            {
              step: 2,
              title: 'Generate Document',
              description: 'Click generate and our AI creates a customized legal document',
            },
            {
              step: 3,
              title: 'Download & Use',
              description: 'Download in your preferred format and use with your attorney',
            },
          ].map((item, idx) => (
            <div key={idx} className="text-center">
              <div className="w-10 h-10 rounded-full bg-primary-100 flex items-center justify-center mx-auto mb-4">
                <span className="text-primary-600 font-bold">{item.step}</span>
              </div>
              <h3 className="font-semibold text-gray-900 mb-2">{item.title}</h3>
              <p className="text-gray-600 text-sm">{item.description}</p>
            </div>
          ))}
        </div>
      </section>

      {/* Recent Documents */}
      <section>
        <h2 className="text-2xl font-bold text-gray-900 mb-6">Recent Documents</h2>
        <DocumentList maxItems={5} />
      </section>

      {/* CTA Section */}
      <section className="bg-gradient-to-r from-primary-600 to-blue-600 rounded-lg p-12 text-white text-center">
        <h2 className="text-3xl font-bold mb-4">Ready to Create Your Document?</h2>
        <p className="text-lg text-blue-100 mb-8 max-w-2xl mx-auto">
          Start generating legal documents today. It only takes a few minutes.
        </p>
        <Link href="/generate">
          <Button
            variant="primary"
            size="lg"
            className="inline-flex items-center gap-2 bg-white text-primary-600 hover:bg-gray-100"
          >
            Generate Now <ArrowRight className="w-5 h-5" />
          </Button>
        </Link>
      </section>
    </div>
  );
}
