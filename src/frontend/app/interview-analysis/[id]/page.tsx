"use client";
import { useEffect, useState } from 'react';
import { useParams } from 'next/navigation';
import Link from 'next/link';
import { SiteShell } from '../../../components/SiteShell';

interface TranscriptEntry {
  start?: number;
  end?: number;
  text?: string;
  speaker?: string;
  [key: string]: unknown;
}

interface MicroExpressionEntry {
  timestamp?: number;
  emotion?: string;
  score?: number;
  [key: string]: unknown;
}

interface AnalysisDetail {
  id: string;
  transcript?: TranscriptEntry[] | Record<string, unknown>;
  microexpressions?: MicroExpressionEntry[] | Record<string, unknown>;
  summary?: string;
  status: string;
}

export default function InterviewDetailPage() {
  const params = useParams();
  const id = params?.id as string;
  const [detail, setDetail] = useState<AnalysisDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!id) return;
    const fetchDetail = async () => {
      try {
        const res = await fetch(process.env.NEXT_PUBLIC_ROOM_API_BASE + `/analysis/interviews/${id}`);
        if (!res.ok) throw new Error('Failed to load detail');
        const json = await res.json();
        setDetail(json);
      } catch (err: unknown) {
        if (err instanceof Error) setError(err.message); else setError('Unknown error');
      } finally {
        setLoading(false);
      }
    };
    fetchDetail();
  }, [id]);

  return (
    <SiteShell>
    <div className="space-y-6 px-4 py-6 max-w-6xl mx-auto">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold">Detalhes da Entrevista </h1>
        <Link href="/interview-analysis" className="text-sm text-blue-600 underline">Voltar para a lista</Link>
      </div>
      {loading && <p className="text-sm text-gray-500">Carregando...</p>}
      {error && <p className="text-sm text-red-600">{error}</p>}
      {detail && (
        <div className="space-y-4">
          <div className="p-4 border rounded bg-white">
            <h2 className="font-semibold mb-2">Metadata</h2>
            <p><span className="font-medium">ID:</span> {detail.id}</p>
            <p><span className="font-medium">Status:</span> {detail.status}</p>
          </div>
          <div className="grid md:grid-cols-2 gap-6">
            <div className="p-4 border rounded bg-white overflow-auto max-h-[400px]">
              <h2 className="font-semibold mb-2">Transcrição</h2>
              <pre className="whitespace-pre-wrap text-xs">{JSON.stringify(detail.transcript, null, 2)}</pre>
            </div>
            <div className="p-4 border rounded bg-white overflow-auto max-h-[400px]">
              <h2 className="font-semibold mb-2">Microexpressões</h2>
              <pre className="whitespace-pre-wrap text-xs">{JSON.stringify(detail.microexpressions, null, 2)}</pre>
            </div>
          </div>
          <div className="p-4 border rounded bg-white">
            <h2 className="font-semibold mb-2">Sumário de IA</h2>
            {detail.summary ? (
              <p className="text-sm leading-relaxed whitespace-pre-wrap">{detail.summary}</p>
            ) : (
              <p className="text-sm text-gray-500">Sumário ainda não disponível.</p>
            )}
          </div>
        </div>
      )}
    </div>
    </SiteShell>
  );
}
