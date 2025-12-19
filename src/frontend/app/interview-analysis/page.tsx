"use client";
import { useEffect, useState } from 'react';
import Link from 'next/link';
import { SiteShell } from '../../components/SiteShell';

interface InterviewSummary {
  id: string;
  sessionId?: string;
  timestamp?: string | number;
  hasTranscript?: boolean;
  hasMicroexpression?: boolean;
}

export default function InterviewAnalysisListPage() {
  const [data, setData] = useState<InterviewSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const res = await fetch(process.env.NEXT_PUBLIC_ROOM_API_BASE + '/analysis/interviews');
        if (!res.ok) throw new Error('Failed to load interviews');
        const json = await res.json();
        setData(json);
      } catch (e) {
        const msg = e instanceof Error ? e.message : 'Unknown error';
        setError(msg);
      } finally {
        setLoading(false);
      }
    };
    fetchData();
  }, []);

  return (
    <SiteShell>
      <div className="space-y-6">
      <h1 className="text-2xl font-semibold">Análise de Entrevistas</h1>
      {loading && <p className="text-sm text-gray-500">Carregando...</p>}
      {error && <p className="text-sm text-red-600">{error}</p>}
      {!loading && !error && (
        <table className="min-w-full text-sm border">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-3 py-2 text-left font-medium">ID da Entrevista</th>
              <th className="px-3 py-2 text-left font-medium">Sessão</th>
              <th className="px-3 py-2 text-left font-medium">Transcrição</th>
              <th className="px-3 py-2 text-left font-medium">Microexpressão</th>
            </tr>
          </thead>
          <tbody>
            {data.map(row => (
              <tr key={row.id} className="border-t hover:bg-gray-50 cursor-pointer" >
                <td className="px-3 py-2 text-blue-600 underline">
                  <Link href={`/interview-analysis/${encodeURIComponent(row.id)}`}>{row.id}</Link>
                </td>
                <td className="px-3 py-2">{row.sessionId || '—'}</td>
                <td className="px-3 py-2">{row.hasTranscript ? 'Yes' : 'No'}</td>
                <td className="px-3 py-2">{row.hasMicroexpression ? 'Yes' : 'No'}</td>
              </tr>
            ))}
            {data.length === 0 && (
              <tr>
                <td colSpan={4} className="px-3 py-6 text-center text-gray-500">Nenhuma entrevista encontrada.</td>
              </tr>
            )}
          </tbody>
        </table>
      )}
      </div>
    </SiteShell>
  );
}
