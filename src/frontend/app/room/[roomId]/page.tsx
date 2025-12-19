'use client';

import dynamic from 'next/dynamic';
import { useParams } from 'next/navigation';
import { useEffect, useState, useCallback } from 'react';
import { API_BASE } from '@/utils/api';
import React from 'react';

const LANGUAGE_OPTIONS = [
  { value: 'en', label: 'Inglês' },
  { value: 'es', label: 'Espanhol' },
  { value: 'pt', label: 'Português' },
  // Add more as needed
];

const CallCompositeWrapper = dynamic(() => import('../../../components/CallCompositeWrapper'), { ssr: false });

export default function RoomPage() {
  const { roomId } = useParams<{ roomId: string }>();
  console.log('[RoomPage] MOUNT', { roomId });
  const [acsCredentials, setAcsCredentials] = useState<null | { user_id: string; token: string; expires_on: number }>(null);
  const [error, setError] = useState<string | null>(null);
  const [selectedLanguage, setSelectedLanguage] = useState<string>('pt');
  const [languageConfirmed, setLanguageConfirmed] = useState<boolean>(false);

  useEffect(() => {
    if (!roomId || !languageConfirmed) {
      // Wait for language selection before fetching token
      return;
    }

    let isMounted = true;

    const fetchAcsTokenAndNotifyBot = async () => {
      try {
        // 1. Fetch ACS token
        console.log('[RoomPage] Fetching ACS token for roomId:', roomId);
        const res = await fetch(`${API_BASE}/rooms/${roomId}/token`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ language: selectedLanguage }),
        });
        const text = await res.text();
        console.log('[RoomPage] Token fetch response:', res.status, res.statusText, '| Body:', text);
        if (!res.ok) throw new Error('Failed to get ACS token: ' + text);
        const data = JSON.parse(text);
        console.log('[RoomPage] ACS credentials data:', data);
        if (isMounted) {
          setAcsCredentials(data);
        }
      } catch (err: unknown) {
        console.error('[RoomPage] Error fetching ACS token or notifying bot:', err);
        setError(err instanceof Error ? err.message : 'Could not join room');
      }
    };
    fetchAcsTokenAndNotifyBot();
    return () => { isMounted = false; };
  }, [roomId, languageConfirmed, selectedLanguage]);

  const renderCallComposite = useCallback(() => {
    if (!acsCredentials) {
      console.log('[RoomPage] No ACS credentials, not rendering CallCompositeWrapper');
      return null;
    }
    console.log('[RoomPage] Rendering CallCompositeWrapper with:', { roomId, acsCredentials });
    return (
      <CallCompositeWrapper
        roomId={roomId as string}
        acsUserId={acsCredentials.user_id}
        acsToken={acsCredentials.token}
      />
    );
  }, [roomId, acsCredentials]);

  return (
    <section style={{ height: '100vh', width: '100vw' }}>
      {!languageConfirmed && (
        <div className="p-6 flex flex-col items-center justify-center h-full">
          <div className="mb-4 text-lg font-semibold">Selecione seu idioma</div>
          <select
            className="mb-4 p-2 border rounded"
            value={selectedLanguage}
            onChange={e => setSelectedLanguage(e.target.value)}
          >
            {LANGUAGE_OPTIONS.map(opt => (
              <option key={opt.value} value={opt.value}>{opt.label}</option>
            ))}
          </select>
          <button
            className="px-4 py-2 bg-blue-600 text-white rounded"
            onClick={() => setLanguageConfirmed(true)}
          >
            Entrar na sala
          </button>
          <div className="mb-4 text-lg font-semibold">Ao entrar na sala, você concorda com a captura de voz e vídeo para análise do entrevistador</div>
        </div>
      )}
      {languageConfirmed && (
        <>
          {!roomId ? (
            <div className="p-6 text-red-600">Room ID is missing</div>
          ) : error ? (
            <div className="p-6 text-red-600">{error}</div>
          ) : acsCredentials ? (
            renderCallComposite()
          ) : (
            <div className="p-6 text-blue-600">Joining room...</div>
          )}
        </>
      )}
    </section>
  );
}