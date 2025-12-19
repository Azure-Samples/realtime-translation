'use client'

import Link from 'next/link'
import { useState } from 'react'
import { useRouter } from 'next/navigation'
import { createRoom, API_BASE } from '@/utils/api';

export default function Home() {
  const [roomId, setRoomId] = useState('');
  const router = useRouter();
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleJoinRoom: React.FormEventHandler<HTMLFormElement> = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    setError(null);
    setIsLoading(true);
    if (roomId.trim()) {
      try {
        const res = await fetch(`${API_BASE}/acs/users`, { method: 'POST' });
        if (!res.ok) throw new Error('Failed to create ACS user');
        const { id: acsUserId } = await res.json();
        const participants = [
          { id: acsUserId, role: 'Presenter' }
        ];
        const room = await createRoom(60, participants);
        router.push(`/room/${room.room_id}?anonymous=true`);
      } catch (error: unknown) {
        setError(error instanceof Error ? error.message : 'Failed to create or join room');
      } finally {
        setIsLoading(false);
      }
    }
  };

  return (
    <main className="h-screen flex flex-col items-center justify-center bg-gradient-to-b from-blue-50 to-white">
      <h1 className="text-4xl font-bold mb-8 text-blue-700">Entrevistas com IA</h1>
      
      <div className="w-full max-w-md p-8 bg-white rounded-lg shadow-lg border border-blue-100">
        <form onSubmit={handleJoinRoom} className="space-y-6">
          <div>
            <input
              type="text"
              id="roomId"
              value={roomId}
              onChange={(e) => setRoomId(e.target.value)}
              className="block w-full px-4 py-3 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
              placeholder="Entre com o ID da entrevista"
              required
            />
          </div>
          
          <button
            type="submit"
            className="w-full flex justify-center py-3 px-4 border border-transparent rounded-md shadow-sm text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 transition duration-150"
            disabled={isLoading}
          >
            {isLoading ? 'Joining...' : 'Entrar como convidado'}
          </button>
        </form>

        {error && (
          <div className="mt-4 text-red-600 text-center">{error}</div>
        )}
        
        <div className="mt-8 grid grid-cols-2 gap-4">
          <Link href="/signup" className="block">
            <button className="w-full py-3 px-4 bg-green-500 text-white rounded-md hover:bg-green-600 transition duration-150">
              Inscreva-se
            </button>
          </Link>
          <Link href="/signin" className="block">
            <button className="w-full py-3 px-4 bg-blue-500 text-white rounded-md hover:bg-blue-600 transition duration-150">
              Entrar
            </button>
          </Link>
        </div>
        <div className="mt-4">
          <Link href="/interview-analysis" className="block">
            <button className="w-full py-3 px-4 bg-red-600 text-white rounded-md hover:bg-red-700 transition duration-150 font-medium shadow">
              Ver Análises de Entrevistas
            </button>
          </Link>
        </div>
      </div>
    </main>
  );
}